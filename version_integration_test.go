//go:build integration

package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestVersionsCoexistAndRecoverStoredPlans(t *testing.T) {
	clock := NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	old := DefineActivity[string, string]("transform").WithVersion("1", "text/1", "text/1")
	newer := DefineActivity[int, int]("transform").WithVersion("2", "number/1", "number/1")
	v1 := Define("versioned", func(in string) ([]ActivitySpec, error) {
		step, err := old.Step("transform", in)
		return []ActivitySpec{step}, err
	}).WithVersion("1", "text/1")
	v2 := Define("versioned", func(in int) ([]ActivitySpec, error) {
		step, err := newer.Step("transform", in)
		return []ActivitySpec{step}, err
	}).WithVersion("2", "number/1")
	if err := Register(engine.registry, v1); err != nil {
		t.Fatal(err)
	}
	if err := Register(engine.registry, v2); err != nil {
		t.Fatal(err)
	}
	first, err := Start(t.Context(), engine, v1, "old", WithStartKey("same"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Start(t.Context(), engine, v2, 2, WithStartKey("same"))
	if err != nil || first.ID == second.ID {
		t.Fatalf("version-scoped starts: %#v %v", second, err)
	}
	duplicate, err := Start(t.Context(), engine, v1, "old", WithStartKey("same"))
	if err != nil || duplicate.ID != first.ID || duplicate.DefinitionVersion != "1" || duplicate.InputSchema != "text/1" {
		t.Fatalf("duplicate: %#v %v", duplicate, err)
	}
	crashed, err := engine.claimActivity(t.Context(), "old-crashed", time.Second, map[string]capability{activityKey("transform", "1"): {"transform", "1", old.contract(), "text/1", "text/1"}})
	if err != nil || crashed.WorkflowID != first.ID {
		t.Fatalf("old claim: %#v %v", crashed, err)
	}
	// A deployment registers changed definition code. Existing work must never call it.
	restarted, err := New(engine.pool, NewRegistry(), clock)
	if err != nil {
		t.Fatal(err)
	}
	changed := v1
	changed.Plan = func(string) ([]ActivitySpec, error) {
		t.Error("reran old definition")
		return nil, errors.New("removed plan")
	}
	if err := Register(restarted.registry, changed); err != nil {
		t.Fatal(err)
	}
	newWorker, err := NewWorker(restarted, WorkerConfig{ID: "new"}, newer.Handle(func(ctx context.Context, in int) (int, error) {
		metadata, _ := Metadata(ctx)
		if metadata.ActivityVersion != "2" || metadata.InputSchema != "number/1" {
			return 0, errors.New("wrong version metadata")
		}
		return in * 2, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := newWorker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("new worker: %v %v", worked, err)
	}
	clock.Advance(time.Second)
	if _, err := restarted.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if worked, err := newWorker.RunOnce(t.Context()); err != nil || worked {
		t.Fatalf("new worker claimed old work: %v %v", worked, err)
	}
	unsupported, err := newWorker.UnsupportedActivities(t.Context())
	if err != nil || len(unsupported) != 1 || unsupported[0].Version != "1" || unsupported[0].InputSchema != "text/1" {
		t.Fatalf("diagnostic: %#v %v", unsupported, err)
	}
	oldWorker, err := NewWorker(restarted, WorkerConfig{ID: "both"}, old.Handle(func(_ context.Context, in string) (string, error) { return in + " done", nil }), newer.Handle(func(_ context.Context, in int) (int, error) { return in, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := oldWorker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("old handler: %v %v", worked, err)
	}
	if out, err := old.WorkflowResult(t.Context(), restarted, first.ID); err != nil || out != "old done" {
		t.Fatalf("old result: %q %v", out, err)
	}
	if out, err := newer.WorkflowResult(t.Context(), restarted, second.ID); err != nil || out != 4 {
		t.Fatalf("new result: %d %v", out, err)
	}
	if _, err := old.WithVersion("2", "text/1", "text/1").WorkflowResult(t.Context(), restarted, first.ID); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	if err := restarted.CompleteActivity(t.Context(), crashed, []byte(`"stale"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	activities, _ := restarted.Activities(t.Context(), first.ID)
	if activities[0].AttemptCount != 2 || activities[0].Version != "1" || activities[0].OutputSchema != "text/1" {
		t.Fatal(activities)
	}
}

func TestSchemaIdentityFencesHandlersAndCleanup(t *testing.T) {
	engine, _ := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
	create := DefineActivity[string, string]("schema-create").WithVersion("v2", "input/2", "output/2")
	undo := DefineActivity[string, string]("schema-undo").WithVersion("v3", "cleanup/3", "cleanup/3")
	definition := Define("schemas", func(in string) ([]ActivitySpec, error) {
		cleanup, err := undo.Step("undo", in)
		if err != nil {
			return nil, err
		}
		first, err := create.Step("first", in, WithCompensation(cleanup))
		if err != nil {
			return nil, err
		}
		last, err := create.Step("last", in)
		return []ActivitySpec{first, last}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := New(engine.pool, NewRegistry(), engine.clock)
	wrong, err := NewWorker(other, WorkerConfig{ID: "wrong-schema"}, create.WithVersion("v2", "input/1", "output/2").Handle(func(_ context.Context, in string) (string, error) { t.Error("incompatible schema ran"); return in, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := wrong.RunOnce(t.Context()); err != nil || worked {
		t.Fatalf("schema routing: %v %v", worked, err)
	}
	planned, _ := engine.Activities(t.Context(), execution.ID)
	if planned[0].AttemptCount != 0 {
		t.Fatal("mismatch consumed attempt")
	}
	completeForwardSteps(t, engine, 1)
	failForward(t, engine)
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || len(activities) != 3 || activities[2].Version != "v3" || activities[2].InputSchema != "cleanup/3" {
		t.Fatalf("cleanup identity: %#v %v", activities, err)
	}
	worker, err := NewWorker(other, WorkerConfig{ID: "cleanup"}, undo.Handle(func(ctx context.Context, in string) (string, error) {
		if _, err := create.WithVersion("v2", "input/1", "output/2").Result(ctx, "first"); !errors.Is(err, ErrTypeMismatch) {
			return "", errors.New("schema mismatch accepted")
		}
		return create.Result(ctx, "first")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("cleanup: %v %v", worked, err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.CompensationState != CompensationCompleted {
		t.Fatalf("outcome: %#v %v", state, err)
	}
}
