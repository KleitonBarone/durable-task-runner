//go:build integration

package durable

import (
	"context"
	"testing"
	"time"
)

func TestWorkersClaimOnlyCompatibleActivities(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine, _ := newIntegrationEngine(t, SystemClock{}, 1)
	a := DefineActivity[string, string]("a")
	b := DefineActivity[string, string]("b")
	definition := Define("heterogeneous", func(input string) ([]ActivitySpec, error) {
		first, err := a.Step("first", input)
		if err != nil {
			return nil, err
		}
		second, err := b.Step("second", input)
		return []ActivitySpec{first, second}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(ctx, engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	config := WorkerConfig{ID: "worker", LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PollInterval: time.Millisecond}
	echo := func(_ context.Context, input string) (string, error) { return input, nil }
	workerB, err := NewWorker(engine, config, b.Handle(echo))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := workerB.RunOnce(ctx); err != nil || worked {
		t.Fatalf("B claimed A: %v %v", worked, err)
	}
	unsupported, err := workerB.UnsupportedActivities(ctx)
	if err != nil || len(unsupported) != 1 || unsupported[0].Name != "a" || unsupported[0].ReadyCount != 1 {
		t.Fatalf("unsupported: %#v %v", unsupported, err)
	}
	// Separate processes have separate registries. An incompatible type with the
	// same name must also remain unclaimed, without failing the execution.
	otherEngine, err := New(engine.pool, NewRegistry(), SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	wrong := DefineActivity[int, string]("a")
	wrongWorker, err := NewWorker(otherEngine, config, wrong.Handle(func(context.Context, int) (string, error) { t.Error("wrong handler called"); return "", nil }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := wrongWorker.RunOnce(ctx); err != nil || worked {
		t.Fatalf("incompatible claim: %v %v", worked, err)
	}
	workerA, err := NewWorker(engine, config, a.Handle(echo))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := workerA.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("A did not claim: %v %v", worked, err)
	}
	if worked, err := workerA.RunOnce(ctx); err != nil || worked {
		t.Fatalf("A claimed B: %v %v", worked, err)
	}
	if worked, err := workerB.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("B did not claim: %v %v", worked, err)
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != WorkflowCompleted {
		t.Fatalf("result: %#v %v", result, err)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil || activities[0].AttemptCount != 1 || activities[1].AttemptCount != 1 {
		t.Fatalf("attempts: %#v %v", activities, err)
	}
}
