//go:build integration

package durable

import (
	"context"
	"testing"
	"time"

	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
)

func TestTypedPlanPersistsStepIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := testdb.Pool(t)
	registry := NewRegistry()
	a := DefineActivity[string, int]("length")
	definition := Define("typed", func(input string) ([]ActivitySpec, error) {
		first, err := a.Step("first", input)
		if err != nil {
			return nil, err
		}
		second, err := a.Step("second", input)
		return []ActivitySpec{first, second}, err
	})
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, err := New(pool, registry, SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(ctx, engine, definition, "hello")
	if err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil || len(activities) != 2 || activities[0].StepKey != "first" || activities[1].StepKey != "second" {
		t.Fatalf("activities = %#v, err = %v", activities, err)
	}
	config := WorkerConfig{ID: "typed", LeaseDuration: time.Second, HeartbeatInterval: time.Millisecond, PollInterval: time.Millisecond}
	wrong := DefineActivity[int, int]("length")
	if _, err := NewWorker(engine, config, wrong.Handle(func(context.Context, int) (int, error) { return 0, nil })); err == nil {
		t.Fatal("incompatible worker accepted")
	}
	worker, err := NewWorker(engine, config, a.Handle(func(_ context.Context, input string) (int, error) { return len(input), nil }))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := worker.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != WorkflowCompleted {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}
