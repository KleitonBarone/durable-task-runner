//go:build integration

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
		second, err := a.Step("second", input+"!")
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
	if _, err := a.WorkflowResult(ctx, engine, execution.ID); !errors.Is(err, ErrResultUnavailable) {
		t.Fatalf("early final result: %v", err)
	}
	worker, err := NewWorker(engine, config, a.Handle(func(ctx context.Context, input string) (int, error) {
		if _, err := a.Result(ctx, "second"); !errors.Is(err, ErrResultUnavailable) {
			return 0, fmt.Errorf("future/self read: %v", err)
		}
		if input == "hello" {
			if _, err := a.Result(ctx, "first"); !errors.Is(err, ErrResultUnavailable) {
				return 0, fmt.Errorf("self read: %v", err)
			}
			return len(input), nil
		}
		if _, err := wrong.Result(ctx, "first"); !errors.Is(err, ErrTypeMismatch) {
			return 0, fmt.Errorf("wrong type read: %v", err)
		}
		if _, err := a.Result(ctx, "missing"); !errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("missing result: %v", err)
		}
		previous, err := a.Result(ctx, "first")
		return previous + len(input), err
	}))
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
	value, err := a.WorkflowResult(ctx, engine, execution.ID)
	if err != nil || value != 11 || string(result.Output) != "11" {
		t.Fatalf("final result = %d, raw %s, err = %v", value, result.Output, err)
	}
	attempts, err := engine.Attempts(ctx, activities[0].ID)
	if err != nil || len(attempts) != 1 || attempts[0].State != AttemptCompleted {
		t.Fatalf("attempts = %#v, err = %v", attempts, err)
	}
	if _, err := engine.Workflow(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing workflow: %v", err)
	}
}

func TestInvalidPayloadDoesNotPersistOrAcknowledge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine, definition := newIntegrationEngine(t, SystemClock{}, 1)
	if _, err := Start(ctx, engine, definition, integrationInput{Value: strings.Repeat("x", MaxPayloadBytes)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized start: %v", err)
	}
	var count int
	if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_executions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid start persisted: %d %v", count, err)
	}
	execution, err := Start(ctx, engine, definition, integrationInput{Value: "valid"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(ctx, "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`{`)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid completion: %v", err)
	}
	if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != WorkflowCompleted {
		t.Fatalf("valid completion failed: %#v %v", result, err)
	}
}
