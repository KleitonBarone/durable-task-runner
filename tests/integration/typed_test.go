//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
)

func TestTypedPlanPersistsStepIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := testdb.Pool(t)
	registry := durable.NewRegistry()
	a := durable.DefineActivity[string, int]("length")
	definition := durable.Define("typed", func(input string) ([]durable.ActivitySpec, error) {
		first, err := a.Step("first", input)
		if err != nil {
			return nil, err
		}
		second, err := a.Step("second", input+"!")
		return []durable.ActivitySpec{first, second}, err
	})
	if err := durable.Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, err := newTestEngine(pool, registry, durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(ctx, engine.Engine, definition, "hello")
	if err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil || len(activities) != 2 || activities[0].StepKey != "first" || activities[1].StepKey != "second" {
		t.Fatalf("activities = %#v, err = %v", activities, err)
	}
	config := durable.WorkerConfig{ID: "typed", LeaseDuration: time.Second, HeartbeatInterval: time.Millisecond, PollInterval: time.Millisecond}
	wrong := durable.DefineActivity[int, int]("length")
	if _, err := durable.NewWorker(engine.Engine, config, wrong.Handle(func(context.Context, int) (int, error) { return 0, nil })); err == nil {
		t.Fatal("incompatible worker accepted")
	}
	if _, err := a.WorkflowResult(ctx, engine.Engine, execution.ID); !errors.Is(err, durable.ErrResultUnavailable) {
		t.Fatalf("early final result: %v", err)
	}
	worker, err := durable.NewWorker(engine.Engine, config, a.Handle(func(ctx context.Context, input string) (int, error) {
		if _, err := a.Result(ctx, "second"); !errors.Is(err, durable.ErrResultUnavailable) {
			return 0, fmt.Errorf("future/self read: %v", err)
		}
		if input == "hello" {
			if _, err := a.Result(ctx, "first"); !errors.Is(err, durable.ErrResultUnavailable) {
				return 0, fmt.Errorf("self read: %v", err)
			}
			return len(input), nil
		}
		if _, err := wrong.Result(ctx, "first"); !errors.Is(err, durable.ErrTypeMismatch) {
			return 0, fmt.Errorf("wrong type read: %v", err)
		}
		if _, err := a.Result(ctx, "missing"); !errors.Is(err, durable.ErrNotFound) {
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
	if err != nil || result.State != durable.WorkflowCompleted {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	value, err := a.WorkflowResult(ctx, engine.Engine, execution.ID)
	if err != nil || value != 11 || string(result.Output) != "11" {
		t.Fatalf("final result = %d, raw %s, err = %v", value, result.Output, err)
	}
	attempts, err := engine.Attempts(ctx, activities[0].ID)
	if err != nil || len(attempts) != 1 || attempts[0].State != durable.AttemptCompleted {
		t.Fatalf("attempts = %#v, err = %v", attempts, err)
	}
	if _, err := engine.Workflow(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("missing workflow: %v", err)
	}
}

func TestNilInterfaceInputPersistsAndCompletes(t *testing.T) {
	engine, _ := newIntegrationEngine(t, durable.NewManualClock(time.Unix(1, 0)), 1)
	activity := durable.DefineActivity[any, any]("echo-null")
	definition := durable.Define("null-input", func(input any) ([]durable.ActivitySpec, error) {
		step, err := activity.Step("echo", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine.Engine, definition, nil, durable.WithStartKey("null"))
	if err != nil || string(execution.Input) != "null" {
		t.Fatalf("nil start: %#v %v", execution, err)
	}
	duplicate, err := durable.Start[any](t.Context(), engine.Engine, definition, json.RawMessage("null"), durable.WithStartKey("null"))
	if err != nil || duplicate.ID != execution.ID {
		t.Fatalf("JSON-equivalent null start: %#v %v", duplicate, err)
	}
	if _, err := durable.Start[any](t.Context(), engine.Engine, definition, "different", durable.WithStartKey("null")); !errors.Is(err, durable.ErrStartConflict) {
		t.Fatalf("conflicting start: %v", err)
	}
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "null"}, activity.Handle(func(_ context.Context, input any) (any, error) {
		if input != nil {
			return nil, fmt.Errorf("handler input = %#v, want nil", input)
		}
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("null handler: %v %v", worked, err)
	}
	value, err := activity.WorkflowResult(t.Context(), engine.Engine, execution.ID)
	if err != nil || value != nil {
		t.Fatalf("typed null result: %#v %v", value, err)
	}
	stored, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || stored.State != durable.WorkflowCompleted || string(stored.Input) != "null" || string(stored.Output) != "null" {
		t.Fatalf("persisted nulls: %#v %v", stored, err)
	}
}

func TestHandlerFailuresRemainFailures(t *testing.T) {
	for _, kind := range []string{"empty", "panic", "exit", "nul"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			engine, definition := newIntegrationEngine(t, durable.SystemClock{}, 2)
			execution, err := durable.Start(ctx, engine.Engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("")
			if kind == "nul" {
				cause = errors.New("message with \x00 NUL")
			}
			worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "failure", LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PollInterval: time.Millisecond}, durable.Handle("step_1", func(context.Context, integrationInput) (string, error) {
				switch kind {
				case "panic":
					panic(cause)
				case "exit":
					runtime.Goexit()
				}
				return "ignored", cause
			}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = worker.RunOnce(ctx)
			if kind == "empty" && !errors.Is(err, cause) {
				t.Fatalf("lost error: %v", err)
			}
			var panicErr *durable.HandlerPanicError
			if (kind == "panic" || kind == "exit") && !errors.As(err, &panicErr) {
				t.Fatalf("panic outcome: %v", err)
			}
			result, err := engine.Workflow(ctx, execution.ID)
			if err != nil || result.State != durable.WorkflowFailed || result.Output != nil {
				t.Fatalf("result: %#v %v", result, err)
			}
			activities, err := engine.Activities(ctx, execution.ID)
			if err != nil || activities[0].State != durable.ActivityFailed || activities[1].State != durable.ActivityBlocked {
				t.Fatalf("activities: %#v %v", activities, err)
			}
			attempts, err := engine.Attempts(ctx, activities[0].ID)
			if err != nil || len(attempts) != 1 || attempts[0].State != durable.AttemptFailed {
				t.Fatalf("attempts: %#v %v", attempts, err)
			}
			if kind == "nul" && attempts[0].Error != `message with \u0000 NUL` {
				t.Fatalf("failure text: %q", attempts[0].Error)
			}
			var active int
			if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM leases WHERE state = 'active'`).Scan(&active); err != nil || active != 0 {
				t.Fatalf("active leases: %d %v", active, err)
			}
		})
	}
}

func TestStoredJSONWhitespaceDoesNotInvalidateInputLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	engine, _ := newIntegrationEngine(t, durable.SystemClock{}, 1)
	definition := durable.Define("large-input", func(input []int) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("length", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(ctx, engine.Engine, definition, make([]int, 350000))
	if err != nil {
		t.Fatal(err)
	}
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "large-input"}, durable.Handle("length", func(_ context.Context, input []int) (int, error) { return len(input), nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != durable.WorkflowCompleted || string(result.Output) != "350000" {
		t.Fatalf("large input result: %s %v", result.State, err)
	}
}

func TestInvalidPayloadDoesNotPersistOrAcknowledge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine, definition := newIntegrationEngine(t, durable.SystemClock{}, 1)
	if _, err := durable.Start(ctx, engine.Engine, definition, integrationInput{Value: strings.Repeat("x", durable.MaxPayloadBytes)}); !errors.Is(err, durable.ErrPayloadTooLarge) {
		t.Fatalf("oversized start: %v", err)
	}
	var count int
	if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_executions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid start persisted: %d %v", count, err)
	}
	rawDefinition := durable.Define("raw-json", func(input json.RawMessage) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("raw", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, rawDefinition); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Start(ctx, engine.Engine, rawDefinition, json.RawMessage(`"\ud800"`)); !errors.Is(err, durable.ErrInvalidInput) {
		t.Fatalf("PostgreSQL-invalid workflow input: %v", err)
	}
	if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_executions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid JSON start persisted: %d %v", count, err)
	}
	execution, err := durable.Start(ctx, engine.Engine, definition, integrationInput{Value: "valid"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(ctx, "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`{`)); !errors.Is(err, durable.ErrInvalidInput) {
		t.Fatalf("invalid completion: %v", err)
	}
	if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`"\ud800"`)); !errors.Is(err, durable.ErrInvalidInput) {
		t.Fatalf("PostgreSQL-invalid JSON completion: %v", err)
	}
	if err := engine.CompleteActivity(ctx, claim, json.RawMessage(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != durable.WorkflowCompleted {
		t.Fatalf("valid completion failed: %#v %v", result, err)
	}
}
