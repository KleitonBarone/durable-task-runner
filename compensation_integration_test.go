//go:build integration

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

var compCreate = DefineActivity[string, string]("create")
var compUndo = DefineActivity[string, string]("undo")

func compensationExecution(t *testing.T, policy RetryPolicy, options ...StartOption) (*Engine, *ManualClock, WorkflowExecution) {
	t.Helper()
	clock := NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := Define("compensation", func(_ string) ([]ActivitySpec, error) {
		var steps []ActivitySpec
		for _, key := range []string{"one", "two"} {
			cleanup, err := compUndo.Step("cleanup", key, WithRetry(policy), WithAttemptTimeout(time.Second))
			if err != nil {
				return nil, err
			}
			step, err := compCreate.Step(key, key, WithCompensation(cleanup))
			if err != nil {
				return nil, err
			}
			steps = append(steps, step)
		}
		ambiguous, err := compUndo.Step("ambiguous-cleanup", "uncommitted")
		if err != nil {
			return nil, err
		}
		last, err := NewActivity("fail", "trigger", WithCompensation(ambiguous))
		return append(steps, last), err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, "input", options...)
	if err != nil {
		t.Fatal(err)
	}
	return engine, clock, execution
}

func completeForwardSteps(t *testing.T, engine *Engine, count int) {
	t.Helper()
	for range count {
		claim, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if claim.Kind != ActivityForward {
			t.Fatalf("forward kind: %#v", claim)
		}
		if err := engine.CompleteActivity(t.Context(), claim, claim.ActivityInput); err != nil {
			t.Fatal(err)
		}
	}
}

func failForward(t *testing.T, engine *Engine) {
	t.Helper()
	claim, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("forward failed")); err != nil {
		t.Fatal(err)
	}
}

func TestCompensationReverseOrderSurvivesEngineRestart(t *testing.T) {
	engine, clock, execution := compensationExecution(t, RetryPolicy{})
	planned, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || len(planned) != 3 || planned[0].Compensation == nil || planned[0].Compensation.Name != "undo" {
		t.Fatalf("persisted declaration: %#v %v", planned, err)
	}
	completeForwardSteps(t, engine, 2)
	failForward(t, engine)
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowCompensating || state.OriginalState != WorkflowFailed || state.LastError != "forward failed" {
		t.Fatalf("trigger: %#v %v", state, err)
	}
	first, err := engine.ClaimActivity(t.Context(), "cleanup-A", time.Minute)
	if err != nil || first.Kind != ActivityCompensation || string(first.ActivityInput) != `"two"` || first.CompensatesActivityID == nil || *first.CompensatesActivityID != planned[1].ID || first.ActivityID == planned[1].ID {
		t.Fatalf("reverse claim: %#v %v", first, err)
	}
	if err := engine.CompleteActivity(t.Context(), first, []byte(`"undone"`)); err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), first, []byte(`"duplicate"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	restarted, err := New(engine.pool, NewRegistry(), clock)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	worker, err := NewWorker(restarted, WorkerConfig{ID: "cleanup-B"}, compUndo.Handle(func(ctx context.Context, key string) (string, error) {
		result, err := compCreate.Result(ctx, key)
		if err != nil {
			return "", err
		}
		metadata, ok := Metadata(ctx)
		if !ok || metadata.Kind != ActivityCompensation || metadata.CompensatesActivityID == nil || metadata.EffectKey == "durable:"+planned[0].ID {
			return "", errors.New("invalid cleanup metadata")
		}
		order = append(order, result)
		return "undone", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(t.Context()); err != nil || !worked {
		t.Fatalf("stored cleanup: %v %v", worked, err)
	}
	if !reflect.DeepEqual(order, []string{"one"}) {
		t.Fatal(order)
	}
	state, err = restarted.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowFailed || state.CompensationState != CompensationCompleted || state.LastError != "forward failed" || state.TerminalReason != "handler_error" {
		t.Fatalf("cleanup result: %#v %v", state, err)
	}
	activities, err := restarted.Activities(t.Context(), execution.ID)
	if err != nil || len(activities) != 5 {
		t.Fatalf("ambiguous step was compensated: %#v %v", activities, err)
	}
}

func TestCompensationRedeliveryKeepsEffectIdentity(t *testing.T) {
	engine, clock, execution := compensationExecution(t, RetryPolicy{})
	completeForwardSteps(t, engine, 2)
	failForward(t, engine)
	first, err := engine.ClaimActivity(t.Context(), "crashed", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	effects := map[string]bool{"durable:" + first.ActivityID: true}
	clock.Advance(500 * time.Millisecond)
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	second, err := engine.ClaimActivity(t.Context(), "replacement", 500*time.Millisecond)
	if err != nil || first.ActivityID != second.ActivityID || second.AttemptNumber != 2 || first.LeaseToken == second.LeaseToken {
		t.Fatalf("replacement: %#v %v", second, err)
	}
	effects["durable:"+second.ActivityID] = true
	if len(effects) != 1 {
		t.Fatal("cleanup effect was duplicated despite application deduplication")
	}
	if err := engine.CompleteActivity(t.Context(), first, []byte(`"stale"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), second, []byte(`"undone"`)); err != nil {
		t.Fatal(err)
	}
	last, err := engine.ClaimActivity(t.Context(), "replacement", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), last, []byte(`"undone"`)); err != nil {
		t.Fatal(err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.CompensationState != CompensationCompleted {
		t.Fatalf("result: %#v %v", state, err)
	}
}

func TestCompensationExhaustionPreservesOriginalCause(t *testing.T) {
	for _, failure := range []string{"handler", "crash"} {
		t.Run(failure, func(t *testing.T) {
			engine, clock, execution := compensationExecution(t, RetryPolicy{MaxAttempts: 2, RetryErrors: true})
			completeForwardSteps(t, engine, 2)
			failForward(t, engine)
			for range 2 {
				claim, err := engine.ClaimActivity(t.Context(), "cleanup", 500*time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				if failure == "handler" {
					if err := engine.FailActivity(t.Context(), claim, errors.New("cleanup failed")); err != nil {
						t.Fatal(err)
					}
				} else {
					clock.Advance(500 * time.Millisecond)
					if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
						t.Fatal(err)
					}
				}
			}
			state, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil || state.State != WorkflowCompensationFailed || state.OriginalState != WorkflowFailed || state.LastError != "forward failed" || state.CompensationState != CompensationFailed || state.CompensationError == "" {
				t.Fatalf("exhaustion: %#v %v", state, err)
			}
			if _, err := engine.ClaimActivity(t.Context(), "late", time.Minute); !errors.Is(err, ErrNoActivity) {
				t.Fatal(err)
			}
			activities, err := engine.Activities(t.Context(), execution.ID)
			if err != nil || activities[4].State != ActivityCancelled || activities[4].AttemptCount != 0 {
				t.Fatalf("remaining cleanup: %#v %v", activities, err)
			}
		})
	}
}

func TestCompensationOutlivesForwardDeadline(t *testing.T) {
	engine, clock, execution := compensationExecution(t, RetryPolicy{}, WithWorkflowTimeout(time.Second), WithCompensationTimeout(10*time.Second))
	completeForwardSteps(t, engine, 2)
	clock.Advance(time.Second)
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		claim, err := engine.ClaimActivity(t.Context(), "cleanup", time.Minute)
		if err != nil || !claim.AttemptDeadline.After(execution.DeadlineAt) {
			t.Fatalf("independent deadline: %#v %v", claim, err)
		}
		if _, err := engine.Heartbeat(t.Context(), claim, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := engine.CompleteActivity(t.Context(), claim, []byte(`"undone"`)); err != nil {
			t.Fatal(err)
		}
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowTimedOut || state.CompensationState != CompensationCompleted || state.TerminalReason != "workflow_deadline" {
		t.Fatalf("timeout cleanup: %#v %v", state, err)
	}
}

func TestCompensationDeadlineStopsCleanup(t *testing.T) {
	for _, limit := range []string{"overall", "attempt"} {
		t.Run(limit, func(t *testing.T) {
			var options []StartOption
			if limit == "overall" {
				options = append(options, WithCompensationTimeout(time.Second))
			}
			engine, clock, execution := compensationExecution(t, RetryPolicy{}, options...)
			completeForwardSteps(t, engine, 2)
			failForward(t, engine)
			claim, err := engine.ClaimActivity(t.Context(), "cleanup", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) {
				t.Fatal(err)
			}
			state, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil || state.State != WorkflowCompensationFailed || state.LastError != "forward failed" {
				t.Fatalf("cleanup limit: %#v %v", state, err)
			}
			want := "attempt_deadline"
			if limit == "overall" {
				want = "compensation_deadline"
			}
			if state.CompensationError != want {
				t.Fatalf("cleanup reason=%s want=%s", state.CompensationError, want)
			}
		})
	}
}

func TestCancellationDuringCompensationDoesNotCancelCleanup(t *testing.T) {
	engine, _, execution := compensationExecution(t, RetryPolicy{})
	completeForwardSteps(t, engine, 1)
	forward, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), execution.ID, "operator", "undo"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowCompensating || state.OriginalState != WorkflowCancelled || state.CancelSettledAt != nil {
		t.Fatalf("cancel cleanup: %#v %v", state, err)
	}
	cleanup, err := engine.ClaimActivity(t.Context(), "cleanup", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), execution.ID, "another operator", "repeat"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Heartbeat(t.Context(), cleanup, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), forward, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), cleanup, []byte(`"undone"`)); err != nil {
		t.Fatal(err)
	}
	state, err = engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowCancelled || state.CancelSettledAt == nil || state.CancelActor != "operator" || state.CompensationState != CompensationCompleted {
		t.Fatalf("cancel settled: %#v %v", state, err)
	}
}

func TestCompensationActivationRollsBackWithHistoryFailure(t *testing.T) {
	engine, _, execution := compensationExecution(t, RetryPolicy{})
	completeForwardSteps(t, engine, 2)
	claim, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.pool.Exec(t.Context(), `CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='compensation.scheduled' THEN RAISE EXCEPTION 'injected cleanup history failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_cleanup BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_cleanup()`); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("forward failed")); err == nil {
		t.Fatal("partial activation accepted")
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowRunning {
		t.Fatalf("partial workflow: %#v %v", state, err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || len(activities) != 3 || activities[2].State != ActivityRunning {
		t.Fatalf("partial cleanup: %#v %v", activities, err)
	}
	if _, err := engine.Heartbeat(t.Context(), claim, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestNoCleanupForUncommittedOrAbsentDeclarations(t *testing.T) {
	engine, _ := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
	definition := Define("no-known-cleanup", func(_ string) ([]ActivitySpec, error) {
		first, err := NewActivity("effect", "no declaration")
		if err != nil {
			return nil, err
		}
		cleanup, err := NewActivity("undo", "uncommitted step")
		if err != nil {
			return nil, err
		}
		last, err := NewActivity("effect", "will fail", WithCompensation(cleanup))
		return []ActivitySpec{first, last}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	completeForwardSteps(t, engine, 1)
	failForward(t, engine)
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != WorkflowFailed || state.CompensationState != CompensationNone {
		t.Fatalf("absent cleanup: %#v %v", state, err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || len(activities) != 2 {
		t.Fatalf("unexpected cleanup: %#v %v", activities, err)
	}
}

func TestCleanupDeclarationRejectsPostgresInvalidInputAtomically(t *testing.T) {
	engine, clock, _ := compensationExecution(t, RetryPolicy{})
	definition := Define("invalid-cleanup", func(_ string) ([]ActivitySpec, error) {
		cleanup, err := NewActivity("cleanup", json.RawMessage(`"\ud800"`))
		if err != nil {
			return nil, err
		}
		step, err := NewActivity("forward", "input", WithCompensation(cleanup))
		return []ActivitySpec{step}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(t.Context(), engine, definition, "input"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cleanup JSON: %v at %s", err, clock.Now())
	}
	var count int
	if err := engine.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflow_executions WHERE definition_name='invalid-cleanup'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial start: %d %v", count, err)
	}
}
