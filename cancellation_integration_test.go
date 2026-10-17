//go:build integration

package durable

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCancellationFencesOwnershipBeforeSettlement(t *testing.T) {
	for _, phase := range []string{"before_claim", "running", "expired"} {
		t.Run(phase, func(t *testing.T) {
			clock := NewManualClock(time.Unix(1, 0))
			engine, definition := newIntegrationEngine(t, clock, 2)
			execution, err := Start(t.Context(), engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			var claim Claim
			if phase != "before_claim" {
				claim, err = engine.ClaimActivity(t.Context(), "worker", time.Second)
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "expired" {
				clock.Advance(time.Second)
			}
			request, err := engine.Cancel(t.Context(), execution.ID, "operator", "stop import")
			if err != nil || request.State != WorkflowCancelRequested || request.CancelActor != "operator" || request.CancelRequestedAt == nil || request.CancelSettledAt != nil {
				t.Fatalf("request: %#v %v", request, err)
			}
			if _, err := engine.ClaimActivity(t.Context(), "later", time.Minute); !errors.Is(err, ErrNoActivity) {
				t.Fatalf("claim after cancellation: %v", err)
			}
			if phase != "before_claim" {
				if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) {
					t.Fatal(err)
				}
				if _, err := engine.Heartbeat(t.Context(), claim, time.Minute); !errors.Is(err, ErrLeaseLost) {
					t.Fatal(err)
				}
				attempts, err := engine.Attempts(t.Context(), claim.ActivityID)
				if err != nil || len(attempts) != 1 || attempts[0].State != AttemptCancelled {
					t.Fatalf("attempts: %#v %v", attempts, err)
				}
			}
			if _, err := engine.Cancel(t.Context(), execution.ID, "second actor", "second reason"); err != nil {
				t.Fatal(err)
			}
			if n, err := engine.RecoverAbandoned(t.Context(), 10); err != nil || n != 1 {
				t.Fatalf("settlement: %d %v", n, err)
			}
			if n, err := engine.RecoverAbandoned(t.Context(), 10); err != nil || n != 0 {
				t.Fatalf("repeat settlement: %d %v", n, err)
			}
			result, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil || result.State != WorkflowCancelled || result.CancelReason != "stop import" || result.CancelSettledAt == nil {
				t.Fatalf("settled: %#v %v", result, err)
			}
			activities, err := engine.Activities(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, activity := range activities {
				if activity.State != ActivityCancelled {
					t.Fatalf("activity: %#v", activity)
				}
			}
			history, err := engine.Timeline(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			requests := 0
			for _, event := range history {
				if event.Type == "workflow.cancel_requested" {
					requests++
				}
			}
			if requests != 1 {
				t.Fatalf("requests=%d", requests)
			}
		})
	}
}

func TestCancellationDuringRetryWaitStopsPromotion(t *testing.T) {
	engine, clock, execution := retryExecution(t, RetryPolicy{RetryErrors: true, InitialInterval: time.Second})
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("retry")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), execution.ID, "operator", "stop waiting"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ClaimActivity(t.Context(), "later", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatal(err)
	}
}

func TestCancellationRacesClaimAndCompletion(t *testing.T) {
	for _, operation := range []string{"claim", "completion"} {
		t.Run(operation, func(t *testing.T) {
			engine, definition := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
			execution, err := Start(t.Context(), engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			var claim Claim
			if operation == "completion" {
				claim, err = engine.ClaimActivity(t.Context(), "first", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			}
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, err := engine.Cancel(t.Context(), execution.ID, "operator", "race")
				errs <- err
			}()
			go func() {
				defer wg.Done()
				<-start
				if operation == "claim" {
					var err error
					claim, err = engine.ClaimActivity(t.Context(), "racer", time.Minute)
					errs <- err
				} else {
					errs <- engine.CompleteActivity(t.Context(), claim, []byte(`"done"`))
				}
			}()
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil && !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrNoActivity) {
					t.Fatal(err)
				}
			}
			if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
				t.Fatal(err)
			}
			result, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			if result.State != WorkflowCancelled && !(operation == "completion" && result.State == WorkflowCompleted) {
				t.Fatalf("race result: %#v", result)
			}
			if _, err := engine.ClaimActivity(t.Context(), "late", time.Minute); !errors.Is(err, ErrNoActivity) {
				t.Fatal(err)
			}
			if claim.LeaseToken != "" {
				if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCancellationPreservesTerminalAndTimeoutOutcomes(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Minute, WithWorkflowTimeout(time.Second))
	clock.Advance(time.Second)
	result, err := engine.Cancel(t.Context(), execution.ID, "operator", "late")
	if err != nil || result.State != WorkflowTimedOut || result.CancelRequestedAt != nil || result.TerminalReason != "workflow_deadline" {
		t.Fatalf("deadline ordering: %#v %v", result, err)
	}
	result, err = engine.Cancel(t.Context(), execution.ID, "operator", "again")
	if err != nil || result.State != WorkflowTimedOut {
		t.Fatalf("terminal: %#v %v", result, err)
	}
	if _, err := engine.Cancel(t.Context(), execution.ID, "", "invalid"); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	missing, _ := newID()
	if _, err := engine.Cancel(t.Context(), missing, "operator", ""); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestCompletedExecutionIgnoresCancellation(t *testing.T) {
	engine, definition := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 1)
	execution, err := Start(t.Context(), engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"done"`)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Cancel(t.Context(), execution.ID, "operator", "too late")
	if err != nil || result.State != WorkflowCompleted || result.CancelRequestedAt != nil || string(result.Output) != `"done"` {
		t.Fatalf("completed cancellation: %#v %v", result, err)
	}
}

func TestCancellationRacesExpiredLeaseRecovery(t *testing.T) {
	clock := NewManualClock(time.Unix(1, 0))
	engine, definition := newIntegrationEngine(t, clock, 2)
	execution, err := Start(t.Context(), engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "vanished", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	start, done := make(chan struct{}), make(chan error, 2)
	go func() { <-start; _, err := engine.Cancel(t.Context(), execution.ID, "operator", "stop"); done <- err }()
	go func() { <-start; _, err := engine.RecoverAbandoned(t.Context(), 10); done <- err }()
	close(start)
	for i := 0; i < 2; i++ {
		if err := runtimeReceive(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowCancelled {
		t.Fatalf("recovery race: %#v %v", result, err)
	}
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"stale"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	var active int
	if err := engine.pool.QueryRow(t.Context(), `SELECT count(*) FROM leases WHERE state='active'`).Scan(&active); err != nil || active != 0 {
		t.Fatalf("active leases=%d %v", active, err)
	}
}

func TestCancellationRollsBackWhenHistoryCannotPersist(t *testing.T) {
	engine, definition := newIntegrationEngine(t, NewManualClock(time.Unix(1, 0)), 2)
	execution, err := Start(t.Context(), engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.pool.Exec(t.Context(), `CREATE FUNCTION reject_cancel_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='activity.cancelled' THEN RAISE EXCEPTION 'injected history failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_cancel BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_cancel_history()`); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Cancel(t.Context(), execution.ID, "operator", "atomic request"); err == nil {
		t.Fatal("history failure accepted")
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowRunning || result.CancelRequestedAt != nil {
		t.Fatalf("partial request: %#v %v", result, err)
	}
	attempts, err := engine.Attempts(t.Context(), claim.ActivityID)
	if err != nil || attempts[0].State != AttemptRunning {
		t.Fatalf("partial attempt: %#v %v", attempts, err)
	}
	if _, err := engine.Heartbeat(t.Context(), claim, time.Minute); err != nil {
		t.Fatalf("ownership was lost on rollback: %v", err)
	}
	events, err := engine.Timeline(t.Context(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "workflow.cancel_requested" {
			t.Fatal("request history escaped rollback")
		}
	}
}
