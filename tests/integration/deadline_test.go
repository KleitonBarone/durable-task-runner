//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func deadlineExecution(t *testing.T, timeout time.Duration, options ...durable.StartOption) (*integrationEngine, *durable.ManualClock, durable.WorkflowExecution) {
	t.Helper()
	clock := durable.NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := durable.Define("deadline", func(input string) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("effect", input, durable.WithAttemptTimeout(timeout))
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine.Engine, definition, "input", options...)
	if err != nil {
		t.Fatal(err)
	}
	return engine, clock, execution
}

func TestHeartbeatsCannotExtendAttemptDeadline(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, 5*time.Second)
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claim.LeaseExpiresAt.Equal(clock.Now().Add(5 * time.Second)) {
		t.Fatal("claim exceeds deadline")
	}
	clock.Advance(4 * time.Second)
	expiry, err := engine.Heartbeat(t.Context(), claim, time.Hour)
	if err != nil || !expiry.Equal(claim.AttemptDeadline) {
		t.Fatalf("heartbeat: %s %v", expiry, err)
	}
	clock.Advance(time.Second)
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, durable.ErrLeaseLost) || !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("late completion: %v", err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != durable.WorkflowTimedOut || result.TerminalReason != "attempt_deadline" {
		t.Fatalf("timeout: %#v %v", result, err)
	}
	attempts, err := engine.Attempts(t.Context(), claim.ActivityID)
	if err != nil || attempts[0].State != durable.AttemptTimedOut {
		t.Fatalf("attempt: %#v %v", attempts, err)
	}
}

func TestDelayedRecoveryPreservesEarlierLeaseExpiry(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		name := "forward"
		if cleanup {
			name = "compensation"
		}
		t.Run(name, func(t *testing.T) {
			var engine *integrationEngine
			var clock *durable.ManualClock
			var execution durable.WorkflowExecution
			leaseDuration := 30 * time.Second
			if cleanup {
				engine, clock, execution = compensationExecution(t, durable.RetryPolicy{})
				completeForwardSteps(t, engine, 2)
				failForward(t, engine)
				leaseDuration = 500 * time.Millisecond
			} else {
				engine, clock, execution = deadlineExecution(t, 5*time.Minute)
			}
			first, err := engine.ClaimActivity(t.Context(), "disappeared", leaseDuration)
			if err != nil {
				t.Fatal(err)
			}
			// Both limits pass while the scheduler is down, but ownership ended first.
			clock.Advance(6 * time.Minute)
			restarted, err := newTestEngine(engine.pool, durable.NewRegistry(), clock)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := restarted.RecoverAbandoned(t.Context(), 10); err != nil || n != 1 {
				t.Fatalf("delayed recovery: %d %v", n, err)
			}
			attempts, err := restarted.Attempts(t.Context(), first.ActivityID)
			if err != nil || len(attempts) != 1 || attempts[0].State != durable.AttemptAbandoned || attempts[0].Error != "lease_expired" {
				t.Fatalf("earlier lease expiry must remain abandonment: %#v %v", attempts, err)
			}
			leases, err := restarted.ListLeases(t.Context(), first.ActivityID, durable.PageRequest{})
			if err != nil || len(leases.Items) != 1 || leases.Items[0].State != durable.LeaseExpired {
				t.Fatalf("expired ownership: %#v %v", leases, err)
			}
			if n, err := restarted.RecoverAbandoned(t.Context(), 10); err != nil || n != 0 {
				t.Fatalf("repeated recovery: %d %v", n, err)
			}
			second, err := restarted.ClaimActivity(t.Context(), "replacement", leaseDuration)
			if err != nil || second.ActivityID != first.ActivityID || second.AttemptNumber != 2 || second.StepKey != first.StepKey {
				t.Fatalf("replacement: %#v %v", second, err)
			}
			if err := restarted.CompleteActivity(t.Context(), first, []byte(`"late"`)); !errors.Is(err, durable.ErrLeaseLost) {
				t.Fatalf("stale acknowledgement: %v", err)
			}
			if err := restarted.CompleteActivity(t.Context(), second, []byte(`"done"`)); err != nil {
				t.Fatal(err)
			}
			want := durable.WorkflowCompleted
			if cleanup {
				remaining, err := restarted.ClaimActivity(t.Context(), "replacement", time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if err := restarted.CompleteActivity(t.Context(), remaining, []byte(`"done"`)); err != nil {
					t.Fatal(err)
				}
				want = durable.WorkflowFailed
			}
			result, err := restarted.Workflow(t.Context(), execution.ID)
			if err != nil || result.State != want {
				t.Fatalf("recovered workflow: %#v %v", result, err)
			}
		})
	}
}

func TestWorkflowDeadlineStopsUnclaimedWork(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Minute, durable.WithWorkflowTimeout(2*time.Second), durable.WithStartKey("deadline"))
	clock.Advance(2 * time.Second)
	if _, err := engine.ClaimActivity(t.Context(), "late", time.Minute); !errors.Is(err, durable.ErrNoActivity) {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != durable.WorkflowTimedOut || result.TerminalReason != "workflow_deadline" {
		t.Fatalf("deadline: %#v %v", result, err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || activities[0].State != durable.ActivityTimedOut || activities[0].AttemptCount != 0 {
		t.Fatalf("unclaimed: %#v %v", activities, err)
	}
}

func TestReconciliationLinksNewExecutionWithoutReopeningOld(t *testing.T) {
	engine, clock, old := deadlineExecution(t, time.Second, durable.WithWorkflowTimeout(time.Second))
	definition := durable.Define("reconcile", func(input string) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("effect", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Start(t.Context(), engine.Engine, definition, "fix", durable.WithPredecessor(old.ID)); !errors.Is(err, durable.ErrInvalidInput) {
		t.Fatalf("active predecessor: %v", err)
	}
	clock.Advance(time.Second)
	if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	next, err := durable.Start(t.Context(), engine.Engine, definition, "fix", durable.WithPredecessor(old.ID), durable.WithStartKey("reconcile"))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := engine.Workflow(t.Context(), next.ID)
	if err != nil || stored.PreviousExecutionID == nil || *stored.PreviousExecutionID != old.ID {
		t.Fatalf("link: %#v %v", stored, err)
	}
	previous, err := engine.Workflow(t.Context(), old.ID)
	if err != nil || previous.State != durable.WorkflowTimedOut {
		t.Fatalf("predecessor: %#v %v", previous, err)
	}
	duplicate, err := durable.Start(t.Context(), engine.Engine, definition, "fix", durable.WithStartKey("reconcile"))
	if err != nil || duplicate.ID != next.ID {
		t.Fatalf("duplicate: %#v %v", duplicate, err)
	}
}

func TestCompletionRacingTimeoutCannotReopenExecution(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Second)
	claim, err := engine.ClaimActivity(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`))
		if !errors.Is(err, durable.ErrLeaseLost) {
			t.Errorf("completion: %v", err)
		}
	}()
	go func() {
		defer group.Done()
		if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
			t.Error(err)
		}
	}()
	group.Wait()
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != durable.WorkflowTimedOut || result.Output != nil {
		t.Fatalf("race result: %#v %v", result, err)
	}
}

func TestWorkerCancelsHandlerAtDeadline(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Second)
	started, cancelled := make(chan struct{}), make(chan struct{})
	controlled := &runtimeClock{ManualClock: clock, created: make(chan time.Duration, 10)}
	engine, err := newTestEngine(engine.pool, engine.registry, controlled)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "deadline"}, durable.Handle("effect", func(ctx context.Context, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(t.Context()); done <- err }()
	runtimeReceive(t, started)
	runtimeReceive(t, controlled.created)
	runtimeReceive(t, controlled.created)
	clock.Advance(time.Second)
	if err := runtimeReceive(t, done); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatal(err)
	}
	runtimeReceive(t, cancelled)
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != durable.WorkflowTimedOut {
		t.Fatalf("worker deadline: %#v %v", result, err)
	}
}

func TestAttemptTimeoutRetriesOnlyWithinPersistedBudget(t *testing.T) {
	engine, clock, execution := retryExecution(t, durable.RetryPolicy{MaxAttempts: 2, RetryErrors: true, InitialInterval: time.Second})
	first, err := engine.ClaimActivity(t.Context(), "first", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Minute)
	if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || activities[0].State != durable.ActivityWaiting {
		t.Fatalf("timeout retry: %#v %v", activities, err)
	}
	clock.Advance(time.Second)
	if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	second, err := engine.ClaimActivity(t.Context(), "second", 10*time.Minute)
	if err != nil || second.AttemptNumber != 2 {
		t.Fatalf("retry claim: %#v %v", second, err)
	}
	if err := engine.CompleteActivity(t.Context(), first, []byte(`"stale"`)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Minute)
	if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != durable.WorkflowTimedOut || result.TerminalReason != "attempt_budget_exhausted" {
		t.Fatalf("timeout budget: %#v %v", result, err)
	}
}
