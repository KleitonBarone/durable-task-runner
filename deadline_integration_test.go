//go:build integration

package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func deadlineExecution(t *testing.T, timeout time.Duration, options ...StartOption) (*Engine, *ManualClock, WorkflowExecution) {
	t.Helper()
	clock := NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := Define("deadline", func(input string) ([]ActivitySpec, error) {
		step, err := NewActivity("effect", input, WithAttemptTimeout(timeout))
		return []ActivitySpec{step}, err
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
	if err := engine.CompleteActivity(t.Context(), claim, []byte(`"late"`)); !errors.Is(err, ErrLeaseLost) || !errors.Is(err, ErrExecutionDeadline) {
		t.Fatalf("late completion: %v", err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowTimedOut || result.TerminalReason != "attempt_deadline" {
		t.Fatalf("timeout: %#v %v", result, err)
	}
	attempts, err := engine.Attempts(t.Context(), claim.ActivityID)
	if err != nil || attempts[0].State != AttemptTimedOut {
		t.Fatalf("attempt: %#v %v", attempts, err)
	}
}

func TestWorkflowDeadlineStopsUnclaimedWork(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Minute, WithWorkflowTimeout(2*time.Second), WithStartKey("deadline"))
	clock.Advance(2 * time.Second)
	if _, err := engine.ClaimActivity(t.Context(), "late", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowTimedOut || result.TerminalReason != "workflow_deadline" {
		t.Fatalf("deadline: %#v %v", result, err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || activities[0].State != ActivityTimedOut || activities[0].AttemptCount != 0 {
		t.Fatalf("unclaimed: %#v %v", activities, err)
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
		if !errors.Is(err, ErrLeaseLost) {
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
	if err != nil || result.State != WorkflowTimedOut || result.Output != nil {
		t.Fatalf("race result: %#v %v", result, err)
	}
}

func TestWorkerCancelsHandlerAtDeadline(t *testing.T) {
	engine, clock, execution := deadlineExecution(t, time.Second)
	started, cancelled := make(chan struct{}), make(chan struct{})
	worker, err := NewWorker(engine, WorkerConfig{ID: "deadline"}, Handle("effect", func(ctx context.Context, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	controlled := &runtimeClock{ManualClock: clock, created: make(chan time.Duration, 10)}
	engine.clock = controlled
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(t.Context()); done <- err }()
	runtimeReceive(t, started)
	runtimeReceive(t, controlled.created)
	runtimeReceive(t, controlled.created)
	clock.Advance(time.Second)
	if err := runtimeReceive(t, done); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	runtimeReceive(t, cancelled)
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowTimedOut {
		t.Fatalf("worker deadline: %#v %v", result, err)
	}
}

func TestAttemptTimeoutRetriesOnlyWithinPersistedBudget(t *testing.T) {
	engine, clock, execution := retryExecution(t, RetryPolicy{MaxAttempts: 2, RetryErrors: true, InitialInterval: time.Second})
	first, err := engine.ClaimActivity(t.Context(), "first", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Minute)
	if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(t.Context(), execution.ID)
	if err != nil || activities[0].State != ActivityWaiting {
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
	if err := engine.CompleteActivity(t.Context(), first, []byte(`"stale"`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Minute)
	if _, err := engine.RecoverAbandoned(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowTimedOut || result.TerminalReason != "attempt_budget_exhausted" {
		t.Fatalf("timeout budget: %#v %v", result, err)
	}
}
