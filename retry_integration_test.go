//go:build integration

package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func retryExecution(t *testing.T, policy RetryPolicy) (*Engine, *ManualClock, WorkflowExecution) {
	t.Helper()
	clock := NewManualClock(time.Unix(1, 0))
	engine, _ := newIntegrationEngine(t, clock, 1)
	definition := Define("retry", func(input string) ([]ActivitySpec, error) {
		step, err := NewActivity("effect", input, WithRetry(policy))
		return []ActivitySpec{step}, err
	})
	if err := Register(engine.registry, definition); err != nil {
		t.Fatal(err)
	}
	execution, err := Start(t.Context(), engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	return engine, clock, execution
}

func TestRetryDueTimeSurvivesRestartAndExhaustion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	engine, clock, execution := retryExecution(t, RetryPolicy{MaxAttempts: 3, RetryErrors: true, InitialInterval: time.Second, MaximumInterval: 2 * time.Second})
	first, err := engine.ClaimActivity(ctx, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(ctx, first, errors.New("retry me")); err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil || activities[0].State != ActivityWaiting || !activities[0].AvailableAt.Equal(clock.Now().Add(time.Second)) {
		t.Fatalf("retry wait: %#v %v", activities, err)
	}
	attempts, err := engine.Attempts(ctx, first.ActivityID)
	if err != nil || attempts[0].RetryDecision != "retry" || attempts[0].RetryDueAt == nil || !attempts[0].RetryDueAt.Equal(activities[0].AvailableAt) {
		t.Fatalf("retry decision: %#v %v", attempts, err)
	}
	clock.Advance(time.Second - time.Microsecond)
	if n, err := engine.RecoverAbandoned(ctx, 100); err != nil || n != 0 {
		t.Fatalf("early scheduler: %d %v", n, err)
	}
	if _, err := engine.ClaimActivity(ctx, "early", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatalf("early claim: %v", err)
	}
	restarted, err := New(engine.pool, NewRegistry(), clock)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Microsecond)
	if n, err := restarted.RecoverAbandoned(ctx, 100); err != nil || n != 1 {
		t.Fatalf("due scheduler: %d %v", n, err)
	}
	second, err := restarted.ClaimActivity(ctx, "second", time.Minute)
	if err != nil || second.AttemptNumber != 2 {
		t.Fatalf("second claim: %#v %v", second, err)
	}
	if err := restarted.FailActivity(ctx, second, errors.New("again")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := restarted.RecoverAbandoned(ctx, 100); err != nil {
		t.Fatal(err)
	}
	third, err := restarted.ClaimActivity(ctx, "third", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.FailActivity(ctx, third, errors.New("last")); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(ctx, execution.ID)
	if err != nil || result.State != WorkflowFailed || result.TerminalReason != "attempt_budget_exhausted" {
		t.Fatalf("exhausted: %#v %v", result, err)
	}
	if _, err := restarted.ClaimActivity(ctx, "fourth", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatalf("budget exceeded: %v", err)
	}
}

func TestAbandonmentConsumesFiniteClaimBudget(t *testing.T) {
	engine, clock, execution := retryExecution(t, RetryPolicy{})
	for i := 1; i <= 3; i++ {
		claim, err := engine.ClaimActivity(t.Context(), "vanishing", time.Second)
		if err != nil || claim.AttemptNumber != i {
			t.Fatalf("claim %d: %#v %v", i, claim, err)
		}
		clock.Advance(time.Second)
		if n, err := engine.RecoverAbandoned(t.Context(), 100); err != nil || n != 1 {
			t.Fatalf("recovery %d: %d %v", i, n, err)
		}
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowFailed || result.TerminalReason != "attempt_budget_exhausted" {
		t.Fatalf("abandonment budget: %#v %v", result, err)
	}
	if _, err := engine.ClaimActivity(t.Context(), "extra", time.Second); !errors.Is(err, ErrNoActivity) {
		t.Fatalf("unbounded abandonment: %v", err)
	}
}

func TestPermanentFailureDoesNotRetryAndSchedulerPromotionIsIdempotent(t *testing.T) {
	engine, clock, execution := retryExecution(t, RetryPolicy{RetryErrors: true, InitialInterval: time.Second})
	claim, err := engine.ClaimActivity(t.Context(), "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("retry")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	var group sync.WaitGroup
	counts := make(chan int, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			n, err := engine.RecoverAbandoned(t.Context(), 100)
			if err != nil {
				t.Error(err)
			}
			counts <- n
		}()
	}
	group.Wait()
	close(counts)
	total := 0
	for n := range counts {
		total += n
	}
	if total != 1 {
		t.Fatalf("duplicate promotion: %d", total)
	}
	claim, err = engine.ClaimActivity(t.Context(), "second", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, Permanent(errors.New("stop"))); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || result.State != WorkflowFailed || result.TerminalReason != "permanent_error" {
		t.Fatalf("permanent: %#v %v", result, err)
	}
}
