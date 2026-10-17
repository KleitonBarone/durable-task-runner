//go:build integration

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
)

type integrationInput struct {
	Value string `json:"value"`
}

func integrationDefinition(steps int) Definition[integrationInput] {
	return Define("integration_workflow", func(input integrationInput) ([]ActivitySpec, error) {
		activities := make([]ActivitySpec, steps)
		for i := range activities {
			activity, err := NewActivity(fmt.Sprintf("step_%d", i+1), input)
			if err != nil {
				return nil, err
			}
			activities[i] = activity
		}
		return activities, nil
	})
}

func newIntegrationEngine(t testing.TB, clock Clock, steps int) (*Engine, Definition[integrationInput]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := testdb.Pool(t)

	registry := NewRegistry()
	definition := integrationDefinition(steps)
	if err := Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, err := New(pool, registry, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return engine, definition
}

func TestStartPersistsIntentAndMakesOnlyFirstActivityClaimable(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	engine, definition := newIntegrationEngine(t, clock, 2)

	execution, err := Start(ctx, engine, definition, integrationInput{Value: "persisted"})
	if err != nil {
		t.Fatal(err)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activities) != 2 || activities[0].State != ActivityReady || activities[1].State != ActivityBlocked {
		t.Fatalf("unexpected persisted activities: %#v", activities)
	}
	if got, want := string(execution.Input), `{"value":"persisted"}`; got != want {
		t.Fatalf("workflow input = %s, want %s", got, want)
	}
	claim, err := engine.ClaimActivity(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claim.ActivityID != activities[0].ID {
		t.Fatalf("claimed activity %s, want first activity %s", claim.ActivityID, activities[0].ID)
	}
	if _, err := engine.ClaimActivity(ctx, "other-worker", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatalf("second activity became claimable early: %v", err)
	}
}

func TestConcurrentClaimsCreateOneAttemptAndLease(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := Start(ctx, engine, definition, integrationInput{Value: "one claim"})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 12
	start := make(chan struct{})
	claims := make(chan Claim, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			claim, err := engine.ClaimActivity(ctx, fmt.Sprintf("worker-%d", worker), time.Minute)
			if err == nil {
				claims <- claim
				return
			}
			if !errors.Is(err, ErrNoActivity) {
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		t.Errorf("claim failed unexpectedly: %v", err)
	}
	if got := len(claims); got != 1 {
		t.Fatalf("successful claims = %d, want 1", got)
	}

	var attempts, leases int
	if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM attempts at JOIN activities a ON a.id = at.activity_id WHERE a.workflow_id = $1`, execution.ID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := engine.pool.QueryRow(ctx, `SELECT count(*) FROM leases l JOIN activities a ON a.id = l.activity_id WHERE a.workflow_id = $1`, execution.ID).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || leases != 1 {
		t.Fatalf("attempts = %d, leases = %d, want one of each", attempts, leases)
	}
}

func TestHeartbeatExtendsLeaseUntilItReallyExpires(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	engine, definition := newIntegrationEngine(t, clock, 1)
	if _, err := Start(ctx, engine, definition, integrationInput{Value: "heartbeat"}); err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(ctx, "worker", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(9 * time.Second)
	expiresAt, err := engine.Heartbeat(ctx, claim, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if want := clock.Now().Add(10 * time.Second); !expiresAt.Equal(want) {
		t.Fatalf("lease expires at %v, want %v", expiresAt, want)
	}
	var heartbeatAt time.Time
	if err := engine.pool.QueryRow(ctx, `SELECT heartbeat_at FROM leases WHERE token = $1`, claim.LeaseToken).Scan(&heartbeatAt); err != nil {
		t.Fatal(err)
	}
	if !heartbeatAt.Equal(clock.Now()) {
		t.Fatalf("recorded heartbeat = %v, want %v", heartbeatAt, clock.Now())
	}
	clock.Advance(2 * time.Second)
	if recovered, err := engine.RecoverAbandoned(ctx, 10); err != nil || recovered != 0 {
		t.Fatalf("recovered before heartbeat extension elapsed: count=%d err=%v", recovered, err)
	}
	clock.Advance(8 * time.Second)
	if recovered, err := engine.RecoverAbandoned(ctx, 10); err != nil || recovered != 1 {
		t.Fatalf("recovery at lease expiry: count=%d err=%v", recovered, err)
	}
	if _, err := engine.Heartbeat(ctx, claim, 10*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired worker heartbeat = %v, want ErrLeaseLost", err)
	}
}

func TestWorkerCrashAfterSideEffectRecoversWithAtLeastOnceDelivery(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := Start(ctx, engine, definition, integrationInput{Value: "crash"})
	if err != nil {
		t.Fatal(err)
	}

	firstClaim, err := engine.ClaimActivity(ctx, "worker-that-disappears", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var sideEffects atomic.Int32
	sideEffects.Add(1)

	clock.Advance(10 * time.Second)
	scheduler, err := NewScheduler(engine, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if recovered, err := scheduler.RunOnce(ctx); err != nil || recovered != 1 {
		t.Fatalf("recover abandoned activity: count=%d err=%v", recovered, err)
	}
	secondClaim, err := engine.ClaimActivity(ctx, "replacement-worker", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sideEffects.Add(1)

	if err := engine.CompleteActivity(ctx, firstClaim, json.RawMessage(`{"stale":true}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion = %v, want ErrLeaseLost", err)
	}
	if err := engine.CompleteActivity(ctx, secondClaim, json.RawMessage(`{"kept":true}`)); err != nil {
		t.Fatal(err)
	}
	if got := sideEffects.Load(); got != 2 {
		t.Fatalf("side effects = %d, want 2 to demonstrate duplicate delivery", got)
	}

	workflow, err := engine.Workflow(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if workflow.State != WorkflowCompleted {
		t.Fatalf("workflow state = %s, want completed", workflow.State)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activities) != 1 || activities[0].State != ActivityCompleted || activities[0].AttemptCount != 2 {
		t.Fatalf("unexpected recovered activity: %#v", activities)
	}

	var states []AttemptState
	rows, err := engine.pool.Query(ctx, `SELECT at.state FROM attempts at JOIN activities a ON a.id = at.activity_id WHERE a.workflow_id = $1 ORDER BY at.number`, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var state AttemptState
		if err := rows.Scan(&state); err != nil {
			t.Fatal(err)
		}
		states = append(states, state)
	}
	if len(states) != 2 || states[0] != AttemptAbandoned || states[1] != AttemptCompleted {
		t.Fatalf("attempt states = %v, want [abandoned completed]", states)
	}

	timeline, err := engine.Timeline(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantEvents := []string{
		"workflow.started", "activity.scheduled", "activity.claimed",
		"attempt.abandoned", "activity.recovered", "activity.claimed",
		"activity.completed", "workflow.completed",
	}
	if len(timeline) != len(wantEvents) {
		t.Fatalf("timeline length = %d, want %d: %#v", len(timeline), len(wantEvents), timeline)
	}
	for i, want := range wantEvents {
		if timeline[i].Type != want {
			t.Fatalf("timeline event %d = %q, want %q", i, timeline[i].Type, want)
		}
	}
}

func TestHandlerFailureEndsWorkflowWithoutRetry(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	engine, definition := newIntegrationEngine(t, clock, 1)
	execution, err := Start(ctx, engine, definition, integrationInput{Value: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(ctx, claim, errors.New("deterministic handler failure")); err != nil {
		t.Fatal(err)
	}

	workflow, err := engine.Workflow(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if workflow.State != WorkflowFailed {
		t.Fatalf("workflow state = %s, want failed", workflow.State)
	}
	activities, err := engine.Activities(ctx, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activities) != 1 || activities[0].State != ActivityFailed || activities[0].AttemptCount != 1 {
		t.Fatalf("unexpected failed activity: %#v", activities)
	}
	if _, err := engine.ClaimActivity(ctx, "other-worker", time.Minute); !errors.Is(err, ErrNoActivity) {
		t.Fatalf("failed activity was delivered again: %v", err)
	}

	var attemptState AttemptState
	var leaseState LeaseState
	if err := engine.pool.QueryRow(ctx, `
		SELECT at.state, l.state
		FROM attempts at JOIN leases l ON l.attempt_id = at.id
		WHERE at.id = $1`, claim.AttemptID).Scan(&attemptState, &leaseState); err != nil {
		t.Fatal(err)
	}
	if attemptState != AttemptFailed || leaseState != LeaseReleased {
		t.Fatalf("attempt state = %s, lease state = %s", attemptState, leaseState)
	}
}
