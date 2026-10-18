//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
)

func TestObservationsFollowCommitAndRedactSensitiveData(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	base, definition := newIntegrationEngine(t, clock, 1)
	var mu sync.Mutex
	var events []durable.Observation
	var logs bytes.Buffer
	logger := durable.SlogObserver(slog.New(slog.NewJSONHandler(&logs, nil)))
	engine, err := newTestEngine(base.pool, base.registry, clock, durable.WithObserver(durable.ObserverFunc(func(ctx context.Context, event durable.Observation) {
		if event.Type == "workflow.started" {
			var exists bool
			if err := base.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_executions WHERE id=$1)`, event.WorkflowID).Scan(&exists); err != nil || !exists {
				t.Errorf("observer ran before commit: %v %v", exists, err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
		logger.Observe(ctx, event)
	})), durable.WithObserver(durable.ObserverFunc(func(context.Context, durable.Observation) { panic("ignored diagnostic failure") })))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.pool.Exec(t.Context(), `CREATE FUNCTION reject_observation_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected history failure'; END $$;CREATE TRIGGER observation_fixture BEFORE INSERT ON execution_history FOR EACH ROW EXECUTE FUNCTION reject_observation_fixture()`); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{Value: "private-input"}); err == nil {
		t.Fatal("injected failure succeeded")
	}
	if len(events) != 0 {
		t.Fatal("rolled back transitions were observed")
	}
	if _, err := engine.pool.Exec(t.Context(), `DROP TRIGGER observation_fixture ON execution_history`); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{Value: "private-input"}, durable.WithStartKey("private-key"))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second * 2)
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "worker"}, durable.Handle("step_1", func(_ context.Context, _ integrationInput) (string, error) { return "", errors.New("private-error") }))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOnce(t.Context()); !worked || err == nil {
		t.Fatalf("recorded failure: %v %v", worked, err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != durable.WorkflowFailed {
		t.Fatalf("observer panic changed outcome: %#v %v", state, err)
	}
	mu.Lock()
	defer mu.Unlock()
	types := map[string]bool{}
	for _, event := range events {
		types[event.Type] = true
		if event.Type == "activity.claimed" && (event.ReadyAge != 2*time.Second || event.Duration < 0 || event.Version != "1" || event.StepKey != "step-0" || event.Kind != durable.ActivityForward) {
			t.Fatalf("claim measurement: %#v", event)
		}
	}
	for _, required := range []string{"workflow.started", "activity.claimed", "handler.finished", "activity.failed", "workflow.failed"} {
		if !types[required] {
			t.Fatalf("missing %s: %#v", required, types)
		}
	}
	if text := logs.String(); strings.Contains(text, "private-") || strings.Contains(text, "durable:") || strings.Contains(text, "lease_token") {
		t.Fatalf("diagnostic leaked data: %s", text)
	}
}

func TestLeaseLossIsObservedOnceAfterLostAcknowledgement(t *testing.T) {
	clock := durable.NewManualClock(time.Unix(1000, 0))
	base, definition := newIntegrationEngine(t, clock, 1)
	var mu sync.Mutex
	var losses int
	engine, err := newTestEngine(base.pool, base.registry, clock, durable.WithObserver(durable.ObserverFunc(func(_ context.Context, event durable.Observation) {
		if event.Type == "lease.lost" {
			mu.Lock()
			losses++
			mu.Unlock()
		}
	})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Start(t.Context(), engine.Engine, definition, integrationInput{}); err != nil {
		t.Fatal(err)
	}
	worker, err := durable.NewWorker(engine.Engine, durable.WorkerConfig{ID: "lost"}, durable.Handle("step_1", func(context.Context, integrationInput) (string, error) {
		clock.Advance(time.Second * 30)
		return "finished", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(t.Context()); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if losses != 1 {
		t.Fatalf("lease loss observations = %d", losses)
	}
}

func TestObservationsExplainRecoveryBudgetAndCleanupOutcomes(t *testing.T) {
	engine, clock, execution := compensationExecution(t, durable.RetryPolicy{MaxAttempts: 1})
	var events []durable.Observation
	observed, err := newTestEngine(engine.pool, engine.registry, clock, durable.WithObserver(durable.ObserverFunc(func(_ context.Context, event durable.Observation) { events = append(events, event) })))
	if err != nil {
		t.Fatal(err)
	}
	completeForwardSteps(t, observed, 2)
	if _, err := observed.Cancel(t.Context(), execution.ID, "private-actor", "private-cancel-reason"); err != nil {
		t.Fatal(err)
	}
	if _, err := observed.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := observed.ClaimActivity(t.Context(), "crashed-cleanup", 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	clock.Advance(500 * time.Millisecond)
	if _, err := observed.RecoverAbandoned(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	var exhausted bool
	for _, event := range events {
		types[event.Type] = true
		if strings.Contains(event.Reason, "private-") {
			t.Fatal("private reason observed")
		}
		exhausted = exhausted || event.Reason == "attempt_budget_exhausted"
	}
	for _, kind := range []string{"workflow.cancel_requested", "workflow.compensating", "attempt.abandoned", "activity.failed", "workflow.compensation_failed"} {
		if !types[kind] {
			t.Fatalf("missing %s: %#v", kind, types)
		}
	}
	if !exhausted {
		t.Fatal("cleanup claim budget exhaustion was not identified")
	}
}
