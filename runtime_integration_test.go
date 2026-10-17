//go:build integration

package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
)

type observedClock struct {
	*durable.ManualClock
	created chan time.Duration
}

func (c *observedClock) NewTicker(interval time.Duration) durable.Ticker {
	ticker := c.ManualClock.NewTicker(interval)
	c.created <- interval
	return ticker
}

func loopEngine(t *testing.T) (*durable.Engine, *observedClock, durable.WorkflowExecution, *pgxpool.Pool) {
	t.Helper()
	clock := &observedClock{ManualClock: durable.NewManualClock(time.Unix(0, 0)), created: make(chan time.Duration, 10)}
	registry := durable.NewRegistry()
	definition := durable.Define("loop", func(input string) ([]durable.ActivitySpec, error) {
		activity, err := durable.NewActivity("effect", input)
		return []durable.ActivitySpec{activity}, err
	})
	if err := durable.Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	pool := testdb.Pool(t)
	engine, err := durable.New(pool, registry, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	execution, err := durable.Start(t.Context(), engine, definition, "input")
	if err != nil {
		t.Fatal(err)
	}
	return engine, clock, execution, pool
}

func loopWorker(t *testing.T, engine *durable.Engine, handler func(context.Context, string) (string, error)) *durable.Worker {
	t.Helper()
	worker, err := durable.NewWorker(engine, durable.WorkerConfig{
		ID: "loop-worker", LeaseDuration: 10 * time.Second, HeartbeatInterval: 3 * time.Second, PollInterval: time.Second,
	}, durable.Handle("effect", handler))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for loop progress")
		var zero T
		return zero
	}
}

// Waiting on database state observes progress; fake time owns all timing decisions.
func await(t *testing.T, predicate func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !predicate() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for persisted state")
		case <-ticker.C:
		}
	}
}

func TestWorkerRunOnceHeartbeatsWithControlledTime(t *testing.T) {
	engine, clock, execution, pool := loopEngine(t)
	started, finish := make(chan struct{}), make(chan struct{})
	worker := loopWorker(t, engine, func(ctx context.Context, input string) (string, error) {
		close(started)
		select {
		case <-finish:
			return input, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(ctx); done <- err }()
	receive(t, started)
	receive(t, clock.created)
	clock.Advance(9 * time.Second)
	// Observe a completed heartbeat through a second connection, before crossing
	// the original expiry. No scheduling sleep determines the assertion.
	await(t, func() bool {
		var heartbeat time.Time
		err := pool.QueryRow(t.Context(), `SELECT l.heartbeat_at FROM leases l
			JOIN activities a ON a.id = l.activity_id WHERE a.workflow_id = $1 AND l.state = 'active'`, execution.ID).Scan(&heartbeat)
		return err == nil && heartbeat.Equal(clock.Now())
	})
	clock.Advance(2 * time.Second)
	if n, err := engine.RecoverAbandoned(t.Context(), 10); err != nil || n != 0 {
		t.Fatalf("healthy lease recovered: n=%d err=%v", n, err)
	}
	close(finish)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	state, err := engine.Workflow(t.Context(), execution.ID)
	if err != nil || state.State != durable.WorkflowCompleted {
		t.Fatalf("workflow = %#v, err = %v", state, err)
	}
}

func TestWorkerLeaseLossCancelsItsHandler(t *testing.T) {
	engine, clock, _, _ := loopEngine(t)
	cancelled := make(chan struct{})
	worker := loopWorker(t, engine, func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		close(cancelled)
		return "", ctx.Err()
	})
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(t.Context()); done <- err }()
	receive(t, clock.created)
	clock.Advance(10 * time.Second)
	if err := receive(t, done); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("error = %v, want lease loss", err)
	}
	receive(t, cancelled)
	if n, err := engine.RecoverAbandoned(t.Context(), 10); err != nil || n != 1 {
		t.Fatalf("recovery = %d, %v", n, err)
	}
}

func TestSchedulerRunRecoversOnControlledTick(t *testing.T) {
	engine, clock, execution, _ := loopEngine(t)
	if _, err := engine.ClaimActivity(t.Context(), "vanished", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	scheduler, err := durable.NewScheduler(engine, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	receive(t, clock.created)
	clock.Advance(10 * time.Second)
	await(t, func() bool {
		activities, err := engine.Activities(t.Context(), execution.ID)
		return err == nil && activities[0].State == durable.ActivityReady
	})
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown = %v", err)
	}
}

func TestWorkerRunProcessesWorkAndStopsOnContextCancellation(t *testing.T) {
	engine, _, execution, _ := loopEngine(t)
	worker := loopWorker(t, engine, func(_ context.Context, input string) (string, error) { return input, nil })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	await(t, func() bool {
		execution, err := engine.Workflow(t.Context(), execution.ID)
		return err == nil && execution.State == durable.WorkflowCompleted
	})
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown = %v", err)
	}
}

func TestWorkerRunContinuesAfterRecordedHandlerFailure(t *testing.T) {
	engine, _, execution, _ := loopEngine(t)
	wantErr := errors.New("fake handler failed")
	definition := durable.Define[string]("loop", nil)
	second, err := durable.Start(t.Context(), engine, definition, "success")
	if err != nil {
		t.Fatal(err)
	}
	worker := loopWorker(t, engine, func(_ context.Context, input string) (string, error) {
		if input == "input" {
			return "", wantErr
		}
		return input, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	await(t, func() bool {
		first, err1 := engine.Workflow(t.Context(), execution.ID)
		last, err2 := engine.Workflow(t.Context(), second.ID)
		return err1 == nil && err2 == nil && first.State == durable.WorkflowFailed && last.State == durable.WorkflowCompleted && worker.Health().Healthy
	})
	if health := worker.Health(); !health.Running || health.LastError != nil {
		t.Fatalf("task failure poisoned health: %#v", health)
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestUnresponsiveHandlerCannotOverlapAnotherInvocation(t *testing.T) {
	engine, clock, _, _ := loopEngine(t)
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(release); receive(t, finished) }()
	worker := loopWorker(t, engine, func(context.Context, string) (string, error) {
		close(started)
		defer close(finished)
		<-release
		return "late", nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := worker.RunOnce(ctx); done <- err }()
	receive(t, started)
	receive(t, clock.created)
	if _, err := worker.RunOnce(t.Context()); !errors.Is(err, durable.ErrWorkerBusy) {
		t.Fatalf("concurrent invocation: %v", err)
	}
	cancel()
	for interval := receive(t, clock.created); interval != 5*time.Second; interval = receive(t, clock.created) {
		if interval != 5*time.Minute {
			t.Fatalf("unexpected runtime timer = %s", interval)
		}
	}
	clock.Advance(5 * time.Second)
	if err := receive(t, done); !errors.Is(err, durable.ErrHandlerUnresponsive) || !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := worker.RunOnce(t.Context()); !errors.Is(err, durable.ErrHandlerUnresponsive) {
		t.Fatalf("overlapping handler permitted: %v", err)
	}
}
