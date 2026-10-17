//go:build integration

package durable

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerReconnectsAfterTransientConnectionFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	engine, definition := newIntegrationEngine(t, SystemClock{}, 1)
	execution, err := Start(ctx, engine, definition, integrationInput{Value: "reconnect"})
	if err != nil {
		t.Fatal(err)
	}
	var disconnected atomic.Bool
	disconnected.Store(true)
	config := engine.pool.Config()
	config.MaxConns = 1
	config.AfterConnect = func(context.Context, *pgx.Conn) error {
		if disconnected.Load() {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("injected connection outage")}
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	clock := &runtimeClock{ManualClock: NewManualClock(execution.CreatedAt), created: make(chan time.Duration, 20)}
	// Start and runtime must share persisted time, while connection waiting uses
	// real context deadlines and bounded observation timeouts.
	runtimeEngine, err := New(pool, engine.registry, clock)
	if err != nil {
		t.Fatal(err)
	}
	// This verifies reconnect behavior, not connection/claim latency. Leave room
	// for the real PostgreSQL round trips when other verification work is active.
	worker, err := NewWorker(runtimeEngine, WorkerConfig{ID: "reconnect", DatabaseTimeout: 500 * time.Millisecond}, Handle("step_1", func(_ context.Context, input integrationInput) (string, error) { return input.Value, nil }))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	runtimeReceive(t, clock.created)
	if interval := runtimeReceive(t, clock.created); interval != 100*time.Millisecond {
		t.Fatalf("backoff: %s", interval)
	}
	if health := worker.Health(); health.Healthy || health.ConsecutiveFailures != 1 {
		t.Fatalf("outage health: %#v", health)
	}
	disconnected.Store(false)
	clock.Advance(100 * time.Millisecond)
	deadline := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	for {
		result, err := engine.Workflow(ctx, execution.ID)
		if err != nil {
			t.Fatal(err)
		}
		if result.State == WorkflowCompleted && worker.Health().Healthy {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("worker exited during reconnect: %v; health=%#v", err, worker.Health())
		case <-ctx.Done():
			t.Fatalf("worker did not reconnect: %#v", worker.Health())
		case <-deadline.C:
		}
	}
	cancel()
	if err := runtimeReceive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
