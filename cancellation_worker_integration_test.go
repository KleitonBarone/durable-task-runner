//go:build integration

package durable

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerHonorsDurableCancellation(t *testing.T) {
	for _, cooperative := range []bool{true, false} {
		name := "cooperative"
		if !cooperative {
			name = "ignores_context"
		}
		t.Run(name, func(t *testing.T) {
			clock := &runtimeClock{ManualClock: NewManualClock(time.Unix(1, 0)), created: make(chan time.Duration, 10)}
			engine, definition := newIntegrationEngine(t, clock, 2)
			execution, err := Start(t.Context(), engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			started, cancelled, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseHandler()
			var laterCalls atomic.Int32
			worker, err := NewWorker(engine, WorkerConfig{ID: "cancel-worker"},
				Handle("step_1", func(ctx context.Context, input integrationInput) (integrationInput, error) {
					defer close(returned)
					close(started)
					<-ctx.Done()
					close(cancelled)
					if !cooperative {
						<-release
						return input, nil
					}
					return integrationInput{}, ctx.Err()
				}),
				Handle("step_2", func(_ context.Context, input integrationInput) (integrationInput, error) {
					laterCalls.Add(1)
					return input, nil
				}))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := worker.RunOnce(t.Context()); done <- err }()
			runtimeReceive(t, started)
			runtimeReceive(t, clock.created)
			runtimeReceive(t, clock.created)
			if _, err := engine.Cancel(t.Context(), execution.ID, "operator", "stop handler"); err != nil {
				t.Fatal(err)
			}
			clock.Advance(10 * time.Second)
			runtimeReceive(t, cancelled)
			// Settlement is independent of whether the handler returned.
			if _, err := engine.RecoverAbandoned(t.Context(), 10); err != nil {
				t.Fatal(err)
			}
			result, err := engine.Workflow(t.Context(), execution.ID)
			if err != nil || result.State != WorkflowCancelled {
				t.Fatalf("settled: %#v %v", result, err)
			}
			if !cooperative {
				if interval := runtimeReceive(t, clock.created); interval != 5*time.Second {
					t.Fatalf("grace=%s", interval)
				}
				clock.Advance(5 * time.Second)
			}
			err = runtimeReceive(t, done)
			if !errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrHandlerUnresponsive) == cooperative {
				t.Fatalf("worker cancellation: %v", err)
			}
			if !cooperative {
				if _, err := worker.RunOnce(t.Context()); !errors.Is(err, ErrHandlerUnresponsive) {
					t.Fatalf("worker reuse: %v", err)
				}
			}
			releaseHandler()
			runtimeReceive(t, returned)
			if laterCalls.Load() != 0 {
				t.Fatal("cancelled successor ran")
			}
			result, err = engine.Workflow(t.Context(), execution.ID)
			if err != nil || result.State != WorkflowCancelled || result.Output != nil {
				t.Fatalf("late handler outcome: %#v %v", result, err)
			}
		})
	}
}
