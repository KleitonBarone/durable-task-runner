package durable

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type runtimeClock struct {
	*ManualClock
	created chan time.Duration
}

func (c *runtimeClock) NewTicker(interval time.Duration) Ticker {
	ticker := c.ManualClock.NewTicker(interval)
	c.created <- interval
	return ticker
}

func runtimeReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not progress")
		var zero T
		return zero
	}
}

func TestRuntimeReconnectBudgetUsesControlledBackoff(t *testing.T) {
	clock := &runtimeClock{ManualClock: NewManualClock(time.Unix(1, 0)), created: make(chan time.Duration, 10)}
	health := &runtimeHealth{}
	calls := make(chan struct{}, 10)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		done <- runtimeLoop(ctx, clock, time.Second, health, func(context.Context) (bool, error) { calls <- struct{}{}; return false, io.EOF })
	}()
	if got := runtimeReceive(t, clock.created); got != time.Second {
		t.Fatal(got)
	}
	for i := range 5 {
		runtimeReceive(t, calls)
		want := 100 * time.Millisecond * time.Duration(1<<i)
		if got := runtimeReceive(t, clock.created); got != want {
			t.Fatalf("backoff = %s, want %s", got, want)
		}
		if status := health.snapshot(); status.Healthy || status.ConsecutiveFailures != i+1 {
			t.Fatalf("health = %#v", status)
		}
		clock.Advance(want)
	}
	runtimeReceive(t, calls)
	if err := runtimeReceive(t, done); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if status := health.snapshot(); status.Running || status.ConsecutiveFailures != 6 {
		t.Fatalf("exhausted health = %#v", status)
	}
}

func TestTransientDatabaseErrorClassification(t *testing.T) {
	for _, code := range []string{"08006", "57P01", "40001", "40P01"} {
		if !transientDatabaseError(&pgconn.PgError{Code: code}) {
			t.Fatal(code)
		}
	}
	for _, code := range []string{"28P01", "42P01", "23505"} {
		if transientDatabaseError(&pgconn.PgError{Code: code}) {
			t.Fatal(code)
		}
	}
	if transientDatabaseError(context.Canceled) || !transientDatabaseError(context.DeadlineExceeded) {
		t.Fatal("context classification")
	}
}

func TestWorkerDefaultsAndValidation(t *testing.T) {
	engine := &Engine{registry: NewRegistry()}
	handler := Handle("echo", func(context.Context, string) (string, error) { return "", nil })
	worker, err := NewWorker(engine, WorkerConfig{ID: "defaults"}, handler)
	if err != nil || worker.config.LeaseDuration != 30*time.Second || worker.config.DatabaseTimeout != 5*time.Second {
		t.Fatalf("defaults = %#v %v", worker, err)
	}
	for _, config := range []WorkerConfig{{ID: "negative", PollInterval: -1}, {ID: "negative", LeaseDuration: -1}, {ID: "negative", HeartbeatInterval: -1}, {ID: "headroom", LeaseDuration: time.Second, HeartbeatInterval: time.Second / 2, DatabaseTimeout: time.Second / 2}, {ID: "negative", ShutdownGracePeriod: -1}} {
		if _, err := NewWorker(engine, config, handler); err == nil {
			t.Fatalf("accepted %#v", config)
		}
	}
}
