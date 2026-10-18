//go:build integration

package integration

import (
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fixture owns its dependencies so tests never reach into the engine.
type integrationEngine struct {
	*durable.Engine
	pool     *pgxpool.Pool
	registry *durable.Registry
	clock    durable.Clock
}

func newTestEngine(pool *pgxpool.Pool, registry *durable.Registry, clock durable.Clock, options ...durable.EngineOption) (*integrationEngine, error) {
	engine, err := durable.New(pool, registry, clock, options...)
	if err != nil {
		return nil, err
	}
	return &integrationEngine{Engine: engine, pool: pool, registry: registry, clock: clock}, nil
}

type runtimeClock struct {
	*durable.ManualClock
	created chan time.Duration
}

func (c *runtimeClock) NewTicker(interval time.Duration) durable.Ticker {
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
