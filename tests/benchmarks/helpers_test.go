//go:build integration

package benchmarks

import (
	"context"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

type integrationInput struct {
	Value string `json:"value"`
}

type benchmarkEngine struct {
	*durable.Engine
	pool *pgxpool.Pool
}

func newBenchmarkEngine(t testing.TB) (*benchmarkEngine, durable.Definition[integrationInput]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := testdb.Pool(t)
	registry := durable.NewRegistry()
	definition := durable.Define("integration_workflow", func(input integrationInput) ([]durable.ActivitySpec, error) {
		step, err := durable.NewActivity("step_1", input)
		return []durable.ActivitySpec{step}, err
	})
	if err := durable.Register(registry, definition); err != nil {
		t.Fatal(err)
	}
	engine, err := durable.New(pool, registry, durable.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return &benchmarkEngine{Engine: engine, pool: pool}, definition
}
