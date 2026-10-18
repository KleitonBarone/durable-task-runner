//go:build integration

package durable

import (
	"context"
	"errors"
	"fmt"
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

func completeForwardSteps(t *testing.T, engine *Engine, count int) {
	t.Helper()
	for range count {
		claim, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if claim.Kind != ActivityForward {
			t.Fatalf("forward kind: %#v", claim)
		}
		if err := engine.CompleteActivity(t.Context(), claim, claim.ActivityInput); err != nil {
			t.Fatal(err)
		}
	}
}

func failForward(t *testing.T, engine *Engine) {
	t.Helper()
	claim, err := engine.ClaimActivity(t.Context(), "forward", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.FailActivity(t.Context(), claim, errors.New("forward failed")); err != nil {
		t.Fatal(err)
	}
}
