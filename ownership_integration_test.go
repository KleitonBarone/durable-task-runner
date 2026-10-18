//go:build integration

package durable

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/KleitonBarone/durable-task-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

type skewedSystemClock struct{ SystemClock }

func (skewedSystemClock) Now() time.Time { return time.Now().Add(24 * time.Hour) }

func TestProductionOwnershipIgnoresHostClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	engine, definition := newIntegrationEngine(t, skewedSystemClock{}, 1)
	execution, err := Start(ctx, engine, definition, integrationInput{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := engine.ClaimActivity(ctx, "skewed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var dbNow time.Time
	if err := engine.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		t.Fatal(err)
	}
	if delta := claim.LeaseExpiresAt.Sub(dbNow); delta <= 0 || delta > time.Minute {
		t.Fatalf("lease uses host time: %s", delta)
	}
	if delta := dbNow.Sub(execution.CreatedAt); delta < 0 || delta > time.Minute {
		t.Fatalf("start uses host time: %s", delta)
	}
}

func TestOwnershipTimeIsSampledAfterLockWait(t *testing.T) {
	for _, operation := range []string{"heartbeat", "completion"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			clock := NewManualClock(time.Unix(1, 0))
			engine, definition := newIntegrationEngine(t, clock, 1)
			execution, err := Start(ctx, engine, definition, integrationInput{})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := engine.ClaimActivity(ctx, "old", 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			config := engine.pool.Config()
			config.ConnConfig.RuntimeParams["application_name"] = "ownership-" + execution.ID
			pool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			other, err := New(pool, engine.registry, clock)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := engine.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, `SELECT id FROM workflow_executions WHERE id=$1 FOR UPDATE`, execution.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				if operation == "heartbeat" {
					_, err := other.Heartbeat(ctx, claim, time.Minute)
					done <- err
				} else {
					done <- other.CompleteActivity(ctx, claim, []byte(`"late"`))
				}
			}()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting bool
				if err := engine.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, "ownership-"+execution.ID).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("mutation did not wait for workflow lock")
				case <-ticker.C:
				}
			}
			clock.Advance(10 * time.Second)
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := runtimeReceive(t, done); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale pre-lock timestamp accepted: %v", err)
			}
		})
	}
}

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
