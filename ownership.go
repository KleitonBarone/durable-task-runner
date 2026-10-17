package durable

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (SystemClock) databaseTime() {}

// now is sampled after mutation locks. Only explicitly injected test clocks
// bypass PostgreSQL time; runtime timers do not decide durable ownership.
func (e *Engine) now(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	if _, production := e.clock.(interface{ databaseTime() }); !production {
		return e.clock.Now().UTC(), nil
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}
	return now.UTC(), nil
}

func lockOwnershipWorkflow(ctx context.Context, tx pgx.Tx, token string) (string, WorkflowState, error) {
	var id string
	var state WorkflowState
	err := tx.QueryRow(ctx, `SELECT w.id, w.state FROM workflow_executions w JOIN activities a ON a.workflow_id=w.id JOIN leases l ON l.activity_id=a.id WHERE l.token=$1 FOR UPDATE OF w`, token).Scan(&id, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrLeaseLost
	}
	if err != nil {
		return "", "", fmt.Errorf("lock ownership workflow: %w", err)
	}
	return id, state, nil
}
