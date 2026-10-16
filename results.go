package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type activityContextKey struct{}
type activityContext struct {
	engine *Engine
	claim  Claim
}

// Result reads a completed earlier step in the current handler's workflow.
// The handler context carries execution identity; callers cannot read a future
// step or a result with a different activity type.
func (a ActivityDefinition[Input, Output]) Result(ctx context.Context, key string) (Output, error) {
	var output Output
	current, ok := ctx.Value(activityContextKey{}).(activityContext)
	if !ok {
		return output, fmt.Errorf("%w: Result requires a handler context", ErrResultUnavailable)
	}
	var name, contract string
	var position int
	var state ActivityState
	var raw json.RawMessage
	err := current.engine.pool.QueryRow(ctx, `SELECT name, contract, position, state, output FROM activities WHERE workflow_id = $1 AND step_key = $2`, current.claim.WorkflowID, key).Scan(&name, &contract, &position, &state, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return output, fmt.Errorf("%w: step %q", ErrNotFound, key)
	}
	if err != nil {
		return output, fmt.Errorf("read step result: %w", err)
	}
	if position >= current.claim.Position || state != ActivityCompleted {
		return output, ErrResultUnavailable
	}
	return a.decode(name, contract, raw)
}

// WorkflowResult reads the last step's output after successful completion.
func (a ActivityDefinition[Input, Output]) WorkflowResult(ctx context.Context, engine *Engine, workflowID string) (Output, error) {
	var output Output
	var state WorkflowState
	var name, contract string
	var raw json.RawMessage
	err := engine.pool.QueryRow(ctx, `SELECT w.state, a.name, a.contract, w.output FROM workflow_executions w JOIN activities a ON a.workflow_id = w.id WHERE w.id = $1 ORDER BY a.position DESC LIMIT 1`, workflowID).Scan(&state, &name, &contract, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return output, ErrNotFound
	}
	if err != nil {
		return output, fmt.Errorf("read workflow result: %w", err)
	}
	if state != WorkflowCompleted {
		return output, ErrResultUnavailable
	}
	return a.decode(name, contract, raw)
}

func (a ActivityDefinition[Input, Output]) decode(name, contract string, raw json.RawMessage) (Output, error) {
	var output Output
	if name != a.name || contract != a.contract() {
		return output, ErrTypeMismatch
	}
	if err := json.Unmarshal(raw, &output); err != nil {
		return output, fmt.Errorf("decode stored result: %w", err)
	}
	return output, nil
}

// Attempts lists attempts for one activity in attempt-number order.
func (e *Engine) Attempts(ctx context.Context, activityID string) ([]Attempt, error) {
	var exists bool
	if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM activities WHERE id = $1)`, activityID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("read activity: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := e.pool.Query(ctx, `SELECT id, activity_id, number, worker_id, state, COALESCE(error_text, ''), started_at, ended_at FROM attempts WHERE activity_id = $1 ORDER BY number`, activityID)
	if err != nil {
		return nil, fmt.Errorf("read attempts: %w", err)
	}
	defer rows.Close()
	var attempts []Attempt
	for rows.Next() {
		var attempt Attempt
		if err := rows.Scan(&attempt.ID, &attempt.ActivityID, &attempt.Number, &attempt.WorkerID, &attempt.State, &attempt.Error, &attempt.StartedAt, &attempt.EndedAt); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}
