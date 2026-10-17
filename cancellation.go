package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// Cancel durably prevents further forward claims and fences active ownership.
// The scheduler settles the request without waiting for handler cooperation.
// Repeated and terminal requests preserve the original outcome and metadata.
func (e *Engine) Cancel(ctx context.Context, id, actor, reason string) (WorkflowExecution, error) {
	if strings.TrimSpace(actor) == "" || len(actor) > 200 || len(reason) > 16*1024 || !utf8.ValidString(actor) || !utf8.ValidString(reason) || strings.ContainsRune(actor, 0) || strings.ContainsRune(reason, 0) {
		return WorkflowExecution{}, fmt.Errorf("%w: cancellation requires a UTF-8 actor of 1-200 bytes and reason up to 16 KiB, without NUL", ErrInvalidInput)
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return WorkflowExecution{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state WorkflowState
	if err := tx.QueryRow(ctx, `SELECT state FROM workflow_executions WHERE id=$1 FOR UPDATE`, id).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return WorkflowExecution{}, ErrNotFound
	} else if err != nil {
		return WorkflowExecution{}, err
	}
	if state == WorkflowRunning {
		now, err := e.now(ctx, tx)
		if err != nil {
			return WorkflowExecution{}, err
		}
		var deadlinePassed bool
		if err := tx.QueryRow(ctx, `SELECT deadline_at <= $2 FROM workflow_executions WHERE id=$1`, id, now).Scan(&deadlinePassed); err != nil {
			return WorkflowExecution{}, err
		}
		if deadlinePassed {
			if err := e.timeoutWorkflow(ctx, tx, id, "workflow_deadline", now); err != nil {
				return WorkflowExecution{}, err
			}
		} else {
			if err := ValidateWorkflowTransition(state, WorkflowCancelRequested); err != nil {
				return WorkflowExecution{}, err
			}
			if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state='cancel_requested',cancel_actor=$2,cancel_reason=$3,cancel_requested_at=$4,updated_at=$4,terminal_reason='cancelled' WHERE id=$1`, id, actor, reason, now); err != nil {
				return WorkflowExecution{}, err
			}
			if err := appendHistory(ctx, tx, id, nil, nil, "workflow.cancel_requested", map[string]any{"actor": actor, "reason": reason}, now); err != nil {
				return WorkflowExecution{}, err
			}
			if err := stopActivities(ctx, tx, id, ActivityCancelled, AttemptCancelled, "cancelled", now); err != nil {
				return WorkflowExecution{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowExecution{}, err
	}
	return e.Workflow(ctx, id)
}
