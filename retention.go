package durable

import (
	"context"
	"fmt"
	"time"
)

// RetentionPolicy selects terminal executions strictly older than CompletedBefore.
// The cutoff is required and cannot be in the future. Limit defaults to 100, maximum 500.
type RetentionPolicy struct {
	CompletedBefore time.Time
	Limit           int
}

// RetentionResult identifies executions whose details were deleted atomically.
type RetentionResult struct {
	Deleted      int
	ExecutionIDs []string
}

// PurgeTerminal deliberately deletes terminal execution details and history in
// bounded workflow batches. It is never called automatically by the scheduler.
// Keyed input moves to the deduplication record and remains retained indefinitely.
func (e *Engine) PurgeTerminal(ctx context.Context, policy RetentionPolicy) (RetentionResult, error) {
	if policy.Limit == 0 {
		policy.Limit = 100
	}
	if policy.CompletedBefore.IsZero() || policy.Limit < 1 || policy.Limit > 500 {
		return RetentionResult{}, fmt.Errorf("%w: retention requires a cutoff and limit of 1-500", ErrInvalidInput)
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return RetentionResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT id,definition_name,definition_version,start_key FROM workflow_executions WHERE state IN ('completed','failed','timed_out','cancelled','compensation_failed') AND completed_at<$1 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2`, policy.CompletedBefore, policy.Limit)
	if err != nil {
		return RetentionResult{}, err
	}
	type candidate struct {
		id, name, version string
		key               *string
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.name, &item.version, &item.key); err != nil {
			rows.Close()
			return RetentionResult{}, err
		}
		candidates = append(candidates, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return RetentionResult{}, err
	}
	now, err := e.now(ctx, tx)
	if err != nil {
		return RetentionResult{}, err
	}
	if policy.CompletedBefore.After(now) {
		return RetentionResult{}, fmt.Errorf("%w: retention cutoff cannot be in the future", ErrInvalidInput)
	}
	result := RetentionResult{ExecutionIDs: make([]string, 0, len(candidates))}
	for _, item := range candidates {
		if item.key != nil {
			updated, err := tx.Exec(ctx, `UPDATE start_keys k SET input=w.input,purged_at=$2 FROM workflow_executions w WHERE k.execution_id=$1 AND w.id=k.execution_id AND k.purged_at IS NULL AND k.definition_name=$3 AND k.definition_version=$4 AND k.start_key=$5`, item.id, now, item.name, item.version, item.key)
			if err != nil {
				return RetentionResult{}, err
			}
			if updated.RowsAffected() != 1 {
				return RetentionResult{}, fmt.Errorf("deduplication record is missing for execution %s", item.id)
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM workflow_executions WHERE id=$1`, item.id); err != nil {
			return RetentionResult{}, err
		}
		result.ExecutionIDs = append(result.ExecutionIDs, item.id)
		if observed, ok := tx.(*observedTx); ok {
			observed.events = append(observed.events, Observation{Type: "workflow.purged", At: now, WorkflowID: item.id, Version: item.version, Outcome: "purged", Reason: "retention"})
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RetentionResult{}, err
	}
	result.Deleted = len(result.ExecutionIDs)
	return result, nil
}
