package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (e *Engine) RecoverAbandoned(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("recovery limit must be positive")
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	searchTime, err := e.now(ctx, tx)
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT w.id FROM workflow_executions w WHERE w.state='running' AND EXISTS (SELECT 1 FROM activities a JOIN leases l ON l.activity_id=a.id WHERE a.workflow_id=w.id AND l.state='active' AND l.expires_at <= $1) ORDER BY w.id FOR UPDATE OF w SKIP LOCKED LIMIT $2`, searchTime, limit)
	if err != nil {
		return 0, fmt.Errorf("select recovery workflows: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, id := range ids {
		n, err := e.recoverWorkflow(ctx, tx, id)
		if err != nil {
			return 0, err
		}
		recovered += n
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit recovery: %w", err)
	}
	return recovered, nil
}

// The caller owns the workflow row before this function locks its ownership rows.
func (e *Engine) recoverWorkflow(ctx context.Context, tx pgx.Tx, workflowID string) (int, error) {
	rows, err := tx.Query(ctx, `SELECT l.id, l.activity_id, l.attempt_id, l.expires_at FROM leases l JOIN activities a ON a.id=l.activity_id JOIN attempts at ON at.id=l.attempt_id WHERE a.workflow_id=$1 AND l.state='active' AND a.state='running' AND at.state='running' FOR UPDATE OF a, at, l`, workflowID)
	if err != nil {
		return 0, err
	}
	type ownership struct {
		leaseID, activityID, attemptID string
		expiresAt                      time.Time
	}
	var items []ownership
	for rows.Next() {
		var item ownership
		if err := rows.Scan(&item.leaseID, &item.activityID, &item.attemptID, &item.expiresAt); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	now, err := e.now(ctx, tx)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, item := range items {
		if item.expiresAt.After(now) {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE leases SET state='expired', ended_at=$2 WHERE id=$1`, item.leaseID, now); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET state='abandoned', ended_at=$2 WHERE id=$1`, item.attemptID, now); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE activities SET state='ready', updated_at=$2 WHERE id=$1`, item.activityID, now); err != nil {
			return 0, err
		}
		if err := appendHistory(ctx, tx, workflowID, &item.activityID, &item.attemptID, "attempt.abandoned", map[string]any{"reason": "lease_expired"}, now); err != nil {
			return 0, err
		}
		if err := appendHistory(ctx, tx, workflowID, &item.activityID, nil, "activity.recovered", map[string]any{}, now); err != nil {
			return 0, err
		}
		recovered++
	}
	return recovered, nil
}
