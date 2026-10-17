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
	rows, err := tx.Query(ctx, `SELECT w.id FROM workflow_executions w WHERE w.state='running' AND (w.deadline_at <= $1 OR EXISTS (SELECT 1 FROM activities a JOIN leases l ON l.activity_id=a.id WHERE a.workflow_id=w.id AND l.state='active' AND l.expires_at <= $1) OR EXISTS (SELECT 1 FROM activities a WHERE a.workflow_id=w.id AND a.state='retry_wait' AND a.available_at <= $1)) ORDER BY w.id FOR UPDATE OF w SKIP LOCKED LIMIT $2`, searchTime, limit)
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
	rows, err := tx.Query(ctx, `SELECT l.id, l.activity_id, l.attempt_id, l.expires_at,at.deadline_at FROM leases l JOIN activities a ON a.id=l.activity_id JOIN attempts at ON at.id=l.attempt_id WHERE a.workflow_id=$1 AND l.state='active' AND a.state='running' AND at.state='running' FOR UPDATE OF a, at, l`, workflowID)
	if err != nil {
		return 0, err
	}
	type ownership struct {
		leaseID, activityID, attemptID string
		expiresAt                      time.Time
		deadlineAt                     time.Time
	}
	var items []ownership
	for rows.Next() {
		var item ownership
		if err := rows.Scan(&item.leaseID, &item.activityID, &item.attemptID, &item.expiresAt, &item.deadlineAt); err != nil {
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
	var deadline time.Time
	if err := tx.QueryRow(ctx, `SELECT deadline_at FROM workflow_executions WHERE id=$1`, workflowID).Scan(&deadline); err != nil {
		return 0, err
	}
	if !deadline.After(now) {
		return 1, e.timeoutWorkflow(ctx, tx, workflowID, "workflow_deadline", now)
	}
	recovered := 0
	for _, item := range items {
		if item.expiresAt.After(now) {
			continue
		}
		d := delivery{workflowID: workflowID, activityID: item.activityID, attemptID: item.attemptID, leaseID: item.leaseID}
		state, message := AttemptAbandoned, "lease_expired"
		if !item.deadlineAt.After(now) {
			state, message = AttemptTimedOut, "attempt_deadline"
		}
		if err := e.recordAttemptFailure(ctx, tx, d, state, message, false, now); err != nil {
			return 0, err
		}
		recovered++
	}
	rows, err = tx.Query(ctx, `SELECT a.id FROM activities a JOIN workflow_executions w ON w.id=a.workflow_id WHERE a.workflow_id=$1 AND w.state='running' AND a.state='retry_wait' AND a.available_at <= $2 FOR UPDATE OF a`, workflowID, now)
	if err != nil {
		return 0, err
	}
	var due []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, id := range due {
		if _, err := tx.Exec(ctx, `UPDATE activities SET state='ready',updated_at=$2 WHERE id=$1`, id, now); err != nil {
			return 0, err
		}
		if err := appendHistory(ctx, tx, workflowID, &id, nil, "activity.ready", map[string]any{"reason": "retry_due"}, now); err != nil {
			return 0, err
		}
	}
	return recovered + len(due), nil
}
