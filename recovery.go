package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RecoverAbandoned reconciles at most limit workflows with expired ownership,
// deadlines, cancellation requests, or due retries. It returns reconciled work
// items, which need not equal the workflow count. Prefer Scheduler for a run loop.
func (e *Engine) RecoverAbandoned(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("recovery limit must be positive")
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	searchTime, err := e.now(ctx, tx)
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT w.id FROM workflow_executions w WHERE w.state='cancel_requested' OR (w.state IN ('running','compensating') AND ((CASE WHEN w.state='compensating' THEN w.compensation_deadline_at ELSE w.deadline_at END) <= $1 OR EXISTS (SELECT 1 FROM activities a JOIN leases l ON l.activity_id=a.id WHERE a.workflow_id=w.id AND l.state='active' AND l.expires_at <= $1) OR EXISTS (SELECT 1 FROM activities a WHERE a.workflow_id=w.id AND a.state='retry_wait' AND a.available_at <= $1))) ORDER BY w.id FOR UPDATE OF w SKIP LOCKED LIMIT $2`, searchTime, limit)
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
	var state WorkflowState
	if err := tx.QueryRow(ctx, `SELECT state FROM workflow_executions WHERE id=$1`, workflowID).Scan(&state); err != nil {
		return 0, err
	}
	if state == WorkflowCancelRequested {
		now, err := e.now(ctx, tx)
		if err != nil {
			return 0, err
		}
		return 1, e.beginCompensation(ctx, tx, workflowID, WorkflowCancelled, "cancelled", "cancelled", now)
	}
	if !workingWorkflow(state) {
		return 0, nil
	}
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
	if err := tx.QueryRow(ctx, `SELECT CASE WHEN state='compensating' THEN compensation_deadline_at ELSE deadline_at END FROM workflow_executions WHERE id=$1`, workflowID).Scan(&deadline); err != nil {
		return 0, err
	}
	if !deadline.After(now) {
		if state == WorkflowCompensating {
			return 1, e.failCompensation(ctx, tx, workflowID, "compensation_deadline", "compensation_deadline", now)
		}
		return 1, e.timeoutWorkflow(ctx, tx, workflowID, "workflow_deadline", now)
	}
	recovered := 0
	for _, item := range items {
		if item.expiresAt.After(now) {
			continue
		}
		d := delivery{workflowID: workflowID, activityID: item.activityID, attemptID: item.attemptID, leaseID: item.leaseID}
		state, message := AttemptAbandoned, "lease_expired"
		// Scheduler downtime must not turn an earlier loss of ownership into a
		// handler timeout. Equality means the deadline capped the lease.
		if !item.deadlineAt.After(item.expiresAt) {
			state, message = AttemptTimedOut, "attempt_deadline"
		}
		if err := e.recordAttemptFailure(ctx, tx, d, state, message, false, now); err != nil {
			return 0, err
		}
		recovered++
	}
	rows, err = tx.Query(ctx, `SELECT a.id FROM activities a JOIN workflow_executions w ON w.id=a.workflow_id WHERE a.workflow_id=$1 AND ((w.state='running' AND a.kind='forward') OR (w.state='compensating' AND a.kind='compensation')) AND a.state='retry_wait' AND a.available_at <= $2 FOR UPDATE OF a`, workflowID, now)
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

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// settleDeadline is called only after workflow and ownership rows are locked.
func (e *Engine) settleDeadline(ctx context.Context, tx pgx.Tx, id, token string, now time.Time) (bool, error) {
	var passed bool
	err := tx.QueryRow(ctx, `SELECT (CASE WHEN w.state='compensating' THEN w.compensation_deadline_at ELSE w.deadline_at END) <= $2 OR (l.state='active' AND at.state='running' AND at.deadline_at <= $2) FROM workflow_executions w JOIN activities a ON a.workflow_id=w.id JOIN leases l ON l.activity_id=a.id JOIN attempts at ON at.id=l.attempt_id WHERE w.id=$1 AND l.token=$3`, id, now, token).Scan(&passed)
	if err != nil {
		return false, err
	}
	if !passed {
		return false, nil
	}
	_, err = e.recoverWorkflow(ctx, tx, id)
	return err == nil, err
}

// stopActivities runs under the workflow lock and fences ownership atomically
// with stopping pending work. It never waits for an application handler.
func stopActivities(ctx context.Context, tx pgx.Tx, id string, activityState ActivityState, attemptState AttemptState, reason string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id FROM activities WHERE workflow_id=$1 AND state IN ('blocked','ready','retry_wait','running') ORDER BY position FOR UPDATE`, id)
	if err != nil {
		return err
	}
	var activities []string
	for rows.Next() {
		var activityID string
		if err := rows.Scan(&activityID); err != nil {
			rows.Close()
			return err
		}
		activities = append(activities, activityID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT l.id,l.activity_id,l.attempt_id FROM leases l JOIN activities a ON a.id=l.activity_id JOIN attempts at ON at.id=l.attempt_id WHERE a.workflow_id=$1 AND l.state='active' AND at.state='running' FOR UPDATE OF at,l`, id)
	if err != nil {
		return err
	}
	var deliveries []delivery
	for rows.Next() {
		d := delivery{workflowID: id}
		if err := rows.Scan(&d.leaseID, &d.activityID, &d.attemptID); err != nil {
			rows.Close()
			return err
		}
		deliveries = append(deliveries, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, d := range deliveries {
		if _, err := tx.Exec(ctx, `UPDATE leases SET state='expired',ended_at=$2 WHERE id=$1`, d.leaseID, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET state=$4,ended_at=$2,error_text=$3,retry_decision='stop' WHERE id=$1`, d.attemptID, now, reason, attemptState); err != nil {
			return err
		}
		if err := appendHistory(ctx, tx, id, &d.activityID, &d.attemptID, "attempt."+string(attemptState), map[string]any{"reason": reason}, now); err != nil {
			return err
		}
	}
	for _, activityID := range activities {
		if _, err := tx.Exec(ctx, `UPDATE activities SET state=$4,updated_at=$2,completed_at=$2,last_error=$3 WHERE id=$1`, activityID, now, reason, activityState); err != nil {
			return err
		}
		if err := appendHistory(ctx, tx, id, &activityID, nil, "activity."+string(activityState), map[string]any{"reason": reason}, now); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) timeoutWorkflow(ctx context.Context, tx pgx.Tx, id, reason string, now time.Time) error {
	if err := stopActivities(ctx, tx, id, ActivityTimedOut, AttemptTimedOut, reason, now); err != nil {
		return err
	}
	return e.beginCompensation(ctx, tx, id, WorkflowTimedOut, reason, reason, now)
}
