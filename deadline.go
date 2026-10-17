package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// settleDeadline is called only after workflow and ownership rows are locked.
func (e *Engine) settleDeadline(ctx context.Context, tx pgx.Tx, id, token string, now time.Time) (bool, error) {
	var passed bool
	err := tx.QueryRow(ctx, `SELECT w.deadline_at <= $2 OR (l.state='active' AND at.state='running' AND at.deadline_at <= $2) FROM workflow_executions w JOIN activities a ON a.workflow_id=w.id JOIN leases l ON l.activity_id=a.id JOIN attempts at ON at.id=l.attempt_id WHERE w.id=$1 AND l.token=$3`, id, now, token).Scan(&passed)
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
	if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state='timed_out',terminal_reason=$2,last_error=$2,updated_at=$3,completed_at=$3 WHERE id=$1 AND state='running'`, id, reason, now); err != nil {
		return fmt.Errorf("time out workflow: %w", err)
	}
	return appendHistory(ctx, tx, id, nil, nil, "workflow.timed_out", map[string]any{"reason": reason}, now)
}
