package durable

import (
	"context"
	"fmt"
)

func (e *Engine) RecoverAbandoned(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("recovery limit must be positive")
	}
	now := e.clock.Now().UTC()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin abandoned activity recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT l.id, l.activity_id, l.attempt_id, a.workflow_id, l.state, a.state, at.state
		FROM leases l
		JOIN activities a ON a.id = l.activity_id
		JOIN attempts at ON at.id = l.attempt_id
		JOIN workflow_executions w ON w.id = a.workflow_id
		WHERE l.state = 'active'
		  AND l.expires_at <= $1
		  AND a.state = 'running'
		  AND at.state = 'running'
		  AND w.state = 'running'
		ORDER BY l.expires_at, l.id
		FOR UPDATE OF l, a, at SKIP LOCKED
		LIMIT $2`, now, limit)
	if err != nil {
		return 0, fmt.Errorf("select abandoned activities: %w", err)
	}
	type abandoned struct {
		leaseID, activityID, attemptID, workflowID string
		leaseState                                 LeaseState
		activityState                              ActivityState
		attemptState                               AttemptState
	}
	var abandonedActivities []abandoned
	for rows.Next() {
		var item abandoned
		if err := rows.Scan(&item.leaseID, &item.activityID, &item.attemptID, &item.workflowID, &item.leaseState, &item.activityState, &item.attemptState); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan abandoned activity: %w", err)
		}
		abandonedActivities = append(abandonedActivities, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate abandoned activities: %w", err)
	}
	rows.Close()

	for _, item := range abandonedActivities {
		if err := ValidateLeaseTransition(item.leaseState, LeaseExpired); err != nil {
			return 0, err
		}
		if err := ValidateAttemptTransition(item.attemptState, AttemptAbandoned); err != nil {
			return 0, err
		}
		if err := ValidateActivityTransition(item.activityState, ActivityReady); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE leases SET state = $2, ended_at = $3 WHERE id = $1`, item.leaseID, LeaseExpired, now); err != nil {
			return 0, fmt.Errorf("expire lease: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET state = $2, ended_at = $3 WHERE id = $1`, item.attemptID, AttemptAbandoned, now); err != nil {
			return 0, fmt.Errorf("abandon attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE activities SET state = $2, updated_at = $3 WHERE id = $1`, item.activityID, ActivityReady, now); err != nil {
			return 0, fmt.Errorf("recover activity: %w", err)
		}
		if err := appendHistory(ctx, tx, item.workflowID, &item.activityID, &item.attemptID, "attempt.abandoned", map[string]any{"reason": "lease_expired"}, now); err != nil {
			return 0, err
		}
		if err := appendHistory(ctx, tx, item.workflowID, &item.activityID, nil, "activity.recovered", map[string]any{}, now); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit abandoned activity recovery: %w", err)
	}
	return len(abandonedActivities), nil
}
