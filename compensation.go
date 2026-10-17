package durable

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// beginCompensation is called under the workflow lock before a forward outcome
// becomes terminal. It materializes only declarations of committed steps.
func (e *Engine) beginCompensation(ctx context.Context, tx pgx.Tx, id string, outcome WorkflowState, reason, message string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id,compensation FROM activities WHERE workflow_id=$1 AND kind='forward' AND state='completed' AND compensation IS NOT NULL ORDER BY position DESC`, id)
	if err != nil {
		return err
	}
	type cleanup struct {
		parent string
		plan   CompensationPlan
	}
	var plans []cleanup
	for rows.Next() {
		var item cleanup
		if err := rows.Scan(&item.parent, &item.plan); err != nil {
			rows.Close()
			return err
		}
		plans = append(plans, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var state WorkflowState
	if err := tx.QueryRow(ctx, `SELECT state FROM workflow_executions WHERE id=$1`, id).Scan(&state); err != nil {
		return err
	}
	if len(plans) == 0 {
		if err := ValidateWorkflowTransition(state, outcome); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state=$2,original_state=$2,terminal_reason=$3,last_error=$4,updated_at=$5,completed_at=$5,cancel_settled_at=CASE WHEN $2='cancelled' THEN $5 ELSE cancel_settled_at END WHERE id=$1`, id, outcome, reason, message, now); err != nil {
			return err
		}
		return appendHistory(ctx, tx, id, nil, nil, "workflow."+string(outcome), map[string]any{"reason": reason}, now)
	}
	if err := ValidateWorkflowTransition(state, WorkflowCompensating); err != nil {
		return err
	}
	if err := stopActivities(ctx, tx, id, ActivityCancelled, AttemptCancelled, "forward_stopped", now); err != nil {
		return err
	}
	var timeout time.Duration
	var position int
	if err := tx.QueryRow(ctx, `SELECT compensation_timeout_ns FROM workflow_executions WHERE id=$1`, id).Scan(&timeout); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(position),-1)+1 FROM activities WHERE workflow_id=$1`, id).Scan(&position); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state='compensating',original_state=$2,terminal_reason=$3,last_error=$4,updated_at=$5,completed_at=NULL,compensation_state='running',compensation_deadline_at=$6 WHERE id=$1`, id, outcome, reason, message, now, now.Add(timeout)); err != nil {
		return err
	}
	if err := appendHistory(ctx, tx, id, nil, nil, "workflow.compensating", map[string]any{"original_state": outcome, "reason": reason, "deadline_at": now.Add(timeout)}, now); err != nil {
		return err
	}
	for i, item := range plans {
		activityID, err := newID()
		if err != nil {
			return err
		}
		state := ActivityBlocked
		if i == 0 {
			state = ActivityReady
		}
		plan := item.plan
		if _, err := tx.Exec(ctx, `INSERT INTO activities(id,workflow_id,position,name,input,state,created_at,updated_at,step_key,contract,retry_policy,attempt_timeout_ns,available_at,kind,compensates_id) VALUES($1,$2,$3,$4,$5,$6,$7,$7,$8,$9,$10,$11,$7,'compensation',$12)`, activityID, id, position+i, plan.Name, plan.Input, state, now, "compensate:"+item.parent, plan.Contract, plan.RetryPolicy, plan.AttemptTimeout, item.parent); err != nil {
			return payloadDatabaseError(err)
		}
		if err := appendHistory(ctx, tx, id, &activityID, nil, "compensation.scheduled", map[string]any{"compensates_id": item.parent, "state": state, "name": plan.Name}, now); err != nil {
			return err
		}
	}
	return nil
}

func completeCompensation(ctx context.Context, tx pgx.Tx, id string, now time.Time) error {
	var outcome WorkflowState
	if err := tx.QueryRow(ctx, `SELECT original_state FROM workflow_executions WHERE id=$1`, id).Scan(&outcome); err != nil {
		return err
	}
	if err := ValidateWorkflowTransition(WorkflowCompensating, outcome); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state=$2,compensation_state='completed',compensation_error='',updated_at=$3,completed_at=$3,cancel_settled_at=CASE WHEN $2='cancelled' THEN $3 ELSE cancel_settled_at END WHERE id=$1`, id, outcome, now); err != nil {
		return err
	}
	if err := appendHistory(ctx, tx, id, nil, nil, "workflow.compensation_completed", map[string]any{}, now); err != nil {
		return err
	}
	return appendHistory(ctx, tx, id, nil, nil, "workflow."+string(outcome), map[string]any{"compensation": "completed"}, now)
}

func (e *Engine) failCompensation(ctx context.Context, tx pgx.Tx, id, reason, message string, now time.Time) error {
	activityState, attemptState := ActivityCancelled, AttemptCancelled
	if reason == "compensation_deadline" {
		activityState, attemptState = ActivityTimedOut, AttemptTimedOut
	}
	if err := stopActivities(ctx, tx, id, activityState, attemptState, reason, now); err != nil {
		return err
	}
	if err := ValidateWorkflowTransition(WorkflowCompensating, WorkflowCompensationFailed); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state='compensation_failed',compensation_state='failed',compensation_error=$2,updated_at=$3,completed_at=$3,cancel_settled_at=CASE WHEN original_state='cancelled' THEN $3 ELSE cancel_settled_at END WHERE id=$1`, id, message, now); err != nil {
		return err
	}
	return appendHistory(ctx, tx, id, nil, nil, "workflow.compensation_failed", map[string]any{"reason": reason, "error": message}, now)
}
