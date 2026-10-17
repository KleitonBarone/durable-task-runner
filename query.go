package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (e *Engine) Workflow(ctx context.Context, workflowID string) (WorkflowExecution, error) {
	if err := e.CheckSchema(ctx); err != nil {
		return WorkflowExecution{}, err
	}
	var execution WorkflowExecution
	err := e.pool.QueryRow(ctx, `
		SELECT id, definition_name, input, state, created_at, updated_at, completed_at, output, start_key, terminal_reason, last_error, deadline_at, timeout_ns, previous_execution_id, cancel_actor, cancel_reason, cancel_requested_at, cancel_settled_at,original_state,compensation_state,compensation_error,compensation_timeout_ns,compensation_deadline_at,definition_version,input_schema
		FROM workflow_executions WHERE id = $1`, workflowID).Scan(
		&execution.ID, &execution.DefinitionName, &execution.Input, &execution.State,
		&execution.CreatedAt, &execution.UpdatedAt, &execution.CompletedAt, &execution.Output, &execution.StartKey,
		&execution.TerminalReason, &execution.LastError,
		&execution.DeadlineAt, &execution.Timeout,
		&execution.PreviousExecutionID,
		&execution.CancelActor, &execution.CancelReason, &execution.CancelRequestedAt, &execution.CancelSettledAt,
		&execution.OriginalState, &execution.CompensationState, &execution.CompensationError, &execution.CompensationTimeout, &execution.CompensationDeadlineAt,
		&execution.DefinitionVersion, &execution.InputSchema,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowExecution{}, fmt.Errorf("%w: workflow %q", ErrNotFound, workflowID)
	}
	if err != nil {
		return WorkflowExecution{}, fmt.Errorf("read workflow: %w", err)
	}
	return execution, nil
}

func (e *Engine) Activities(ctx context.Context, workflowID string) ([]Activity, error) {
	if err := e.CheckSchema(ctx); err != nil {
		return nil, err
	}
	rows, err := e.pool.Query(ctx, `
		SELECT id, workflow_id, position, name, input, COALESCE(output, 'null'::jsonb), state, step_key,
		       attempt_count, created_at, updated_at, completed_at, retry_policy, available_at, attempt_timeout_ns, last_error,kind,compensates_id,compensation,version,input_schema,output_schema
		FROM activities WHERE workflow_id = $1 ORDER BY position`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("read activities: %w", err)
	}
	defer rows.Close()
	var activities []Activity
	for rows.Next() {
		var activity Activity
		if err := rows.Scan(
			&activity.ID, &activity.WorkflowID, &activity.Position, &activity.Name,
			&activity.Input, &activity.Output, &activity.State, &activity.StepKey, &activity.AttemptCount,
			&activity.CreatedAt, &activity.UpdatedAt, &activity.CompletedAt,
			&activity.RetryPolicy, &activity.AvailableAt, &activity.AttemptTimeout, &activity.LastError,
			&activity.Kind, &activity.CompensatesActivityID, &activity.Compensation,
			&activity.Version, &activity.InputSchema, &activity.OutputSchema,
		); err != nil {
			return nil, fmt.Errorf("scan activity: %w", err)
		}
		activities = append(activities, activity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate activities: %w", err)
	}
	return activities, nil
}

func (e *Engine) Timeline(ctx context.Context, workflowID string) ([]TimelineEvent, error) {
	if err := e.CheckSchema(ctx); err != nil {
		return nil, err
	}
	rows, err := e.pool.Query(ctx, `
		SELECT sequence, workflow_id, activity_id, attempt_id, event_type, details, recorded_at
		FROM execution_history WHERE workflow_id = $1 ORDER BY sequence`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("read execution timeline: %w", err)
	}
	defer rows.Close()
	var timeline []TimelineEvent
	for rows.Next() {
		var event TimelineEvent
		if err := rows.Scan(
			&event.Sequence, &event.WorkflowID, &event.ActivityID, &event.AttemptID,
			&event.Type, &event.Details, &event.RecordedAt,
		); err != nil {
			return nil, fmt.Errorf("scan execution timeline: %w", err)
		}
		timeline = append(timeline, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution timeline: %w", err)
	}
	return timeline, nil
}
