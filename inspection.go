package durable

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// WorkflowFilter combines optional exact matches and an exclusive creation cutoff.
type WorkflowFilter struct {
	ID                string
	State             WorkflowState
	DefinitionName    string
	DefinitionVersion string
	TerminalReason    string
	CompensationState CompensationState
	CreatedBefore     *time.Time
}

// WorkflowSummary omits payloads and keys. Diagnostic text is opt-in through
// PageRequest.Sensitive. Age is measured using the engine's time authority.
type WorkflowSummary struct {
	ID                string
	DefinitionName    string
	DefinitionVersion string
	State             WorkflowState
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CompletedAt       *time.Time
	Age               time.Duration
	DeadlineAt        time.Time
	TerminalReason    string
	HasError          bool   // Failure outcome or retained diagnostic text, even if redacted.
	LastError         string `json:",omitempty"`
	CancelActor       string `json:",omitempty"`
	CancelReason      string `json:",omitempty"`
	CancelRequestedAt *time.Time
	CancelSettledAt   *time.Time
	OriginalState     WorkflowState
	CompensationState CompensationState
	CompensationError string `json:",omitempty"`
	NextActivity      *PendingActivity
}

// PendingActivity identifies the first unfinished step, including version and retry due time.
type PendingActivity struct {
	Name        string
	Version     string
	State       ActivityState
	Kind        ActivityKind
	AvailableAt time.Time
}

// ListWorkflows returns bounded live pages ordered by stable database ordinal.
// Start a new scan to observe new inserts and concurrent status changes.
func (e *Engine) ListWorkflows(ctx context.Context, filter WorkflowFilter, request PageRequest) (Page[WorkflowSummary], error) {
	switch filter.State {
	case "", WorkflowRunning, WorkflowCompleted, WorkflowFailed, WorkflowTimedOut, WorkflowCancelRequested, WorkflowCancelled, WorkflowCompensating, WorkflowCompensationFailed:
	default:
		return Page[WorkflowSummary]{}, ErrInvalidInput
	}
	switch filter.CompensationState {
	case "", CompensationNone, CompensationRunning, CompensationCompleted, CompensationFailed:
	default:
		return Page[WorkflowSummary]{}, ErrInvalidInput
	}
	if filter.ID != "" {
		var id pgtype.UUID
		if err := id.Scan(filter.ID); err != nil || !id.Valid {
			return Page[WorkflowSummary]{}, ErrInvalidInput
		}
	}
	query := `SELECT w.ordinal AS page_key,w.id,w.definition_name,w.definition_version,w.state,w.created_at,w.updated_at,w.completed_at,w.deadline_at,w.terminal_reason,(w.last_error<>'' OR w.compensation_error<>''),CASE WHEN $8 THEN w.last_error ELSE '' END,CASE WHEN $8 THEN w.cancel_actor ELSE '' END,CASE WHEN $8 THEN w.cancel_reason ELSE '' END,w.cancel_requested_at,w.cancel_settled_at,w.original_state,w.compensation_state,CASE WHEN $8 THEN w.compensation_error ELSE '' END,p.name,p.version,p.state,p.kind,p.available_at
FROM workflow_executions w LEFT JOIN LATERAL (SELECT name,version,state,kind,available_at FROM activities WHERE workflow_id=w.id AND state IN ('blocked','ready','running','retry_wait') ORDER BY position LIMIT 1) p ON true
WHERE ($1='' OR w.id::text=$1) AND ($2='' OR w.state=$2) AND ($3='' OR w.definition_name=$3) AND ($4='' OR w.definition_version=$4) AND ($5='' OR w.terminal_reason=$5) AND ($6='' OR w.compensation_state=$6) AND ($7::timestamptz IS NULL OR w.created_at<$7)`
	args := []any{filter.ID, filter.State, filter.DefinitionName, filter.DefinitionVersion, filter.TerminalReason, filter.CompensationState, filter.CreatedBefore, request.Sensitive}
	return readPage(ctx, e, request, struct {
		Kind   string
		Filter WorkflowFilter
	}{"workflows", filter}, query, "w.ordinal", args, func(rows pgx.Rows, now time.Time) (WorkflowSummary, int64, error) {
		var item WorkflowSummary
		var key int64
		var name, version, state, kind *string
		var due *time.Time
		err := rows.Scan(&key, &item.ID, &item.DefinitionName, &item.DefinitionVersion, &item.State, &item.CreatedAt, &item.UpdatedAt, &item.CompletedAt, &item.DeadlineAt, &item.TerminalReason, &item.HasError, &item.LastError, &item.CancelActor, &item.CancelReason, &item.CancelRequestedAt, &item.CancelSettledAt, &item.OriginalState, &item.CompensationState, &item.CompensationError, &name, &version, &state, &kind, &due)
		item.HasError = item.HasError || item.State == WorkflowFailed || item.State == WorkflowTimedOut || item.State == WorkflowCompensationFailed
		item.Age = max(time.Duration(0), now.Sub(item.CreatedAt))
		if name != nil {
			item.NextActivity = &PendingActivity{*name, *version, ActivityState(*state), ActivityKind(*kind), *due}
		}
		return item, key, err
	})
}

// ListActivities omits inputs, outputs, cleanup payloads, and diagnostic text by
// default. Contract identities and due times explain compatible waiting work.
func (e *Engine) ListActivities(ctx context.Context, workflowID string, request PageRequest) (Page[Activity], error) {
	query := `SELECT position+1 AS page_key,id,workflow_id,position,name,version,input_schema,output_schema,step_key,kind,compensates_id,state,attempt_count,created_at,updated_at,completed_at,retry_policy,available_at,attempt_timeout_ns,CASE WHEN $2 THEN last_error ELSE '' END,contract FROM activities WHERE workflow_id=$1`
	return readPage(ctx, e, request, []any{"activities", workflowID}, query, "position+1", []any{workflowID, request.Sensitive}, func(rows pgx.Rows, _ time.Time) (Activity, int64, error) {
		var item Activity
		var key int64
		err := rows.Scan(&key, &item.ID, &item.WorkflowID, &item.Position, &item.Name, &item.Version, &item.InputSchema, &item.OutputSchema, &item.StepKey, &item.Kind, &item.CompensatesActivityID, &item.State, &item.AttemptCount, &item.CreatedAt, &item.UpdatedAt, &item.CompletedAt, &item.RetryPolicy, &item.AvailableAt, &item.AttemptTimeout, &item.LastError, &item.Contract)
		return item, key, err
	})
}

func (e *Engine) ListAttempts(ctx context.Context, activityID string, request PageRequest) (Page[Attempt], error) {
	query := `SELECT number AS page_key,id,activity_id,number,worker_id,state,CASE WHEN $2 THEN COALESCE(error_text,'') ELSE '' END,started_at,ended_at,retry_decision,retry_due_at,deadline_at FROM attempts WHERE activity_id=$1`
	return readPage(ctx, e, request, []any{"attempts", activityID}, query, "number", []any{activityID, request.Sensitive}, func(rows pgx.Rows, _ time.Time) (Attempt, int64, error) {
		var item Attempt
		var key int64
		err := rows.Scan(&key, &item.ID, &item.ActivityID, &item.Number, &item.WorkerID, &item.State, &item.Error, &item.StartedAt, &item.EndedAt, &item.RetryDecision, &item.RetryDueAt, &item.DeadlineAt)
		return item, key, err
	})
}

// ListLeases never returns ownership tokens, even for sensitive inspection.
func (e *Engine) ListLeases(ctx context.Context, activityID string, request PageRequest) (Page[Lease], error) {
	query := `SELECT ordinal AS page_key,id,activity_id,attempt_id,worker_id,state,acquired_at,heartbeat_at,expires_at,ended_at FROM leases WHERE activity_id=$1`
	return readPage(ctx, e, request, []any{"leases", activityID}, query, "ordinal", []any{activityID}, func(rows pgx.Rows, _ time.Time) (Lease, int64, error) {
		var item Lease
		var key int64
		err := rows.Scan(&key, &item.ID, &item.ActivityID, &item.AttemptID, &item.WorkerID, &item.State, &item.AcquiredAt, &item.HeartbeatAt, &item.ExpiresAt, &item.EndedAt)
		return item, key, err
	})
}

// ListTimeline excludes event details unless sensitive inspection is requested.
func (e *Engine) ListTimeline(ctx context.Context, workflowID string, request PageRequest) (Page[TimelineEvent], error) {
	query := `SELECT sequence AS page_key,sequence,workflow_id,activity_id,attempt_id,event_type,CASE WHEN $2 THEN details ELSE '{}'::jsonb END,recorded_at FROM execution_history WHERE workflow_id=$1`
	return readPage(ctx, e, request, []any{"timeline", workflowID}, query, "sequence", []any{workflowID, request.Sensitive}, func(rows pgx.Rows, _ time.Time) (TimelineEvent, int64, error) {
		var item TimelineEvent
		var key int64
		err := rows.Scan(&key, &item.Sequence, &item.WorkflowID, &item.ActivityID, &item.AttemptID, &item.Type, &item.Details, &item.RecordedAt)
		return item, key, err
	})
}
