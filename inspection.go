package durable

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
		       attempt_count, created_at, updated_at, completed_at, retry_policy, available_at, attempt_timeout_ns, last_error,kind,compensates_id,compensation,version,input_schema,output_schema,contract
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
			&activity.Contract,
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

type activityContextKey struct{}
type activityContext struct {
	engine *Engine
	claim  Claim
}

// Result reads a completed earlier step in the current handler's workflow.
// The handler context carries execution identity; callers cannot read a future
// step or a result with a different activity type.
func (a ActivityDefinition[Input, Output]) Result(ctx context.Context, key string) (Output, error) {
	var output Output
	current, ok := ctx.Value(activityContextKey{}).(activityContext)
	if !ok {
		return output, fmt.Errorf("%w: Result requires a handler context", ErrResultUnavailable)
	}
	if err := current.engine.CheckSchema(ctx); err != nil {
		return output, err
	}
	var stored capability
	var position int
	var state ActivityState
	var raw json.RawMessage
	err := current.engine.pool.QueryRow(ctx, `SELECT name,version,contract,input_schema,output_schema, position, state, output FROM activities WHERE workflow_id = $1 AND step_key = $2`, current.claim.WorkflowID, key).Scan(&stored.name, &stored.version, &stored.contract, &stored.inputSchema, &stored.outputSchema, &position, &state, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return output, fmt.Errorf("%w: step %q", ErrNotFound, key)
	}
	if err != nil {
		return output, fmt.Errorf("read step result: %w", err)
	}
	if position >= current.claim.Position || state != ActivityCompleted {
		return output, ErrResultUnavailable
	}
	return a.decode(stored, raw)
}

// WorkflowResult reads the last step's output after successful completion.
func (a ActivityDefinition[Input, Output]) WorkflowResult(ctx context.Context, engine *Engine, workflowID string) (Output, error) {
	var output Output
	if err := engine.CheckSchema(ctx); err != nil {
		return output, err
	}
	var state WorkflowState
	var stored capability
	var raw json.RawMessage
	err := engine.pool.QueryRow(ctx, `SELECT w.state, a.name,a.version, a.contract,a.input_schema,a.output_schema, w.output FROM workflow_executions w JOIN activities a ON a.workflow_id = w.id WHERE w.id = $1 AND a.kind='forward' ORDER BY a.position DESC LIMIT 1`, workflowID).Scan(&state, &stored.name, &stored.version, &stored.contract, &stored.inputSchema, &stored.outputSchema, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return output, ErrNotFound
	}
	if err != nil {
		return output, fmt.Errorf("read workflow result: %w", err)
	}
	if state != WorkflowCompleted {
		return output, ErrResultUnavailable
	}
	return a.decode(stored, raw)
}

func (a ActivityDefinition[Input, Output]) decode(stored capability, raw json.RawMessage) (Output, error) {
	var output Output
	if stored != (capability{a.name, a.version, a.contract(), a.inputSchema, a.outputSchema}) {
		return output, ErrTypeMismatch
	}
	if err := json.Unmarshal(raw, &output); err != nil {
		return output, fmt.Errorf("decode stored result: %w", err)
	}
	return output, nil
}

// Attempts lists attempts for one activity in attempt-number order.
func (e *Engine) Attempts(ctx context.Context, activityID string) ([]Attempt, error) {
	if err := e.CheckSchema(ctx); err != nil {
		return nil, err
	}
	var exists bool
	if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM activities WHERE id = $1)`, activityID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("read activity: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := e.pool.Query(ctx, `SELECT id, activity_id, number, worker_id, state, COALESCE(error_text, ''), started_at, ended_at, retry_decision, retry_due_at, deadline_at FROM attempts WHERE activity_id = $1 ORDER BY number`, activityID)
	if err != nil {
		return nil, fmt.Errorf("read attempts: %w", err)
	}
	defer rows.Close()
	var attempts []Attempt
	for rows.Next() {
		var attempt Attempt
		if err := rows.Scan(&attempt.ID, &attempt.ActivityID, &attempt.Number, &attempt.WorkerID, &attempt.State, &attempt.Error, &attempt.StartedAt, &attempt.EndedAt, &attempt.RetryDecision, &attempt.RetryDueAt, &attempt.DeadlineAt); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}

// PageRequest bounds a live scan. Sensitive exposes diagnostic text and history
// details, never lease tokens. Application result APIs remain the payload path.
type PageRequest struct {
	Limit     int
	Cursor    string
	Sensitive bool
}

// Page is one bounded live read. Continue with NextCursor and identical filters;
// concurrent commits can change or remove rows between requests.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type pageCursor struct {
	Scope string `json:"scope"`
	After int64  `json:"after"`
	Upper int64  `json:"upper"`
}

func parsePage(request PageRequest, scope any) (PageRequest, pageCursor, error) {
	if request.Limit == 0 {
		request.Limit = 50
	}
	if request.Limit < 1 || request.Limit > 500 || len(request.Cursor) > 4096 {
		return request, pageCursor{}, fmt.Errorf("%w: page limit must be 1-500", ErrInvalidInput)
	}
	encoded, err := json.Marshal(struct {
		Scope     any
		Sensitive bool
	}{scope, request.Sensitive})
	if err != nil {
		return request, pageCursor{}, err
	}
	want := fmt.Sprintf("%x", sha256.Sum256(encoded))
	cursor := pageCursor{Scope: want}
	if request.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(request.Cursor)
		if err != nil {
			return request, cursor, fmt.Errorf("%w: invalid page cursor", ErrInvalidInput)
		}
		if err := json.Unmarshal(raw, &cursor); err != nil || cursor.Scope != want || cursor.After < 0 || cursor.Upper < cursor.After {
			return request, cursor, fmt.Errorf("%w: cursor does not match query", ErrInvalidInput)
		}
	}
	return request, cursor, nil
}

// readPage uses only internal SQL/key identifiers. The cursor is query-bound,
// not an authorization token; database access remains application-owned.
func readPage[T any](ctx context.Context, e *Engine, request PageRequest, scope any, query, key string, args []any, scan func(pgx.Rows, time.Time) (T, int64, error)) (Page[T], error) {
	request, cursor, err := parsePage(request, scope)
	if err != nil {
		return Page[T]{}, err
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return Page[T]{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now, err := e.now(ctx, tx)
	if err != nil {
		return Page[T]{}, err
	}
	if request.Cursor == "" {
		if err := tx.QueryRow(ctx, "SELECT COALESCE(max(page_key),0) FROM ("+query+") page", args...).Scan(&cursor.Upper); err != nil {
			return Page[T]{}, err
		}
	}
	n := len(args)
	query += fmt.Sprintf(" AND %s > $%d AND %s <= $%d ORDER BY %s LIMIT $%d", key, n+1, key, n+2, key, n+3)
	args = append(args, cursor.After, cursor.Upper, request.Limit+1)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return Page[T]{}, err
	}
	defer rows.Close()
	page := Page[T]{Items: make([]T, 0, request.Limit)}
	var last int64
	for rows.Next() {
		item, key, err := scan(rows, now)
		if err != nil {
			return Page[T]{}, err
		}
		if len(page.Items) == request.Limit {
			cursor.After = last
			raw, err := json.Marshal(cursor)
			if err != nil {
				return Page[T]{}, err
			}
			page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		last = key
		page.Items = append(page.Items, item)
	}
	rows.Close()
	return page, rows.Err()
}
