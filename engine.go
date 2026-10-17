package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine coordinates persisted executions. It is safe for concurrent operations.
type Engine struct {
	pool      *pgxpool.Pool
	registry  *Registry
	clock     Clock
	ownsPool  bool
	observers []Observer
}

// New uses an application-owned pool without doing I/O. Use SystemClock in
// production. The first operation checks schema compatibility.
func New(pool *pgxpool.Pool, registry *Registry, clock Clock, options ...EngineOption) (*Engine, error) {
	if pool == nil {
		return nil, fmt.Errorf("PostgreSQL pool is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("workflow registry is required")
	}
	if clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	engine := &Engine{pool: pool, registry: registry, clock: clock}
	for _, option := range options {
		if option.observer == nil {
			return nil, fmt.Errorf("observer is required")
		}
		engine.observers = append(engine.observers, option.observer)
	}
	return engine, nil
}

// Open creates and owns a pool, verifies connectivity, and accepts a known schema
// prefix so the caller can apply pending migrations. It never migrates implicitly.
func Open(ctx context.Context, databaseURL string, registry *Registry, clock Clock, options ...EngineOption) (*Engine, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("configure PostgreSQL: %w", err)
	}
	if config.ConnConfig.ConnectTimeout == 0 {
		config.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	engine, err := New(pool, registry, clock, options...)
	if err != nil {
		pool.Close()
		return nil, err
	}
	engine.ownsPool = true
	tx, err := checkSchemaTransaction(ctx, pool, false)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if err := tx.Rollback(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return engine, nil
}

// Close closes only a pool created by Open. Stop workers and the scheduler first.
func (e *Engine) Close() {
	if e != nil && e.ownsPool {
		e.pool.Close()
	}
}

// Migrate atomically applies pending embedded migrations under an exclusive lock.
func (e *Engine) Migrate(ctx context.Context) error {
	return Migrate(ctx, e.pool)
}

func (e *Engine) start(ctx context.Context, definitionName, definitionVersion string, input any, options ...StartOption) (WorkflowExecution, error) {
	config, err := parseStartOptions(options)
	if err != nil {
		return WorkflowExecution{}, err
	}
	key := config.key
	definition, ok := e.registry.lookupVersion(definitionName, definitionVersion)
	if !ok {
		return WorkflowExecution{}, fmt.Errorf("%w: %q", ErrUnknownWorkflow, definitionName)
	}
	encodedInput, activitySpecs, err := definition.encodePlan(input)
	if err != nil {
		return WorkflowExecution{}, err
	}
	contracts := make(map[string]string)
	for _, spec := range activitySpecs {
		c := capability{spec.Name, spec.Version, spec.contract, spec.InputSchema, spec.OutputSchema}
		key := activityKey(c.name, c.version)
		if previous, ok := contracts[key]; ok && previous != c.signature() {
			return WorkflowExecution{}, fmt.Errorf("activity %q has incompatible types in plan", spec.Name)
		}
		contracts[key] = c.signature()
		if cleanup := spec.compensation; cleanup != nil {
			c := capability{cleanup.Name, cleanup.Version, cleanup.Contract, cleanup.InputSchema, cleanup.OutputSchema}
			key := activityKey(c.name, c.version)
			if previous, ok := contracts[key]; ok && previous != c.signature() {
				return WorkflowExecution{}, fmt.Errorf("cleanup %q has incompatible types in plan", cleanup.Name)
			}
			contracts[key] = c.signature()
		}
	}
	if err := e.registry.bindActivities(contracts); err != nil {
		return WorkflowExecution{}, err
	}

	workflowID, err := newID()
	if err != nil {
		return WorkflowExecution{}, err
	}
	activityIDs := make([]string, len(activitySpecs))
	for i := range activitySpecs {
		activityIDs[i], err = newID()
		if err != nil {
			return WorkflowExecution{}, err
		}
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return WorkflowExecution{}, fmt.Errorf("begin workflow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now, err := e.now(ctx, tx)
	if err != nil {
		return WorkflowExecution{}, err
	}
	execution := WorkflowExecution{ID: workflowID, DefinitionName: definitionName, StartKey: key, Input: encodedInput, State: WorkflowRunning, CreatedAt: now, UpdatedAt: now}
	execution.Timeout = config.timeout
	execution.DeadlineAt = now.Add(config.timeout)
	execution.PreviousExecutionID = config.previous
	execution.CompensationTimeout = config.compensationTimeout
	execution.CompensationState = CompensationNone
	execution.DefinitionVersion = definition.version
	execution.InputSchema = definition.inputSchema

	if key != nil {
		inserted, err := tx.Exec(ctx, `INSERT INTO start_keys(definition_name,definition_version,start_key,execution_id,input_schema,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(definition_name,definition_version,start_key) DO NOTHING`, definitionName, definition.version, key, workflowID, definition.inputSchema, now)
		if err != nil {
			return WorkflowExecution{}, fmt.Errorf("persist start key: %w", err)
		}
		if inserted.RowsAffected() == 0 {
			var existingID string
			var matches bool
			if err := tx.QueryRow(ctx, `SELECT k.execution_id,COALESCE(COALESCE(k.input,w.input)=$4::jsonb AND k.input_schema=$5,false) FROM start_keys k LEFT JOIN workflow_executions w ON w.id=k.execution_id WHERE k.definition_name=$1 AND k.definition_version=$2 AND k.start_key=$3`, definitionName, definition.version, key, encodedInput, definition.inputSchema).Scan(&existingID, &matches); err != nil {
				return WorkflowExecution{}, fmt.Errorf("resolve start key: %w", payloadDatabaseError(err))
			}
			if !matches {
				return WorkflowExecution{}, ErrStartConflict
			}
			if err := tx.Rollback(ctx); err != nil {
				return WorkflowExecution{}, err
			}
			existing, err := e.Workflow(ctx, existingID)
			if errors.Is(err, ErrNotFound) {
				return WorkflowExecution{}, &ExecutionExpiredError{ID: existingID}
			}
			return existing, err
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_executions (id, definition_name, input, state, created_at, updated_at, start_key, timeout_ns, deadline_at, previous_execution_id,compensation_timeout_ns,definition_version,input_schema)
		VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9,$10,$11,$12)`,
		workflowID, definitionName, encodedInput, WorkflowRunning, now, key, config.timeout, execution.DeadlineAt, config.previous, config.compensationTimeout, definition.version, definition.inputSchema)
	if err != nil {
		return WorkflowExecution{}, fmt.Errorf("persist workflow: %w", payloadDatabaseError(err))
	}
	if config.previous != nil {
		var state WorkflowState
		if err := tx.QueryRow(ctx, `SELECT state FROM workflow_executions WHERE id=$1`, config.previous).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
			return WorkflowExecution{}, ErrNotFound
		} else if err != nil {
			return WorkflowExecution{}, err
		}
		if !terminalWorkflow(state) {
			return WorkflowExecution{}, fmt.Errorf("%w: predecessor must be terminal", ErrInvalidInput)
		}
	}
	if err := appendHistory(ctx, tx, workflowID, nil, nil, "workflow.started", map[string]any{
		"definition": definitionName, "version": definition.version, "input_schema": definition.inputSchema, "previous_execution_id": config.previous, "deadline_at": execution.DeadlineAt,
	}, now); err != nil {
		return WorkflowExecution{}, err
	}

	for i, spec := range activitySpecs {
		state := ActivityBlocked
		if i == 0 {
			state = ActivityReady
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO activities (id, workflow_id, position, name, input, state, created_at, updated_at, step_key, contract, retry_policy, attempt_timeout_ns, available_at,compensation,version,input_schema,output_schema)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11, $7,$12,$13,$14,$15)`,
			activityIDs[i], workflowID, i, spec.Name, spec.Input, state, now, spec.Key, spec.contract, spec.policy, spec.attemptTimeout, spec.compensation, spec.Version, spec.InputSchema, spec.OutputSchema)
		if err != nil {
			return WorkflowExecution{}, fmt.Errorf("persist activity %d: %w", i, payloadDatabaseError(err))
		}
		if err := appendHistory(ctx, tx, workflowID, &activityIDs[i], nil, "activity.scheduled", map[string]any{
			"name": spec.Name, "version": spec.Version, "position": i, "state": state, "step_key": spec.Key,
		}, now); err != nil {
			return WorkflowExecution{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowExecution{}, fmt.Errorf("commit workflow: %w", err)
	}
	return execution, nil
}

// ClaimActivity is a low-level claim without handler capability filtering.
// Prefer Worker, which claims only compatible registered handlers.
func (e *Engine) ClaimActivity(ctx context.Context, workerID string, leaseDuration time.Duration) (Claim, error) {
	return e.claimActivity(ctx, workerID, leaseDuration, nil)
}

func (e *Engine) claimActivity(ctx context.Context, workerID string, leaseDuration time.Duration, capabilities map[string]capability) (Claim, error) {
	if workerID == "" {
		return Claim{}, fmt.Errorf("worker id is required")
	}
	if leaseDuration <= 0 {
		return Claim{}, fmt.Errorf("lease duration must be positive")
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return Claim{}, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var claim Claim
	var currentState ActivityState
	var policy RetryPolicy
	var attemptTimeout time.Duration
	var readyAt time.Time
	names, versions, contracts, inputSchemas, outputSchemas := capabilityArrays(capabilities)
	err = tx.QueryRow(ctx, `
		SELECT a.id, a.workflow_id, a.name, a.input, a.attempt_count, a.state, a.step_key, a.position,a.kind,a.compensates_id,a.version,a.input_schema,a.output_schema
		FROM activities a
		JOIN workflow_executions w ON w.id = a.workflow_id
		WHERE a.state = 'ready' AND ((w.state = 'running' AND a.kind='forward') OR (w.state='compensating' AND a.kind='compensation'))
		  AND ($1::boolean OR (a.name,a.version, a.contract,a.input_schema,a.output_schema) IN (SELECT * FROM unnest($2::text[], $3::text[],$4::text[],$5::text[],$6::text[])))
		ORDER BY a.created_at, a.workflow_id, a.position
		FOR UPDATE OF w SKIP LOCKED
		LIMIT 1`, capabilities == nil, names, versions, contracts, inputSchemas, outputSchemas).Scan(
		&claim.ActivityID, &claim.WorkflowID, &claim.ActivityName, &claim.ActivityInput,
		&claim.AttemptNumber, &currentState, &claim.StepKey, &claim.Position, &claim.Kind, &claim.CompensatesActivityID,
		&claim.ActivityVersion, &claim.InputSchema, &claim.OutputSchema,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, ErrNoActivity
	}
	if err != nil {
		return Claim{}, fmt.Errorf("select activity to claim: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT state, attempt_count, retry_policy, attempt_timeout_ns,available_at FROM activities WHERE id=$1 FOR UPDATE`, claim.ActivityID).Scan(&currentState, &claim.AttemptNumber, &policy, &attemptTimeout, &readyAt); err != nil {
		return Claim{}, err
	}
	if currentState != ActivityReady {
		return Claim{}, ErrNoActivity
	}
	now, err := e.now(ctx, tx)
	if err != nil {
		return Claim{}, err
	}
	var workflowDeadline time.Time
	if err := tx.QueryRow(ctx, `SELECT CASE WHEN state='compensating' THEN compensation_deadline_at ELSE deadline_at END FROM workflow_executions WHERE id=$1`, claim.WorkflowID).Scan(&workflowDeadline); err != nil {
		return Claim{}, err
	}
	if !workflowDeadline.After(now) {
		var stopErr error
		if claim.Kind == ActivityCompensation {
			stopErr = e.failCompensation(ctx, tx, claim.WorkflowID, "compensation_deadline", "compensation_deadline", now)
		} else {
			stopErr = e.timeoutWorkflow(ctx, tx, claim.WorkflowID, "workflow_deadline", now)
		}
		if stopErr != nil {
			return Claim{}, stopErr
		}
		if err := tx.Commit(ctx); err != nil {
			return Claim{}, err
		}
		return Claim{}, ErrNoActivity
	}
	if claim.AttemptNumber >= policy.MaxAttempts {
		if _, err := tx.Exec(ctx, `UPDATE activities SET state='failed',updated_at=$2,completed_at=$2,last_error='claim budget exhausted' WHERE id=$1`, claim.ActivityID, now); err != nil {
			return Claim{}, err
		}
		if err := appendHistory(ctx, tx, claim.WorkflowID, &claim.ActivityID, nil, "activity.failed", map[string]any{"reason": "attempt_budget_exhausted"}, now); err != nil {
			return Claim{}, err
		}
		var stopErr error
		if claim.Kind == ActivityCompensation {
			stopErr = e.failCompensation(ctx, tx, claim.WorkflowID, "attempt_budget_exhausted", "claim budget exhausted", now)
		} else {
			stopErr = e.beginCompensation(ctx, tx, claim.WorkflowID, WorkflowFailed, "attempt_budget_exhausted", "claim budget exhausted", now)
		}
		if stopErr != nil {
			return Claim{}, stopErr
		}
		if err := tx.Commit(ctx); err != nil {
			return Claim{}, err
		}
		return Claim{}, ErrNoActivity
	}
	if err := ValidateActivityTransition(currentState, ActivityRunning); err != nil {
		return Claim{}, err
	}

	claim.AttemptNumber++
	claim.AttemptID, err = newID()
	if err != nil {
		return Claim{}, err
	}
	claim.LeaseID, err = newID()
	if err != nil {
		return Claim{}, err
	}
	claim.LeaseToken, err = newID()
	if err != nil {
		return Claim{}, err
	}
	claim.WorkerID = workerID
	if observed, ok := tx.(*observedTx); ok {
		observed.readyAge = max(time.Duration(0), now.Sub(readyAt))
	}
	claim.ClaimedAt = now
	claim.AttemptDeadline = minTime(now.Add(attemptTimeout), workflowDeadline)
	claim.LeaseExpiresAt = minTime(now.Add(leaseDuration), claim.AttemptDeadline)

	if _, err := tx.Exec(ctx, `
		UPDATE activities SET state = $2, attempt_count = $3, updated_at = $4 WHERE id = $1`,
		claim.ActivityID, ActivityRunning, claim.AttemptNumber, now); err != nil {
		return Claim{}, fmt.Errorf("mark activity running: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO attempts (id, activity_id, number, worker_id, state, started_at, deadline_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		claim.AttemptID, claim.ActivityID, claim.AttemptNumber, workerID, AttemptRunning, now, claim.AttemptDeadline); err != nil {
		return Claim{}, fmt.Errorf("persist attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO leases (id, activity_id, attempt_id, token, worker_id, state, acquired_at, heartbeat_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)`,
		claim.LeaseID, claim.ActivityID, claim.AttemptID, claim.LeaseToken, workerID, LeaseActive, now, claim.LeaseExpiresAt); err != nil {
		return Claim{}, fmt.Errorf("persist lease: %w", err)
	}
	if err := appendHistory(ctx, tx, claim.WorkflowID, &claim.ActivityID, &claim.AttemptID, "activity.claimed", map[string]any{
		"attempt": claim.AttemptNumber, "version": claim.ActivityVersion, "step_key": claim.StepKey, "kind": claim.Kind, "lease_expires_at": claim.LeaseExpiresAt, "worker_id": workerID,
	}, now); err != nil {
		return Claim{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Claim{}, fmt.Errorf("commit claim: %w", err)
	}
	return claim, nil
}

// Heartbeat extends current ownership up to the attempt and phase deadlines.
// An expired or fenced token returns ErrLeaseLost, including at exact expiry.
func (e *Engine) Heartbeat(ctx context.Context, claim Claim, extension time.Duration) (time.Time, error) {
	if extension <= 0 {
		return time.Time{}, fmt.Errorf("heartbeat extension must be positive")
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("begin heartbeat: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	workflowID, state, err := lockOwnershipWorkflow(ctx, tx, claim.LeaseToken)
	if err != nil {
		return time.Time{}, err
	}
	if !workingWorkflow(state) {
		return time.Time{}, ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `SELECT l.id FROM leases l JOIN activities a ON a.id=l.activity_id JOIN attempts at ON at.id=l.attempt_id WHERE l.token=$1 FOR UPDATE OF a, at, l`, claim.LeaseToken); err != nil {
		return time.Time{}, err
	}
	now, err := e.now(ctx, tx)
	if err != nil {
		return time.Time{}, err
	}
	if settled, err := e.settleDeadline(ctx, tx, workflowID, claim.LeaseToken, now); err != nil {
		return time.Time{}, err
	} else if settled {
		if err := tx.Commit(ctx); err != nil {
			return time.Time{}, err
		}
		return time.Time{}, errors.Join(ErrLeaseLost, ErrExecutionDeadline)
	}
	requestedExpiry := now.Add(extension)
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE leases l
		SET heartbeat_at = $2, expires_at = LEAST(GREATEST(l.expires_at, $3),at.deadline_at,CASE WHEN w.state='compensating' THEN w.compensation_deadline_at ELSE w.deadline_at END)
		FROM activities a, attempts at, workflow_executions w
		WHERE l.token = $1
		  AND l.activity_id = a.id
		  AND l.attempt_id = at.id
		  AND a.workflow_id = w.id
		  AND l.state = 'active'
		  AND a.state = 'running'
		  AND at.state = 'running'
		  AND l.expires_at > $2
		RETURNING l.expires_at`, claim.LeaseToken, now, requestedExpiry).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("heartbeat activity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, fmt.Errorf("commit heartbeat: %w", err)
	}
	return expiresAt, nil
}

// CompleteActivity atomically acknowledges current ownership, stores JSON output,
// and advances the stored plan. A duplicate or stale acknowledgement is fenced.
func (e *Engine) CompleteActivity(ctx context.Context, claim Claim, output json.RawMessage) error {
	if len(output) == 0 {
		output = json.RawMessage("null")
	}
	if err := validatePayload(output); err != nil {
		return err
	}
	return e.finishActivity(ctx, claim, output, nil)
}

// FailActivity records a non-nil task failure and its persisted retry decision.
func (e *Engine) FailActivity(ctx context.Context, claim Claim, activityErr error) error {
	if activityErr == nil {
		return fmt.Errorf("activity error is required")
	}
	return e.finishActivity(ctx, claim, nil, activityErr)
}

func (e *Engine) finishActivity(ctx context.Context, claim Claim, output json.RawMessage, activityErr error) error {
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return fmt.Errorf("begin activity completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, workflowState, err := lockOwnershipWorkflow(ctx, tx, claim.LeaseToken)
	if err != nil {
		return err
	}
	if !workingWorkflow(workflowState) {
		return ErrLeaseLost
	}

	var workflowID string
	var position int
	var activityState ActivityState
	var activityKind ActivityKind
	var attemptState AttemptState
	var leaseState LeaseState
	var expiresAt time.Time
	var leaseID string
	err = tx.QueryRow(ctx, `
		SELECT a.workflow_id, a.position, a.state, at.state, l.state, l.expires_at, l.id,a.kind
		FROM leases l
		JOIN activities a ON a.id = l.activity_id
		JOIN attempts at ON at.id = l.attempt_id
		WHERE l.token = $1 AND l.activity_id = $2 AND l.attempt_id = $3
		FOR UPDATE OF l, a, at`, claim.LeaseToken, claim.ActivityID, claim.AttemptID).Scan(
		&workflowID, &position, &activityState, &attemptState, &leaseState, &expiresAt, &leaseID, &activityKind,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("lock activity completion: %w", err)
	}
	now, err := e.now(ctx, tx)
	if err != nil {
		return err
	}
	if settled, err := e.settleDeadline(ctx, tx, workflowID, claim.LeaseToken, now); err != nil {
		return err
	} else if settled {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errors.Join(ErrLeaseLost, ErrExecutionDeadline)
	}
	if activityState != ActivityRunning || attemptState != AttemptRunning || leaseState != LeaseActive || !expiresAt.After(now) {
		return ErrLeaseLost
	}
	if err := ValidateLeaseTransition(leaseState, LeaseReleased); err != nil {
		return err
	}

	if activityErr != nil {
		d := delivery{workflowID: workflowID, activityID: claim.ActivityID, attemptID: claim.AttemptID, leaseID: leaseID}
		if err := e.recordAttemptFailure(ctx, tx, d, AttemptFailed, failureMessage(activityErr), isPermanent(activityErr), now); err != nil {
			return err
		}
	} else {
		if err := ValidateActivityTransition(activityState, ActivityCompleted); err != nil {
			return err
		}
		if err := ValidateAttemptTransition(attemptState, AttemptCompleted); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE leases SET state = $2, ended_at = $3 WHERE token = $1`, claim.LeaseToken, LeaseReleased, now); err != nil {
			return fmt.Errorf("release completed lease: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET state = $2, ended_at = $3 WHERE id = $1`, claim.AttemptID, AttemptCompleted, now); err != nil {
			return fmt.Errorf("complete attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE activities SET state = $2, output = $3, updated_at = $4, completed_at = $4, last_error='' WHERE id = $1`, claim.ActivityID, ActivityCompleted, output, now); err != nil {
			return fmt.Errorf("complete activity: %w", payloadDatabaseError(err))
		}
		if err := appendHistory(ctx, tx, workflowID, &claim.ActivityID, &claim.AttemptID, "activity.completed", map[string]any{"attempt": claim.AttemptNumber}, now); err != nil {
			return err
		}

		var nextActivityID string
		err := tx.QueryRow(ctx, `
			SELECT id FROM activities
			WHERE workflow_id = $1 AND position > $2 AND state = 'blocked' AND kind=$3
			ORDER BY position
			LIMIT 1
			FOR UPDATE`, workflowID, position, activityKind).Scan(&nextActivityID)
		switch {
		case err == nil:
			if err := ValidateActivityTransition(ActivityBlocked, ActivityReady); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE activities SET state = $2, updated_at = $3,available_at=$3 WHERE id = $1`, nextActivityID, ActivityReady, now); err != nil {
				return fmt.Errorf("schedule next activity: %w", err)
			}
			if err := appendHistory(ctx, tx, workflowID, &nextActivityID, nil, "activity.ready", map[string]any{}, now); err != nil {
				return err
			}
		case errors.Is(err, pgx.ErrNoRows):
			if workflowState == WorkflowCompensating {
				if err := completeCompensation(ctx, tx, workflowID, now); err != nil {
					return err
				}
				break
			}
			if err := ValidateWorkflowTransition(WorkflowRunning, WorkflowCompleted); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state = $2, updated_at = $3, completed_at = $3, output = $5 WHERE id = $1 AND state = $4`, workflowID, WorkflowCompleted, now, WorkflowRunning, output); err != nil {
				return fmt.Errorf("complete workflow: %w", payloadDatabaseError(err))
			}
			if err := appendHistory(ctx, tx, workflowID, nil, nil, "workflow.completed", map[string]any{}, now); err != nil {
				return err
			}
		default:
			return fmt.Errorf("find next activity: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit activity completion: %w", err)
	}
	return nil
}

func appendHistory(ctx context.Context, tx pgx.Tx, workflowID string, activityID, attemptID *string, eventType string, details any, at time.Time) error {
	encoded, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("encode history event %q: %w", eventType, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO execution_history (workflow_id, activity_id, attempt_id, event_type, details, recorded_at)
		VALUES ($1, $2, $3, $4, $5, $6)`, workflowID, activityID, attemptID, eventType, encoded, at); err != nil {
		return fmt.Errorf("append history event %q: %w", eventType, err)
	}
	if observed, ok := tx.(*observedTx); ok {
		observed.record(workflowID, activityID, attemptID, eventType, encoded, at)
	}
	return nil
}
