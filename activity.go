package durable

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ActivityDefinition ties one durable name to its Go input and output types.
// Keep names and types compatible while executions using them are in progress.
type ActivityDefinition[Input, Output any] struct {
	name                               string
	version, inputSchema, outputSchema string
}

// DefineActivity declares typed step inputs, handler outputs, and result reads.
func DefineActivity[Input, Output any](name string) ActivityDefinition[Input, Output] {
	return ActivityDefinition[Input, Output]{name: name, version: defaultVersion, inputSchema: legacySchema, outputSchema: legacySchema}
}

// WithVersion declares handler and payload compatibility explicitly. Keep the
// previous version registered until its forward and cleanup work has drained.
func (a ActivityDefinition[Input, Output]) WithVersion(version, inputSchema, outputSchema string) ActivityDefinition[Input, Output] {
	a.version, a.inputSchema, a.outputSchema = version, inputSchema, outputSchema
	return a
}

func (a ActivityDefinition[Input, Output]) contract() string {
	return typeIdentity(reflect.TypeFor[Input]()) + " -> " + typeIdentity(reflect.TypeFor[Output]())
}

func typeIdentity(t reflect.Type) string {
	if t.Name() != "" {
		return t.PkgPath() + "/" + t.String()
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return fmt.Sprintf("%s[%d]%s", t.Kind(), arrayLength(t), typeIdentity(t.Elem()))
	case reflect.Map:
		return "map[" + typeIdentity(t.Key()) + "]" + typeIdentity(t.Elem())
	case reflect.Struct:
		// All unnamed structs use the new format. Reusing the v1 identity for
		// non-embedded fields would still match ambiguous old embedded payloads.
		identity := "struct/v2{"
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			identity += fmt.Sprintf("%s/%s:%t:%s:%q;", field.PkgPath, field.Name, field.Anonymous, typeIdentity(field.Type), string(field.Tag))
		}
		return identity + "}"
	default:
		return t.String()
	}
}

func arrayLength(t reflect.Type) int {
	if t.Kind() == reflect.Array {
		return t.Len()
	}
	return 0
}

// Step binds input to a stable key within a workflow. Keys are independent of
// activity names, so one activity definition can appear more than once.
func (a ActivityDefinition[Input, Output]) Step(key string, input Input, options ...StepOption) (ActivitySpec, error) {
	if key == "" {
		return ActivitySpec{}, fmt.Errorf("%w: step key is required", ErrInvalidInput)
	}
	spec, err := NewActivity(a.name, input, options...)
	if err != nil {
		return ActivitySpec{}, err
	}
	spec.Key, spec.contract = key, a.contract()
	spec.Version, spec.InputSchema, spec.OutputSchema = a.version, a.inputSchema, a.outputSchema
	if err := (capability{a.name, a.version, a.contract(), a.inputSchema, a.outputSchema}).validate(); err != nil {
		return ActivitySpec{}, err
	}
	return spec, nil
}

// Handle binds the declaration's types and compatibility identities to a handler.
func (a ActivityDefinition[Input, Output]) Handle(handler func(context.Context, Input) (Output, error)) HandlerRegistration {
	registration := Handle(a.name, handler)
	registration.contract = a.contract()
	registration.Version, registration.InputSchema, registration.OutputSchema = a.version, a.inputSchema, a.outputSchema
	registration.explicitVersion = true
	return registration
}

// ActivityMetadata identifies this delivery and its logical effect. EffectKey
// stays stable across replacement attempts; AttemptID and AttemptNumber change.
type ActivityMetadata struct {
	Kind                  ActivityKind
	CompensatesActivityID *string
	WorkflowID            string
	ActivityID            string
	ActivityName          string
	ActivityVersion       string
	InputSchema           string
	OutputSchema          string
	StepKey               string
	WorkerID              string
	AttemptID             string
	AttemptNumber         int
	EffectKey             string
	AttemptDeadline       time.Time
}

func Metadata(ctx context.Context) (ActivityMetadata, bool) {
	current, ok := ctx.Value(activityContextKey{}).(activityContext)
	if !ok {
		return ActivityMetadata{}, false
	}
	claim := current.claim
	return ActivityMetadata{Kind: claim.Kind, CompensatesActivityID: claim.CompensatesActivityID, WorkflowID: claim.WorkflowID, ActivityID: claim.ActivityID,
		ActivityName: claim.ActivityName, StepKey: claim.StepKey, WorkerID: claim.WorkerID,
		ActivityVersion: claim.ActivityVersion, InputSchema: claim.InputSchema, OutputSchema: claim.OutputSchema,
		AttemptID: claim.AttemptID, AttemptNumber: claim.AttemptNumber,
		EffectKey: "durable:" + claim.ActivityID, AttemptDeadline: claim.AttemptDeadline}, true
}

// RetryPolicy is persisted per step. MaxAttempts includes crashes and defaults
// to three. RetryErrors defaults to false; abandonment can still redeliver.
type RetryPolicy struct {
	MaxAttempts     int           `json:"max_attempts"`
	RetryErrors     bool          `json:"retry_errors"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaximumInterval time.Duration `json:"maximum_interval"`
	JitterFraction  float64       `json:"jitter_fraction"`
}

// StepOption configures persisted retry, timeout, or cleanup behavior.
type StepOption struct {
	retry        *RetryPolicy
	timeout      *time.Duration
	compensation *ActivitySpec
}

// WithRetry sets the step's finite claim budget and optional handler-error retries.
func WithRetry(policy RetryPolicy) StepOption { return StepOption{retry: &policy} }

// WithAttemptTimeout bounds each delivery, including heartbeats. The default is five minutes.
func WithAttemptTimeout(timeout time.Duration) StepOption { return StepOption{timeout: &timeout} }

// WithCompensation declares cleanup for this step if it completes before the
// workflow stops. Cleanup input and policy are persisted with the forward plan.
func WithCompensation(cleanup ActivitySpec) StepOption { return StepOption{compensation: &cleanup} }

func normalizeActivity(spec *ActivitySpec, options []StepOption) error {
	if spec.Version == "" {
		spec.Version = defaultVersion
	}
	if spec.InputSchema == "" {
		spec.InputSchema = legacySchema
	}
	if spec.OutputSchema == "" {
		spec.OutputSchema = legacySchema
	}
	if err := (capability{spec.Name, spec.Version, spec.contract, spec.InputSchema, spec.OutputSchema}).validate(); err != nil {
		return err
	}
	retrySet, timeoutSet, compensationSet := false, false, false
	for _, option := range options {
		switch {
		case option.retry != nil && !retrySet:
			spec.policy = *option.retry
			retrySet = true
		case option.timeout != nil && !timeoutSet:
			if *option.timeout <= 0 {
				return fmt.Errorf("%w: attempt timeout must be positive", ErrInvalidInput)
			}
			spec.attemptTimeout = *option.timeout
			timeoutSet = true
		case option.compensation != nil && !compensationSet:
			cleanup := *option.compensation
			if cleanup.compensation != nil {
				return fmt.Errorf("%w: cleanup cannot declare nested compensation", ErrInvalidInput)
			}
			if strings.TrimSpace(cleanup.Name) == "" {
				return fmt.Errorf("%w: cleanup name is required", ErrInvalidInput)
			}
			if err := validatePayload(cleanup.Input); err != nil {
				return err
			}
			if err := normalizeActivity(&cleanup, nil); err != nil {
				return err
			}
			spec.compensation = &CompensationPlan{Name: cleanup.Name, Version: cleanup.Version, InputSchema: cleanup.InputSchema, OutputSchema: cleanup.OutputSchema, Contract: cleanup.contract, Input: append([]byte(nil), cleanup.Input...), RetryPolicy: cleanup.policy, AttemptTimeout: cleanup.attemptTimeout}
			compensationSet = true
		default:
			return fmt.Errorf("%w: duplicate or invalid step option", ErrInvalidInput)
		}
	}
	if spec.policy.MaxAttempts == 0 {
		spec.policy.MaxAttempts = 3
	}
	if spec.policy.MaximumInterval == 0 {
		spec.policy.MaximumInterval = time.Minute
	}
	if spec.attemptTimeout == 0 {
		spec.attemptTimeout = 5 * time.Minute
	}
	p := spec.policy
	if p.MaxAttempts < 1 || p.MaxAttempts > 1000 || p.InitialInterval < 0 || p.MaximumInterval < time.Microsecond || p.MaximumInterval > 24*time.Hour || p.InitialInterval > p.MaximumInterval || p.JitterFraction < 0 || p.JitterFraction > 1 || math.IsNaN(p.JitterFraction) || math.IsInf(p.JitterFraction, 0) {
		return fmt.Errorf("%w: invalid retry policy", ErrInvalidInput)
	}
	if (p.InitialInterval > 0 && p.InitialInterval < time.Microsecond) || spec.attemptTimeout < time.Microsecond || spec.attemptTimeout > 365*24*time.Hour {
		return fmt.Errorf("%w: duration outside supported range", ErrInvalidInput)
	}
	return nil
}

func retryDelay(policy RetryPolicy, activityID string, attempt int) time.Duration {
	delay := policy.InitialInterval
	for i := 1; i < attempt && delay < policy.MaximumInterval; i++ {
		if delay > policy.MaximumInterval/2 {
			delay = policy.MaximumInterval
		} else {
			delay *= 2
		}
	}
	if policy.JitterFraction != 0 && delay != 0 {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(activityID + ":" + strconv.Itoa(attempt)))
		unit := float64(hash.Sum64()%1000001) / 1000000
		delay = time.Duration(float64(delay) * (1 + policy.JitterFraction*(2*unit-1)))
	}
	return min(delay, policy.MaximumInterval).Truncate(time.Microsecond)
}

type permanentError struct{ cause error }

func (e *permanentError) Error() string { return e.cause.Error() }
func (e *permanentError) Unwrap() error { return e.cause }

// Permanent excludes this error from an opted-in handler retry policy.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{cause: err}
}

func isPermanent(err error) bool {
	var permanent *permanentError
	var panicErr *HandlerPanicError
	return errors.As(err, &permanent) || errors.As(err, &panicErr)
}

type delivery struct{ workflowID, activityID, attemptID, leaseID string }

// recordAttemptFailure runs under workflow and ownership locks. It stores the
// retry decision and due time together with the closed attempt and its history.
func (e *Engine) recordAttemptFailure(ctx context.Context, tx pgx.Tx, d delivery, state AttemptState, message string, permanent bool, now time.Time) error {
	var count int
	var policy RetryPolicy
	var kind ActivityKind
	if err := tx.QueryRow(ctx, `SELECT attempt_count,retry_policy,kind FROM activities WHERE id=$1`, d.activityID).Scan(&count, &policy, &kind); err != nil {
		return err
	}
	retry := count < policy.MaxAttempts && (state == AttemptAbandoned || (policy.RetryErrors && !permanent))
	decision := "stop"
	if count >= policy.MaxAttempts {
		decision = "exhausted"
	} else if retry {
		decision = "retry"
	}
	var due *time.Time
	if retry {
		at := now.Add(retryDelay(policy, d.activityID, count))
		due = &at
	}
	leaseState := LeaseReleased
	if state == AttemptAbandoned || state == AttemptTimedOut {
		leaseState = LeaseExpired
	}
	if err := ValidateAttemptTransition(AttemptRunning, state); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE leases SET state=$2,ended_at=$3 WHERE id=$1`, d.leaseID, leaseState, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE attempts SET state=$2,error_text=$3,ended_at=$4,retry_decision=$5,retry_due_at=$6 WHERE id=$1`, d.attemptID, state, message, now, decision, due); err != nil {
		return err
	}
	if state == AttemptAbandoned {
		if err := appendHistory(ctx, tx, d.workflowID, &d.activityID, &d.attemptID, "attempt.abandoned", map[string]any{"reason": message}, now); err != nil {
			return err
		}
	}
	if state == AttemptTimedOut {
		if err := appendHistory(ctx, tx, d.workflowID, &d.activityID, &d.attemptID, "attempt.timed_out", map[string]any{"reason": message}, now); err != nil {
			return err
		}
	}
	if retry {
		next := ActivityReady
		if due.After(now) {
			next = ActivityWaiting
		}
		if err := ValidateActivityTransition(ActivityRunning, next); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE activities SET state=$2,available_at=$3,last_error=$4,updated_at=$5 WHERE id=$1`, d.activityID, next, due, message, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET last_error=CASE WHEN state='compensating' THEN last_error ELSE $2 END,compensation_error=CASE WHEN state='compensating' THEN $2 ELSE compensation_error END,updated_at=$3 WHERE id=$1`, d.workflowID, message, now); err != nil {
			return err
		}
		event := "activity.retry_scheduled"
		if state == AttemptAbandoned && next == ActivityReady {
			event = "activity.recovered"
		}
		return appendHistory(ctx, tx, d.workflowID, &d.activityID, &d.attemptID, event, map[string]any{"available_at": due, "attempts_used": count, "max_attempts": policy.MaxAttempts}, now)
	}
	reason := "handler_error"
	if permanent {
		reason = "permanent_error"
	}
	if decision == "exhausted" {
		reason = "attempt_budget_exhausted"
	}
	activityState, event := ActivityFailed, "activity.failed"
	if state == AttemptTimedOut {
		activityState, event = ActivityTimedOut, "activity.timed_out"
		if decision != "exhausted" {
			reason = "attempt_deadline"
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE activities SET state=$4,last_error=$2,updated_at=$3,completed_at=$3 WHERE id=$1`, d.activityID, message, now, activityState); err != nil {
		return err
	}
	if err := appendHistory(ctx, tx, d.workflowID, &d.activityID, &d.attemptID, event, map[string]any{"error": message, "reason": reason}, now); err != nil {
		return err
	}
	if kind == ActivityCompensation {
		return e.failCompensation(ctx, tx, d.workflowID, reason, message, now)
	}
	if state == AttemptTimedOut {
		return e.timeoutWorkflow(ctx, tx, d.workflowID, reason, now)
	}
	return e.beginCompensation(ctx, tx, d.workflowID, WorkflowFailed, reason, message, now)
}
