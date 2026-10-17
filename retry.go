package durable

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type RetryPolicy struct {
	MaxAttempts     int           `json:"max_attempts"`
	RetryErrors     bool          `json:"retry_errors"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaximumInterval time.Duration `json:"maximum_interval"`
	JitterFraction  float64       `json:"jitter_fraction"`
}

type StepOption struct {
	retry   *RetryPolicy
	timeout *time.Duration
}

func WithRetry(policy RetryPolicy) StepOption             { return StepOption{retry: &policy} }
func WithAttemptTimeout(timeout time.Duration) StepOption { return StepOption{timeout: &timeout} }

func normalizeActivity(spec *ActivitySpec, options []StepOption) error {
	retrySet, timeoutSet := false, false
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
	if err := tx.QueryRow(ctx, `SELECT attempt_count,retry_policy FROM activities WHERE id=$1`, d.activityID).Scan(&count, &policy); err != nil {
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
	if state == AttemptAbandoned {
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
		if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET last_error=$2,updated_at=$3 WHERE id=$1`, d.workflowID, message, now); err != nil {
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
	if _, err := tx.Exec(ctx, `UPDATE activities SET state='failed',last_error=$2,updated_at=$3,completed_at=$3 WHERE id=$1`, d.activityID, message, now); err != nil {
		return err
	}
	if err := appendHistory(ctx, tx, d.workflowID, &d.activityID, &d.attemptID, "activity.failed", map[string]any{"error": message, "reason": reason}, now); err != nil {
		return err
	}
	return failWorkflow(ctx, tx, d.workflowID, reason, message, now)
}

func failWorkflow(ctx context.Context, tx pgx.Tx, id, reason, message string, now time.Time) error {
	if _, err := tx.Exec(ctx, `UPDATE workflow_executions SET state='failed',terminal_reason=$2,last_error=$3,updated_at=$4,completed_at=$4 WHERE id=$1 AND state='running'`, id, reason, message, now); err != nil {
		return err
	}
	return appendHistory(ctx, tx, id, nil, nil, "workflow.failed", map[string]any{"reason": reason}, now)
}
