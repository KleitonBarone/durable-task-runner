package durable

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Observation contains diagnostics, never payloads, effect keys, ownership
// tokens, or arbitrary error text. Committed events are best effort notifications;
// the execution timeline remains the durable audit record.
type Observation struct {
	Type       string
	At         time.Time
	WorkflowID string
	ActivityID string
	AttemptID  string
	WorkerID   string
	StepKey    string
	Version    string
	Kind       ActivityKind
	Outcome    string
	Reason     string
	Duration   time.Duration
	ReadyAge   time.Duration
}

// Observer must be safe for concurrent calls and return promptly without network
// waits. Transition callbacks run after commit, outside locks. Timing and runtime
// notifications are local observations. Panics cannot change committed outcomes.
type Observer interface {
	Observe(context.Context, Observation)
}
type ObserverFunc func(context.Context, Observation)

func (f ObserverFunc) Observe(ctx context.Context, event Observation) { f(ctx, event) }

type EngineOption struct{ observer Observer }

func WithObserver(observer Observer) EngineOption { return EngineOption{observer: observer} }

func (e *Engine) observe(ctx context.Context, event Observation) {
	for _, observer := range e.observers {
		func() { defer func() { _ = recover() }(); observer.Observe(ctx, event) }()
	}
}

// SlogObserver emits correlated structured diagnostics with no sensitive text.
func SlogObserver(logger *slog.Logger) Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return ObserverFunc(func(ctx context.Context, event Observation) {
		attributes := []slog.Attr{slog.String("event", event.Type), slog.Time("recorded_at", event.At), slog.String("outcome", event.Outcome)}
		for _, field := range []struct{ key, value string }{{"workflow_id", event.WorkflowID}, {"activity_id", event.ActivityID}, {"attempt_id", event.AttemptID}, {"worker_id", event.WorkerID}, {"step_key", event.StepKey}, {"version", event.Version}, {"kind", string(event.Kind)}, {"reason", event.Reason}} {
			if field.value != "" {
				attributes = append(attributes, slog.String(field.key, field.value))
			}
		}
		if event.Type == "activity.claimed" || event.Type == "handler.finished" {
			attributes = append(attributes, slog.Duration("duration", event.Duration))
		}
		if event.Type == "activity.claimed" {
			attributes = append(attributes, slog.Duration("ready_age", event.ReadyAge))
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "durable execution", attributes...)
	})
}

type observedTx struct {
	pgx.Tx
	engine   *Engine
	started  time.Time
	events   []Observation
	readyAge time.Duration
}

func (tx *observedTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	for _, event := range tx.events {
		if event.Type == "activity.claimed" {
			event.Duration = time.Since(tx.started)
			event.ReadyAge = tx.readyAge
		}
		tx.engine.observe(ctx, event)
	}
	return nil
}

func (tx *observedTx) record(workflowID string, activityID, attemptID *string, eventType string, details []byte, at time.Time) {
	var fields struct {
		Version  string       `json:"version"`
		WorkerID string       `json:"worker_id"`
		StepKey  string       `json:"step_key"`
		Kind     ActivityKind `json:"kind"`
		Reason   string       `json:"reason"`
	}
	_ = json.Unmarshal(details, &fields)
	_, outcome, _ := strings.Cut(eventType, ".")
	event := Observation{Type: eventType, At: at, WorkflowID: workflowID, WorkerID: fields.WorkerID, StepKey: fields.StepKey, Version: fields.Version, Kind: fields.Kind, Outcome: outcome}
	if activityID != nil {
		event.ActivityID = *activityID
	}
	if attemptID != nil {
		event.AttemptID = *attemptID
	}
	switch fields.Reason {
	case "handler_error", "permanent_error", "attempt_budget_exhausted", "attempt_deadline", "workflow_deadline", "compensation_deadline", "lease_expired", "cancelled", "forward_stopped", "retry_due":
		event.Reason = fields.Reason
	}
	tx.events = append(tx.events, event)
}

func (e *Engine) runtimeError(ctx context.Context, err error) {
	outcome := "fatal"
	switch {
	case errors.Is(err, ErrIncompatibleSchema):
		outcome = "schema"
	case errors.Is(err, context.DeadlineExceeded):
		outcome = "deadline"
	case transientDatabaseError(err):
		outcome = "database"
	}
	e.observe(ctx, Observation{Type: "runtime.error", At: e.clock.Now(), Outcome: outcome})
}

func handlerOutcome(err error) string {
	var panicErr *HandlerPanicError
	switch {
	case err == nil:
		return "success"
	case errors.As(err, &panicErr):
		return "panic"
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		return "error"
	}
}
