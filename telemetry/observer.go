// Package telemetry optionally adapts durable runtime observations to
// application-owned OpenTelemetry providers. The core runtime does not import it.
package telemetry

import (
	"context"
	"fmt"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Observer struct {
	events   metric.Int64Counter
	duration metric.Float64Histogram
	readyAge metric.Float64Histogram
	tracer   trace.Tracer
}

// New uses providers configured and shut down by the application. It creates no
// exporter, network client, background service, or global provider registration.
func New(metrics metric.MeterProvider, traces trace.TracerProvider) (*Observer, error) {
	if metrics == nil || traces == nil {
		return nil, fmt.Errorf("metric and trace providers are required")
	}
	meter := metrics.Meter("github.com/KleitonBarone/durable-task-runner/telemetry")
	events, err := meter.Int64Counter("durable.events", metric.WithDescription("Observed durable transitions and runtime outcomes"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("durable.duration", metric.WithUnit("s"), metric.WithDescription("Successful claim transaction or handler duration"))
	if err != nil {
		return nil, err
	}
	readyAge, err := meter.Float64Histogram("durable.ready_age", metric.WithUnit("s"), metric.WithDescription("Ready time before a compatible claim"))
	if err != nil {
		return nil, err
	}
	return &Observer{events: events, duration: duration, readyAge: readyAge, tracer: traces.Tracer("github.com/KleitonBarone/durable-task-runner/telemetry")}, nil
}

func (o *Observer) Observe(ctx context.Context, event durable.Observation) {
	typeName, outcome, reason := eventType(event.Type), eventOutcome(event.Outcome), eventReason(event.Reason)
	phase := "unknown"
	if event.Kind == durable.ActivityForward || event.Kind == durable.ActivityCompensation {
		phase = string(event.Kind)
	}
	labels := []attribute.KeyValue{attribute.String("event", typeName), attribute.String("phase", phase), attribute.String("outcome", outcome), attribute.String("reason", reason)}
	options := metric.WithAttributes(labels...)
	o.events.Add(ctx, 1, options)
	if typeName == "activity.claimed" || typeName == "handler.finished" {
		o.duration.Record(ctx, max(0, event.Duration.Seconds()), options)
	}
	if typeName == "activity.claimed" {
		o.readyAge.Record(ctx, max(0, event.ReadyAge.Seconds()), options)
	}
	at := event.At
	if at.IsZero() {
		at = time.Now()
	}
	started := at
	if typeName == "activity.claimed" || typeName == "handler.finished" {
		started = at.Add(-max(time.Duration(0), event.Duration))
	}
	attributes := append(labels, attribute.String("durable.workflow_id", event.WorkflowID), attribute.String("durable.activity_id", event.ActivityID), attribute.String("durable.attempt_id", event.AttemptID), attribute.String("durable.worker_id", event.WorkerID), attribute.String("durable.step_key", event.StepKey), attribute.String("durable.version", event.Version))
	_, span := o.tracer.Start(ctx, "durable."+typeName, trace.WithTimestamp(started), trace.WithAttributes(attributes...))
	switch outcome {
	case "failed", "compensation_failed", "error", "panic", "fatal", "database", "schema", "deadline", "timed_out":
		span.SetStatus(codes.Error, outcome)
	}
	span.End(trace.WithTimestamp(at))
}

// Labels use finite vocabularies even when an application directly supplies an
// observation. Execution/worker/version identities belong to spans, never metrics.
func eventType(value string) string {
	switch value {
	case "workflow.started", "workflow.completed", "workflow.failed", "workflow.timed_out", "workflow.cancel_requested", "workflow.cancelled", "workflow.compensating", "workflow.compensation_completed", "workflow.compensation_failed", "activity.scheduled", "compensation.scheduled", "activity.ready", "activity.claimed", "activity.completed", "activity.failed", "activity.timed_out", "activity.cancelled", "activity.retry_scheduled", "activity.recovered", "attempt.abandoned", "attempt.timed_out", "attempt.cancelled", "handler.finished", "lease.lost", "runtime.error":
		return value
	default:
		return "unknown"
	}
}

func eventOutcome(value string) string {
	switch value {
	case "started", "completed", "failed", "timed_out", "cancel_requested", "cancelled", "compensating", "compensation_completed", "compensation_failed", "scheduled", "ready", "claimed", "retry_scheduled", "recovered", "abandoned", "success", "error", "panic", "lost", "fatal", "database", "schema", "deadline":
		return value
	default:
		return "unknown"
	}
}

func eventReason(value string) string {
	switch value {
	case "":
		return "none"
	case "handler_error", "permanent_error", "attempt_budget_exhausted", "attempt_deadline", "workflow_deadline", "compensation_deadline", "lease_expired", "cancelled", "forward_stopped", "retry_due":
		return value
	default:
		return "unknown"
	}
}
