# Observe runtime behavior

Observers are optional. With no observer, the engine starts no telemetry process
and imports no OpenTelemetry package. The durable timeline remains the audit
record; diagnostic notifications can be lost if a process exits after commit.

## Structured logs

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
engine, err := durable.New(pool, registry, durable.SystemClock{},
    durable.WithObserver(durable.SlogObserver(logger)))
```

`Open` accepts the same options. An engine can have several observers. Logs include
event/outcome, workflow/activity/attempt identities, and available worker, stable
step, version, phase, and reason fields. Claim and handler events measure elapsed
duration; claims also measure ready-work age using the persisted time authority.
A successor's ready time starts when the prior step completes, excluding blocked
time. Handler duration uses the process's monotonic elapsed timer.

Transition callbacks run only after their transaction commits and releases locks.
Rolled-back transitions emit nothing. Handler timing and runtime failures describe
local observations and do not imply that a handler result committed. Lease loss
is reported once per worker delivery, including a rejected acknowledgement.
Runtime errors distinguish database, deadline, schema, and other fatal failures.

Payloads, effect/start keys, ownership tokens, and arbitrary error/cancellation
text are absent. Names, step keys, versions, and IDs remain diagnostic metadata,
so keep secrets out of those identifiers. Sensitive error text is available only
through explicit [inspection](operations.md), not observer records.

An `Observer` implementation must be safe for concurrent calls and return promptly.
Use a bounded application-owned buffer or the telemetry SDK's batch exporter for
network work. A slow observer delays its caller after commit. The engine catches
observer panics so diagnostics cannot change committed outcomes. Observers are
trusted application code and must return normally.

## Optional OpenTelemetry adapter

Import `github.com/KleitonBarone/durable-task-runner/telemetry` and configure your
own OpenTelemetry metric and trace providers:

```go
observer, err := telemetry.New(meterProvider, tracerProvider)
// Handle err, then pass durable.WithObserver(observer) to New or Open.
```

The adapter creates no exporter, network client, global provider, or service.
Your application owns provider startup, shutdown, sampling, and export policy.
Its [OpenTelemetry dependency](https://github.com/open-telemetry/opentelemetry-go/tree/v1.47.0)
requires Go 1.26, within this module's patched Go 1.26.9 minimum. The core package
does not import the adapter; ordinary binaries do not link it.

| Instrument | Meaning |
| --- | --- |
| `durable.events` | Count of observed transitions and runtime outcomes |
| `durable.duration` | Claim transaction and handler durations, seconds |
| `durable.ready_age` | Ready time before a compatible claim, seconds |

Metric labels are finite `event`, `phase`, `outcome`, and `reason` values. No
execution, worker, name, version, step, or effect identity becomes a metric label.
Unknown values map to `unknown`. Traces carry correlation identities as attributes
and use the measured duration for claim/handler spans. These are diagnostic event
spans, not persisted distributed-trace context across worker crashes. Use your
application's tracing inside handlers when it needs external-service spans.

Useful event filters include `attempt.abandoned` for recovered ownership,
`activity.retry_scheduled`/`activity.recovered` for redelivery, `activity.failed`
with `attempt_budget_exhausted`, `workflow.cancel_requested`, and
`workflow.compensation_failed`. Distinguish these task outcomes from
`runtime.error` when alerting on database connectivity and schema readiness.

Tests use an in-memory span exporter and manual metric reader. They verify elapsed
measurements and label bounds without a collector or external service.
