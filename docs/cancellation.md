# Cancel a workflow

Use the public API from your application or an operator command:

```go
execution, err := engine.Cancel(ctx, workflowID, "operator@example.com", "import superseded")
```

Authorize the caller in your application. The actor and reason are diagnostic
text, not credentials. The actor must contain 1-200 UTF-8 bytes; reason may contain
up to 16 KiB. Neither may contain NUL. Do not put secrets in these fields.

The first committed request stops forward claims and fences active leases. It
records `cancel_requested`, actor, reason, and request time. Pending steps and
active attempts become `cancelled`; completed results remain available. The
scheduler settles the workflow as `cancelled` or starts declared compensation.
It never waits for a stopped forward handler. Required cleanup must finish or
fail before cancellation settles. If the scheduler is absent, the request stays
visible and forward claims remain fenced.

Workers observe cancellation at their next heartbeat/control check and cancel
the handler context. Handlers should use that context for bounded external calls.
A handler that ignores it cannot acknowledge success and blocks reuse of that
worker instance after its shutdown grace period. Cancellation does not prove
external work stopped and does not undo effects already performed.

Repeated requests return the execution and preserve the first request's metadata.
A completed, failed, or timed-out execution keeps its original outcome. Final
completion that commits before cancellation wins; cancellation that commits first
fences late completion. An already passed overall deadline keeps its timeout
reason. Reusing a start key still returns the original cancelled execution.

Read `Engine.Workflow` for `CancelActor`, `CancelReason`, `CancelRequestedAt`, and
`CancelSettledAt`. The timeline records the request, stopped attempts/steps, and
settlement. See [ADR 0005](decisions/0005-cancellation.md) for transaction rules.

Run the live-process demonstration with isolated PostgreSQL:

```sh
go run ./cmd/dev e2e -run TestProcessCancellationSurvivesWorkerAndSchedulerRestart .
```

The fixture cancels after a fake effect, observes the handler's context stopping,
restarts both runtime processes, and verifies no successor was claimed. Another
workflow completes after the restart. A saved stale token cannot acknowledge or
heartbeat. The original fake effect remains, demonstrating why cancellation is
different from undoing work.

Observed timeline:

```text
workflow.started
activity.scheduled
activity.scheduled
activity.claimed
workflow.cancel_requested
attempt.cancelled
activity.cancelled
activity.cancelled
workflow.cancelled
```
