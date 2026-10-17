# Reproduce failure recovery

These scenarios use real worker and scheduler processes against disposable
PostgreSQL. They perform local fake effects only. Each command provisions and
cleans up its own database and prints the persisted timeline. Run from a checkout
with Go and Docker available. The excerpts below were captured on 2026-10-17;
line wrapping is added for readability, and event order is unchanged.

## Worker crash after an effect

```sh
go run ./cmd/dev e2e '-run=TestProcessCrashRecovery' .
```

The harness kills a worker before and after its effect, restarts the scheduler,
and tries the old lease token. The after-effect case produced two effects without
application deduplication; the deduplicated case produced one effect across two
deliveries. Both recorded an abandoned attempt and a completed replacement.

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
attempt.abandoned activity.recovered activity.claimed activity.completed
activity.ready activity.claimed activity.completed workflow.completed]
```

The same command exercises bounded graceful shutdown. A committed successor and
rejected stale acknowledgement are asserted, not inferred from process exit.

## Durable retry and exhaustion

```sh
go run ./cmd/dev e2e '-run=TestProcessRetryBackoff|TestProcessRepeatedCrashes' .
```

The retry case restarts both processes during backoff and asserts the replacement
attempt starts no earlier than the original persisted due time.

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
activity.retry_scheduled activity.ready activity.claimed activity.completed
activity.ready activity.claimed activity.completed workflow.completed]
```

The exhaustion case kills three worker processes. Each claim consumes the finite
budget, leaving `attempt_budget_exhausted` as the terminal reason.

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
attempt.abandoned activity.recovered activity.claimed attempt.abandoned
activity.recovered activity.claimed attempt.abandoned activity.failed workflow.failed]
```

## Cancellation through restart

```sh
go run ./cmd/dev e2e '-run=TestProcessCancellation' .
```

The handler observes cancellation. After restarting both processes, no successor
effect runs, the old result is fenced, and an unrelated job still completes.

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
workflow.cancel_requested attempt.cancelled activity.cancelled activity.cancelled
workflow.cancelled]
```

## Cleanup crash and cleanup failure

```sh
go run ./cmd/dev e2e '-run=TestProcessCompensation' .
```

After a forward failure, the harness kills the worker after its cleanup effect.
Recovery delivers cleanup again with the same effect key. Successful cleanup
produces one effect from two deliveries and preserves the original failure:

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
activity.completed activity.ready activity.claimed activity.failed workflow.compensating
compensation.scheduled activity.claimed attempt.abandoned activity.retry_scheduled
activity.ready activity.claimed activity.completed workflow.compensation_completed
workflow.failed]
```

The second case injects further cleanup failures and exhausts the cleanup budget:

```text
timeline=[workflow.started activity.scheduled activity.scheduled activity.claimed
activity.completed activity.ready activity.claimed activity.failed workflow.compensating
compensation.scheduled activity.claimed attempt.abandoned activity.retry_scheduled
activity.ready activity.claimed activity.retry_scheduled activity.ready activity.claimed
activity.failed workflow.compensation_failed]
```

Its original error remains `injected forward failure`; the separate cleanup error
is `injected cleanup failure`. An operator must reconcile external state and start
a linked replacement if needed. Compensation never promises automatic rollback.

For ordinary diagnosis, use the bounded [operator CLI](operations.md), including
attempts, leases, pending version/due time, and explicit sensitive error inspection.
