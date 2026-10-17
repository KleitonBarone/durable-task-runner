# Operate an application

Run application-owned worker processes and one scheduler against PostgreSQL 17.
Definitions, handlers, credentials, and process lifecycle stay in your application.
Use patched supported Go toolchains, currently Go 1.26.9 and Go 1.27.2 or later
patches. The CLI inspects engine state; it does not load handlers.

## Inspect with the CLI

Install the CLI at the same revision as your application:

```sh
go install github.com/KleitonBarone/durable-task-runner/cmd/durable@v1.0.0
```

Inside this checkout, use `go run ./cmd/durable`.
Set `DATABASE_URL` for the database you intend to inspect. The CLI uses at most
two connections and defaults to a ten-second operation deadline.

```sh
durable migrate
durable status -state running
durable status -definition provision -version 1
durable status -state failed -reason attempt_budget_exhausted
durable status -compensation failed
durable activities -id <workflow-id>
durable attempts -id <activity-id>
durable leases -id <activity-id>
durable timeline -id <workflow-id> -limit 50
durable cancel -id <workflow-id> -actor operator -reason "customer requested"
```

`retry_wait` is an activity state. Find its workflow with `status`, then inspect
`NextActivity.State` and `AvailableAt`, or use `activities`. Workflow filters use
the [workflow states](decisions/0002-state-machine-and-failure-model.md), including
`running`, `cancel_requested`, `compensating`, and terminal outcomes. A completed
cleanup preserves its original failed, cancelled, or timed-out workflow outcome.

Commands print one JSON page. Pass its `next_cursor` to the same command with
`-cursor`, preserving its filters and `-sensitive` choice. Pages default to 50 rows
and allow 1-500. `-before` accepts an RFC3339 workflow creation cutoff. Use
`-timeout` to set a bounded database-operation limit. Failures return nonzero.

The library exposes the same bounded operations through `ListWorkflows`,
`ListActivities`, `ListAttempts`, `ListLeases`, and `ListTimeline`.
`WorkflowSummary` includes age, outcome, cancellation settlement, compensation,
and the next pending activity's name/version/state/due time.

## Read a waiting execution

- `ready` needs a worker advertising the exact name, version, Go contract, and
  payload identities. Compare `activities` with deployed registrations. Your
  application can call `Worker.UnsupportedActivities` for a particular worker.
  This is not a fleet availability detector.
- `retry_wait` cannot run before its persisted `AvailableAt`. A running scheduler
  promotes it after that time. Attempts show the decision and remaining budget.
- `running` has an attempt and expiring lease. Inspect its last heartbeat and
  deadline before treating a handler as abandoned. Recovery starts after expiry.
- `blocked` waits for an earlier step. Inspect the earlier attempt or workflow
  terminal reason. Later blocked steps do not run after forward failure.
- `cancel_requested` awaits scheduler settlement and any declared cleanup.
  `compensating` runs cleanup under its independent deadline. Inspect cleanup
  activities and `CompensationError` separately from the original failure.

Terminal executions stay terminal. Reconcile external effects in application code
and start a new execution with a new start key and `WithPredecessor(oldID)`.
Never change rows to ready or reuse an expired lease token as a recovery procedure.

## Redaction and cursor consistency

Default pages omit payloads, start/effect keys, arbitrary error text, and
cancellation actor/reason. `-sensitive` exposes error/cancellation text and history
details, which can contain application data. Lease tokens remain hidden. Activity
names, schemas, stable step keys, and IDs are diagnostic metadata; avoid putting
secrets in them. Result APIs and the original raw queries are application APIs
that can return stored payloads. Protect database access accordingly.

Pages use increasing database keys and capture an upper bound on the first page.
They are live reads, not a transaction snapshot spanning requests. A later commit
with an older reserved key can be missed after the cursor passes it. Rows can
change state or disappear under retention. No key repeats during a scan. Start
a fresh scan to refresh concurrent progress; use PostgreSQL snapshot/export tools
when a transactionally consistent audit export is required.

For handler changes and database upgrades, follow [upgrades](upgrades.md). A
restored snapshot can redeliver effects; application deduplication must cover the
business lifetime independently of engine history.

Enable [structured diagnostics and optional OpenTelemetry](observability.md) in
your application to measure claim age, handler time, recovery, and runtime failures.

## Health and database capacity

`Worker.Health` and `Scheduler.Health` describe their local `Run` loop. A process
starts unhealthy until its first successful operation. Recorded task failures and
fenced ownership are task outcomes, not a broken database connection. Inspect
`Running`, `Healthy`, `ConsecutiveFailures`, and `LastSuccessAt` for process health.
`LastError` can contain driver details, so redact it before exposing an endpoint.
`RunOnce` does not maintain loop health. A healthy loop does not prove that every
pending version has a compatible worker or that a long handler is making progress.

Workers execute one handler per instance. Share a `pgxpool.Pool` with `New` and
configure `MaxConns` deliberately. Budget connections for concurrent engine
operations, handler result reads, heartbeats, the scheduler, and inspection,
plus migration/maintenance headroom. Do not hold an application transaction while
calling an engine method that needs another pool connection. A starting budget
for four worker instances is four concurrent handler/result operations, four
heartbeat/control operations, one scheduler, and two inspection connections.
This is a planning allowance, not a measured minimum. Include separate handler
database pools in PostgreSQL's total `max_connections` budget. `Open` uses pgx's
pool defaults unless its connection string supplies `pool_max_conns`.

Defaults are a 100 ms idle worker poll, a 30 s lease, a heartbeat every 10 s,
and a database-operation timeout of 5 s. Busy workers claim again immediately.
The scheduler defaults to a 1 s poll, batches of 100, and a 5 s operation timeout.
Keep database timeout and heartbeat headroom inside the lease. Increase idle
polling intervals when latency allows, and measure ready age and database load
before increasing workers or batch size. See the [recorded benchmarks](benchmarks.md).

Transient connection failures use bounded reconnect waits of 100, 200, 400, 800,
and 1600 ms. The sixth consecutive failure stops `Run`; application supervision
decides whether to restart it. Authentication and schema failures stop immediately.
Lost acknowledgements are recovered through leases, not replayed in memory.
If PostgreSQL is unavailable, inspection is unavailable too. Use application health
and `runtime.error` telemetry to distinguish that outage from a task failure.

## Time, credentials, and deployment

Production mutations sample PostgreSQL `clock_timestamp()` after acquiring the
relevant locks. Keep the database host clock synchronized. A large forward clock
jump can expire work early; a backward jump delays expiry. Local monotonic timers
drive waiting and handler-duration measurements, not persisted lease ownership.
Use `SystemClock` in production and reserve `ManualClock` for tests.

Use encrypted database connections with certificate verification, such as
`sslmode=verify-full` and a trusted root certificate. The local Compose credentials
and disabled TLS belong only to disposable development databases. Supply secrets
through application configuration; avoid printing connection URLs. Restrict network
access and grant the runtime role only schema usage, table SELECT/INSERT/UPDATE/
DELETE, and sequence USAGE/SELECT on engine objects. A separate migration owner
needs DDL rights. Arrange default privileges for future migrations. Inspection can
use a SELECT-only role; cancellation and migration need their respective rights.
Do not expose the CLI or sensitive pages as an unauthenticated service.

On shutdown, cancel the worker's `Run` context, wait for its bounded grace period,
then close the engine/pool. The default grace is 5 s. The handler receives context
cancellation; if it ignores that request, the instance refuses another handler and
the process should exit. Unacknowledged work waits for lease recovery. Choose the
process supervisor's termination allowance longer than the configured grace and
database timeout. Keep one scheduler running; brief deployment overlap is safe
under workflow locks and is covered by the bounded load test.

For upgrades, stop new starts if necessary, preserve old handler versions until
they drain, and apply migrations with the migration owner before starting binaries
that require the new schema. Follow [upgrade and restore procedures](upgrades.md).
Take backups that include the migration catalog, start keys, and application effect
records. Test restoration into an isolated database before relying on a backup.
Never repair a stopped execution by editing its state. Reconcile the external
effect and create a linked replacement execution through the public API.

## Retention and start keys

The scheduler never deletes executions. An application can explicitly call:

```go
result, err := engine.PurgeTerminal(ctx, durable.RetentionPolicy{
    CompletedBefore: cutoff,
    Limit: 100,
})
```

Supply a bounded context and a nonzero cutoff no later than the engine's current
time. Only terminal executions completed strictly before the cutoff qualify.
The default batch is 100 workflows, with a maximum of 500. Batches use workflow
locks and skip work owned by another mutation. Running, retrying, cancelling,
and compensating executions are protected. Repeat bounded calls according to
your application's maintenance policy.

Deletion removes workflow output, activities and their inputs/results, attempts,
leases, and history in the same transaction. A batch counts workflows, so its
related-row cost depends on plan/history size. Archive required audit records
before purging. Existing predecessor links in other executions remain IDs without
a foreign key; creating a new predecessor link still requires an available
terminal execution.

Start-key deduplication lasts indefinitely. For keyed executions, the original
workflow input and schema identity move into an independent key record during
purge. Input is stored once while the execution is live. A matching start after
purge returns `ErrExecutionExpired`; use `errors.As` for `*ExecutionExpiredError`
to read its original ID. Different JSON input or schema still conflicts. PostgreSQL
JSON equality is preserved, including object key order and equivalent numbers.
A purge cannot release a key for new work, even when it races with starts.

The retained key record contains workflow input and belongs in your data policy.
There is no key-expiration API in v1. Unkeyed executions leave no deduplication
record. Application effect-key records have their own business lifetime and must
survive as needed independently of engine retention and backup restore.
