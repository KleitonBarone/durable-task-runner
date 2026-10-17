# Operate an application

Run application-owned worker processes and one scheduler against PostgreSQL 17.
Definitions, handlers, credentials, and process lifecycle stay in your application.
Use patched supported Go toolchains, currently Go 1.26.9 and Go 1.27.2 or later
patches. The CLI inspects engine state; it does not load handlers.

## Inspect with the CLI

Install `github.com/KleitonBarone/durable-task-runner/cmd/durable` at the same
revision as your application. Inside this checkout, use `go run ./cmd/durable`.
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
