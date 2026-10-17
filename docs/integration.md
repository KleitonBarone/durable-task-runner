# Integrating the current engine

This guide describes implemented behavior. Planned APIs and release gates live in
[ROADMAP.md](../ROADMAP.md). The current API is experimental.

## Start with a working program

[cmd/example/main.go](../cmd/example/main.go) is a complete program using only the
public Go package. It defines typed workflow input, creates two activity specs,
registers the workflow and typed handlers, opens PostgreSQL, applies migrations,
starts an execution, runs its handlers, and prints the timeline. The shared typed
definitions and deduplicated fake service are in [workflow.go](../cmd/example/workflow.go).
The final email step reads the account result through its handler context.

Install an exact pushed revision with
`go get github.com/KleitonBarone/durable-task-runner@<commit>`.
`go run ./cmd/dev consumer` builds and runs a separate consumer module. Set
`DURABLE_CONSUMER_VERSION` to a pushed SHA to use a fresh module cache without
local replacements. There is no release tag yet.

The default example starts worker and scheduler loops in one process and shuts
them down after completion. For separate processes, start local PostgreSQL, set
`DATABASE_URL` to its URL, and run migrations once. For the local Compose database,
set the URL in PowerShell with
`$env:DATABASE_URL = 'postgres://durable:durable@localhost:54329/durable?sslmode=disable'`
or in a POSIX shell with
`export DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable'`.

```sh
go run ./cmd/dev up
go run ./cmd/example -role migrate
```

In two terminals using that URL, run:

```sh
go run ./cmd/example -role worker
go run ./cmd/example -role scheduler
```

In another terminal, start and inspect an execution:

```sh
go run ./cmd/example -role start -key welcome/account-42
go run ./cmd/example -role status -id <printed-workflow-id>
```

Repeating the same start key returns the same execution. Use a new key for a new
execution. Stop the worker and scheduler with Ctrl+C, then stop PostgreSQL with
`go run ./cmd/dev down`. Each process uses `signal.NotifyContext`, waits for runtime
shutdown, and closes its own pool. The `verify` mode automates this process setup
with bounded child lifetimes and checks results, attempts, and fake effects.

## Define and persist the work

`durable.Define` associates a name with a typed input and a function returning a
linear `[]durable.ActivitySpec`. Use a typed activity's `Step` method to serialize
its input. Register the definition with `durable.Register` before starting it.

The plan function runs before the persistence transaction. Keep it deterministic
and free of side effects. It must return at least one activity. `durable.Start`
serializes the workflow input and persists the execution, every activity, and
initial history together. Only the first step starts ready.

Completing a step makes the next one ready in the same transaction. Plans are
fixed at start; branching, parallel activities, and child workflows are absent.
Use `durable.DefineActivity[Input, Output](name)` to share one activity declaration
between `activity.Step(key, input)` in a plan and `activity.Handle(handler)` in a
worker. Step keys are unique within a workflow, independent of activity name.
Conflicting type declarations are rejected within a registry. Keep named Go types
compatible across deployments; these identities do not replace schema versioning.
The raw `NewActivity` and `Handle` functions remain available for existing users.
Outputs are stored as JSON and readable through `Engine.Activities`.

Inside a handler, use `activity.Result(ctx, "earlier-key")` for typed prior-step
output. Self, future, and incomplete results return `ErrResultUnavailable`.
Use `activity.WorkflowResult(ctx, engine, id)` for the final step's typed result
after successful completion. `Engine.Workflow` also includes the raw final output;
`Engine.Attempts` lists each activity's attempts. Use `errors.Is` with `ErrNotFound`,
`ErrTypeMismatch`, and `ErrLeaseLost`. Errors retain their wrapped causes.

Each workflow input, step input, and output has a 1 MiB encoded JSON limit.
Payloads must be valid UTF-8 JSON without NUL strings or numbers outside float64
decoding range. The standard `encoding/json` rules apply, including unknown-field
handling. PostgreSQL jsonb constraints also apply, including rejection of unpaired
surrogate escapes. Invalid payloads return `ErrInvalidInput`; oversized ones also match
`ErrPayloadTooLarge`. Invalid low-level completion output leaves the lease
unacknowledged; the built-in worker records invalid handler output as task failure.

Without a key, starting twice creates two executions. Pass
`durable.WithStartKey("request-42")` to `Start` to deduplicate concurrent calls
within a workflow definition name and engine database/schema. Reusing the key
with JSON-equivalent input returns the original execution, including terminal
executions. Different input returns `ErrStartConflict`. Keys contain 1-200 UTF-8
bytes without NUL and live as long as the execution row. There is no automatic
retention. Keep keys private to the application; they are not authentication.

A start error may have followed a successful commit. Retry with the same key and
input to resolve that ambiguity. Use a new key to intentionally start new work.
Plans must remain deterministic and free of side effects even for duplicate starts.

## Own the database lifecycle

`durable.Open` creates a pool and verifies connectivity. `Engine.Close` closes that
owned pool. Use `durable.New` with an existing `pgxpool.Pool` if the application owns
the pool; closing the engine does not close that supplied pool.

Call `Engine.Migrate` explicitly as a deployment/setup step before processing work.
Migrations are embedded in the binary and serialized with a PostgreSQL transaction
advisory lock. The example's `migrate`, `demo`, and `verify` modes apply them.
Standalone `worker`, `scheduler`, `start`, and `status` modes expect setup to have
completed already.

Use a database/schema dedicated to engine tables. Production connection security,
credentials, access controls, and backups belong to the hosting application. The
Compose credentials in this repository are for local development.

## Run workers and one scheduler

Register handlers with a typed activity's `Handle` method. `durable.NewWorker` takes an engine,
configuration, and registrations. Only the worker ID and at least one handler are
required. Zero configuration values select defaults: a 30-second lease, heartbeat
at one third of the lease, 100 ms polling, database operations bounded by the
smaller of five seconds or one third of the lease, and five seconds to stop a
handler. Negative values are rejected. Database timeout plus heartbeat interval
must fit within the lease. The scheduler defaults to one-second polling and
100 recoveries per batch when configured with zeros.

- `Worker.RunOnce(ctx)` claims at most one activity and waits for its handler while
  heartbeating. It returns `false, nil` when no work is available.
- `Worker.Run(ctx)` polls continuously and runs one handler at a time. Run additional
  worker instances or processes for concurrency, using distinct worker IDs.
- `durable.NewScheduler(engine, pollInterval, batchSize)` creates the recovery loop.
  `Scheduler.Run(ctx)` polls continuously; `RunOnce(ctx)` performs one recovery batch.

Run a single scheduler alongside workers. Give each process a cancellable context
and handle its return value. For example, a scheduler process can use:

```go
// engine is already opened and migrated; ctx is cancelled on process shutdown.
scheduler, err := durable.NewScheduler(engine, time.Second, 100)
if err != nil {
    return err
}
return scheduler.Run(ctx)
```

Workers claim only activity names and type contracts matching their registered
handlers. Other work stays ready. Different worker processes can handle different
activity sets. `Worker.UnsupportedActivities(ctx)` lists up to 100 unsupported
ready types and their counts for this worker. It does not prove that no other
worker can process them. The low-level `Engine.ClaimActivity` is deliberately
unfiltered for applications implementing their own worker protocol.

`RunOnce` returns `TaskError` after recording a handler failure. Its cause remains
available through `errors.Is` and `errors.As`. `Worker.Run` continues to other
executions after recorded failures and cooperative lease loss. A panic or Goexit
in the handler goroutine records `HandlerPanicError`. Failure text is normalized
to UTF-8, escapes NUL, and is limited to 16 KiB in storage.

Worker and scheduler `Run` loops retry transient connection errors, operation
deadlines, PostgreSQL shutdown, serialization failures, and deadlocks. Five
consecutive reconnect waits use 100, 200, 400, 800, and 1600 ms delays; the sixth
failure returns. A successful operation resets this budget. Authentication,
schema, and other persistent SQL errors return immediately. These are runtime
reconnects, not activity retries. Failed acknowledgements remain subject to lease
recovery. `Health()` exposes local running/healthy state, consecutive runtime
failures, last runtime error, and last successful operation time. Before `Run`
starts and after it stops, health is false. It does not describe the worker fleet.

Cancellation of the process context asks the active handler to stop and waits for
its configured grace period. Shutdown does not record workflow cancellation or
release its lease; recovery follows expiry. An unresponsive handler returns
`ErrHandlerUnresponsive` and prevents that worker instance from invoking another
handler until the old one returns. Check this error before treating a joined
`context.Canceled` as clean shutdown. A process can still exit, and Go cannot kill
an individual handler goroutine. Attempt deadlines fence engine writes and ask
handlers to stop; external calls must also have their own limits.

Use one `Run` loop per worker instance. Concurrent `Run`/`RunOnce` calls are
rejected with `ErrWorkerBusy`. Stop runtime loops and wait for them before closing
an owned pool. `Open` defaults connection establishment to five seconds unless
the connection string specifies its own timeout. Caller-supplied pools remain
caller-owned.

## Make side effects safe to repeat

`durable.Metadata(ctx)` gives a handler its workflow ID, step key, activity ID,
worker ID, attempt ID/number, and stable `EffectKey`. Deduplicate the side effect
with that effect key or an application-owned business key. The engine key is
`durable:<activity UUID>` and stays the same for replacement attempts of that
logical step. Different steps have different keys. Attempt IDs/numbers and lease
tokens identify deliveries or ownership, so they must not identify the effect.
Pass a separate suffix when a handler performs several independent effects.

Deduplication belongs to the application or external service. Persist the key and
effect atomically in that system. Recording a key in the engine's database before
calling an unrelated external service cannot make the external effect atomic.

Lease expiry prevents stale engine acknowledgements, but cannot undo an external
effect. If a worker commits an external effect and dies before acknowledging it,
its replacement can repeat the effect. Heartbeats cannot extend an attempt or
workflow deadline. Applications must still bound their own external calls.

## Inspect progress and failures

- `Engine.Workflow(ctx, id)` reads current workflow status.
- `Engine.Activities(ctx, id)` reads ordered activities and their stored outputs.
- `Engine.Timeline(ctx, id)` reads history in database sequence order.
- `Engine.Attempts(ctx, activityID)` reads attempt numbers, workers, outcomes, and errors.

A recovered activity has history such as:

```text
workflow.started
activity.scheduled
activity.claimed
attempt.abandoned
activity.recovered
activity.claimed
activity.completed
workflow.completed
```

Current states are mutable; history records are append-only during execution.
Heartbeats update the lease without appending a history event on every beat.
The integration suite asserts this recovery timeline.

Handler errors are terminal by default. A step can opt into bounded retries using
`WithRetry`; every claim consumes its budget, including abandoned deliveries.
`WithAttemptTimeout` and `WithWorkflowTimeout` bound execution time independently
of heartbeat health. See [retry and deadline behavior](retries.md) for defaults,
terminal reasons, and restart scenarios.

Terminal executions never reopen. Reconcile effects in your application and start
a new execution with `WithPredecessor(oldID)` when needed. Use a new start key;
reusing the old key returns its original terminal execution. There is no
cancellation API, compensation, or definition versioning yet. Preserve the
meaning of registered workflow and activity names until their executions finish.

See [ADR 0002](decisions/0002-state-machine-and-failure-model.md) for transitions,
invariants, and the side-effect/acknowledgement failure window, and
[ADR 0003](decisions/0003-public-integration-contract.md) for API guarantees.
