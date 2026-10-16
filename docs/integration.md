# Integrating the current engine

This guide describes implemented behavior. Planned APIs and release gates live in
[ROADMAP.md](../ROADMAP.md). The current API is experimental.

## Start with a working program

[cmd/example/main.go](../cmd/example/main.go) is a complete program using only the
public Go package. It defines typed workflow input, creates two activity specs,
registers the workflow and typed handlers, opens PostgreSQL, applies migrations,
starts an execution, runs its handlers, and prints the timeline.

Use the checkout for now. The declared module is
`github.com/kleiton/durable-task-runner`, while the repository lives at
`github.com/KleitonBarone/durable-task-runner`. Normal installation from the
repository URL needs that mismatch resolved. Existing example imports match the
current `go.mod`; changing an import alone will not fix packaging.

## Define and persist the work

`durable.Define` associates a name with a typed input and a function returning a
linear `[]durable.ActivitySpec`. Use `durable.NewActivity` to serialize a step's
input. Register the definition with `durable.Register` before starting it.

The plan function runs before the persistence transaction. Keep it deterministic
and free of side effects. It must return at least one activity. `durable.Start`
serializes the workflow input and persists the execution, every activity, and
initial history together. Only the first step starts ready.

Completing a step makes the next one ready in the same transaction. Plans are
fixed at start; branching, parallel activities, and child workflows are absent.
Each activity receives the input stored in its spec. Outputs are stored as JSON
and readable through `Engine.Activities`; they are not automatically passed to
the next step. Activity names do not currently enforce matching input/output types
between a plan and a handler.

Starting twice creates two executions. There is no built-in start-key deduplication.
If a start returns an ambiguous database error, it may have committed. Do not assume
blindly starting again will reuse the same execution.

## Own the database lifecycle

`durable.Open` creates a pool and verifies connectivity. `Engine.Close` closes that
owned pool. Use `durable.New` with an existing `pgxpool.Pool` if the application owns
the pool; closing the engine does not close that supplied pool.

Call `Engine.Migrate` explicitly as a deployment/setup step before processing work.
Migrations are embedded in the binary and serialized with a PostgreSQL transaction
advisory lock. The example applies them at startup for convenience.

Use a database/schema dedicated to engine tables. Production connection security,
credentials, access controls, and backups belong to the hosting application. The
Compose credentials in this repository are for local development.

## Run workers and one scheduler

Register handlers with `durable.Handle`. `durable.NewWorker` takes an engine,
configuration, and registrations. The example shows all required configuration
fields. Lease duration, heartbeat interval, and poll interval must be positive;
the heartbeat interval must be shorter than the lease duration.

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

All workers against the same engine tables must currently support the same set of
activity names. Claims do not filter by registered handlers. A missing handler
fails claimed work. A handler error records workflow failure, and `Worker.Run`
returns the error, stopping that loop. The hosting application must decide how to
restart it. A scheduler/database error also returns to its caller.

Cancellation of the process context asks a handler to stop. Shutdown does not
record workflow cancellation or immediately release its lease; recovery follows
lease expiry. A handler that ignores its context may keep running after lease loss.
There is no hard execution timeout yet.

## Make side effects safe to repeat

Use an application-owned stable business key in activity input and deduplicate
the side effect using that key. Do not use an attempt number or lease token as
the logical effect identity; replacement attempts must identify the same effect.
There is no engine-managed effect key in the current handler API.

Lease expiry prevents stale engine acknowledgements, but cannot undo an external
effect. If a worker commits an external effect and dies before acknowledging it,
its replacement can repeat the effect. A heartbeat can keep a live handler's lease
alive indefinitely today, so applications must bound their own external calls.

## Inspect progress and failures

- `Engine.Workflow(ctx, id)` reads current workflow status.
- `Engine.Activities(ctx, id)` reads ordered activities and their stored outputs.
- `Engine.Timeline(ctx, id)` reads history in database sequence order.

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

Handler failure is terminal for the execution. Later activities stay blocked and
are not claimed. There are no automatic handler-error retries, cancellation API,
compensation steps, definition versions, or operator redrive API yet. Preserve the
meaning of registered workflow and activity names until their executions finish.

See [ADR 0002](decisions/0002-state-machine-and-failure-model.md) for transitions,
invariants, and the side-effect/acknowledgement failure window.
