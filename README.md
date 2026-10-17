# Durable Task Runner

Durable Task Runner is a Go library for background jobs that need to survive
worker crashes. It stores a sequence of activities in PostgreSQL, tracks each
attempt, and recovers unfinished work when a worker disappears.

Use it for jobs such as imports or account provisioning where losing progress
between steps would leave work unfinished. Your application supplies Go handlers;
the engine stores progress and coordinates workers through PostgreSQL.

The project is experimental. The durable core includes bounded retries,
execution deadlines, cooperative cancellation, and explicit compensation.
Safe workflow version upgrades are still planned. See the [roadmap to v1](ROADMAP.md).

## Try it

You need Go 1.24 or later and Docker with Compose. PostgreSQL 17 runs locally
through Compose.

```sh
git clone https://github.com/KleitonBarone/durable-task-runner.git
cd durable-task-runner
go run ./cmd/dev up
go run ./cmd/dev example
```

These commands also work in PowerShell. If Docker is installed only inside WSL,
the development command uses the Ubuntu distribution automatically.

The example runs fake customer welcome steps, prints the timeline and result,
then shuts down. It creates no real accounts or email. Other modes run workers
and the scheduler in separate processes.

Stop PostgreSQL with `go run ./cmd/dev down`. Its development volume is preserved.

## Use it in an application

Start with the complete [Go example](cmd/example/main.go) and the
[integration guide](docs/integration.md).

1. Declare typed activities and give each step a stable key in a linear plan.
2. Register the definition and open an engine connected to PostgreSQL.
3. Apply the embedded migrations, then start an execution with an optional deduplication key.
4. Register typed activity handlers and run workers.
5. Run one scheduler for recovery. Read prior-step results in handlers and inspect
   outcomes through public APIs.

Your application owns runtime configuration and lifecycle. No message broker or
separate control service is required.

Install an exact pushed revision, replacing `<commit>` with its SHA:

```sh
go get github.com/KleitonBarone/durable-task-runner@<commit>
```

Separate consumer installation is verified. Release tags are planned.

## What survives a crash?

The engine commits the workflow and its full activity plan before workers can
claim work. Completing a step and making its successor ready share a transaction.
State changes and their history events commit together.

Workers hold expiring leases and heartbeat while handling an activity. The
scheduler recovers abandoned work within its attempt budget. Old lease tokens
cannot acknowledge replacement attempts.

Activity delivery is **at least once**. A worker can perform a side effect and
crash before recording success, so another worker may repeat it. Your handlers
must use application-level idempotency or deduplication. Handler metadata includes
a stable effect key for this purpose. The engine does not provide exactly-once
side effects.

A handler error fails its workflow unless its step opts into bounded retries.
The default budget is three claims, including crashes. Attempts default to five
minutes and workflows to 24 hours; heartbeats cannot extend these limits. See
[retry and deadline options](docs/retries.md). The worker continues to other jobs.
The last step's output is the workflow result. Keep handlers compatible with
executions in progress.

Applications can [cancel an execution](docs/cancellation.md). A committed request
stops forward progress and asks handlers to stop; it does not undo external effects.
Optional [compensation](docs/compensation.md) runs declared cleanup for completed
steps in reverse order and records cleanup failures separately.

## Development

Run all checks with `go run ./cmd/dev check`. It starts disposable PostgreSQL,
runs formatting, static analysis, unit and integration tests, kills and recovers
real worker processes, runs the example and a separate consumer module, and
removes its test containers.

Read [CONTRIBUTING.md](CONTRIBUTING.md) for focused tests, race checks, and debugging.
See the [roadmap](ROADMAP.md) for the remaining work through v1.

## License

[MIT](LICENSE)
