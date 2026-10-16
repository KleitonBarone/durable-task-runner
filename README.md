# Durable Task Runner

Durable Task Runner is a Go library for background jobs that need to survive
worker crashes. It stores a sequence of activities in PostgreSQL, tracks each
attempt, and recovers unfinished work when a worker disappears.

Use it for jobs such as imports or account provisioning where losing progress
between steps would leave work unfinished. Your application supplies Go handlers;
the engine stores progress and coordinates workers through PostgreSQL.

The project is experimental. The durable execution core works, but retries,
timeouts, cancellation, compensation, and safe workflow version upgrades are
still planned. See the [roadmap to v1](ROADMAP.md).

## Try it

You need Go 1.24 or later and Docker with Compose. PostgreSQL 17 runs locally
through Compose.

```sh
git clone https://github.com/KleitonBarone/durable-task-runner.git
cd durable-task-runner
docker compose up -d --wait
DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable' go run ./cmd/example
```

In PowerShell, replace the last line with:

```powershell
$env:DATABASE_URL = 'postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go run ./cmd/example
```

The example runs a two-step customer welcome workflow and prints its execution
timeline, ending in `workflow.completed`. Both handlers are local fakes; they do
not create real accounts or send email. The example runs in one process and does
not start a continuous scheduler.

Stop PostgreSQL with `docker compose down`. Its development volume is preserved.

## Use it in an application

Start with the complete [Go example](cmd/example/main.go) and the
[integration guide](docs/integration.md).

1. Define a workflow that turns typed input into a fixed sequence of activities.
2. Register the definition and open an engine connected to PostgreSQL.
3. Apply the embedded migrations, then start an execution.
4. Register typed activity handlers and run workers.
5. Run one scheduler to recover expired leases. Read status and history through
   the engine's query methods.

Workers can run as separate processes. The library provides worker and scheduler
loops; your application owns their configuration and lifecycle. No message broker
or separate control service is required.

For now, use the checkout to evaluate integration. The Go module path needs to be
aligned with this repository before a normal tagged installation is ready.

## What survives a crash?

The engine commits the workflow and its full activity plan before workers can
claim work. Completing a step and making its successor ready share a transaction.
State changes and their history events commit together.

Workers hold expiring leases and heartbeat while handling an activity. If a worker
disappears, the scheduler abandons its attempt and makes the activity available
again. The old lease token can no longer acknowledge it.

Activity delivery is **at least once**. A worker can perform a side effect and
crash before recording success, so another worker may repeat it. Your handlers
must use application-level idempotency or deduplication. The engine does not
provide exactly-once side effects.

A handler error currently fails the workflow. Activity outputs are stored, but
later steps do not automatically receive them. Keep definitions and handlers
compatible while executions are in progress.

## Development

Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, focused tests, database checks,
and the current verification limits. The [roadmap](ROADMAP.md) puts a reproducible
development loop and process crash tests before further execution features.

## License

[MIT](LICENSE)
