# AGENTS.md

## Project orientation

Durable Task Runner explores how multi-step jobs survive retries, cancellation, and worker failure. Correct state transitions matter more than feature count.

Read `README.md` and `ROADMAP.md` before changing scope. Milestones 0, 1, 1A, 1B, and 2A are implemented. Milestone 2B is next. Describe later milestones as planned until their exit gates pass.

## Product rules

- Persist workflow intent before scheduling work.
- Treat activities as at-least-once. Require idempotency or an explicit deduplication strategy.
- Keep workflow state transitions explicit, append-only where practical, and tested.
- A worker heartbeat extends ownership. It must not silently turn into permanent ownership.
- Cancellation is a state transition, not a process kill. Define what happens to in-flight activities.
- Compensation is explicit workflow behavior. Do not pretend rollback is automatic.
- Workflow version changes must not corrupt executions already in progress.
- Keep stable step keys and effect keys independent of attempts and lease tokens. Built-in workers claim only compatible activity names and type contracts.
- A recorded task failure must not stop a healthy worker. Preserve bounded reconnect and shutdown behavior, including one active handler per worker instance.

## Keep the execution model understandable

Start with one scheduler, worker processes, and one durable database. Do not add a broker, distributed lock service, or workflow DSL until the existing model cannot express a required behavior.

The runtime is Go 1.24 or later with PostgreSQL 17 and `pgx`. Keep the single-module shape described in `docs/decisions/0001-runtime-database-and-repository.md`. Prefer types that make invalid workflow and activity states difficult to represent.

## Verification

Run the relevant checks before handing off changes:

```sh
go run ./cmd/dev fmt
go run ./cmd/dev check
go run ./cmd/dev race
```

`check` starts isolated PostgreSQL, checks formatting and static analysis, runs unit/integration/process tests and the example, verifies a separate consumer module with worker/scheduler processes, and cleans up its own containers. It does not need `up` and does not use the development database. Run `race` for runtime/concurrency changes. Make targets call these same commands.

Use `go run ./cmd/dev test -run TestName .` for focused unit work. Explicit `integration` runs require `DURABLE_TEST_DATABASE_URL` pointing to a disposable database and fail when it is missing. See `CONTRIBUTING.md` for direct Go commands, WSL behavior, diagnostics, and targeted repetitions.

Use deterministic tests with a controllable clock. Cover worker crashes between side effects and acknowledgements, expired leases, duplicate delivery, concurrent claims, invalid transitions, recovery, retry exhaustion, and deadline races. Add cancellation races and compensation failure tests with those features.

Do not use real third-party services in tests. Model activities with deterministic fakes and explicit failure injection.

## Documentation boundaries

Keep human setup, examples, and durability guarantees in `README.md`. Keep agent operating rules here. Record changes to execution semantics in an architecture decision and update `ROADMAP.md` when milestone scope changes.
