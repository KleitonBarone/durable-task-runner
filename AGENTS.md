# AGENTS.md

## Project orientation

Durable Task Runner is a Go library for persisted linear jobs that survive worker failure. Correct state transitions matter more than feature count.

Read `README.md` and `ROADMAP.md` before changing scope. The bounded v1 roadmap is complete and v1.0.0 is published. Preserve its documented execution, compatibility, and retention contracts. New scope needs a new plan.

## Product rules

- Persist workflow intent before scheduling work.
- Treat activities as at-least-once. Require idempotency or an explicit deduplication strategy.
- Keep workflow state transitions explicit, append-only where practical, and tested.
- A worker heartbeat extends ownership. It must not silently turn into permanent ownership.
- Cancellation is a state transition, not a process kill. Define what happens to in-flight activities.
- Compensation is explicit workflow behavior. Do not pretend rollback is automatic.
- Workflow version changes must not corrupt executions already in progress.
- Keep name/version/schema identities immutable, including cleanup. Preserve applied migration SQL; runtime mutations check the current checksum catalog under a shared migration lock.
- Keep stable step keys and effect keys independent of attempts and lease tokens. Built-in workers claim only compatible activity names and type contracts.
- A recorded task failure must not stop a healthy worker. Preserve bounded reconnect and shutdown behavior, including one active handler per worker instance.

## Keep the execution model understandable

Start with one scheduler, worker processes, and one durable database. Do not add a broker, distributed lock service, or workflow DSL until the existing model cannot express a required behavior.

The runtime is Go 1.26.9 or later with PostgreSQL 17 and `pgx`. ADR 0008 supersedes ADR 0001's Go minimum after dependency/security verification. Keep the single-module shape described in `docs/decisions/0001-runtime-database-and-repository.md`. Prefer types that make invalid workflow and activity states difficult to represent.

Keep the public library in the root `durable` package. Group related declarations
in the existing concern files before adding another root file. See the repository
map in `CONTRIBUTING.md` for source ownership.

Put public API database scenarios in `tests/integration`, process scenarios in
`tests/e2e`, and database benchmarks in `tests/benchmarks`. Keep unit tests and
GoDoc examples beside the library. Root integration tests are reserved for private
ownership races, migration internals, and history/type compatibility. Own test
pools and clocks in fixtures; do not export engine internals to relocate a test.

## Verification

Run the relevant checks before handing off changes:

```sh
go run ./cmd/dev fmt
go run ./cmd/dev check
go run ./cmd/dev race
go run ./cmd/dev vuln
```

`check` starts isolated PostgreSQL, checks formatting and static analysis, runs unit/integration/process tests and the example, verifies a separate consumer module with worker/scheduler recovery, restores a snapshot into a second isolated database, and cleans up its own containers. It does not need `up` and does not use the development database. Run `race` for runtime/concurrency changes. Make targets call these same commands.

Use `go run ./cmd/dev test -run TestName .` for focused unit work. Explicit `integration` runs require `DURABLE_TEST_DATABASE_URL` pointing to a disposable database and fail when it is missing. See `CONTRIBUTING.md` for direct Go commands, WSL behavior, diagnostics, and targeted repetitions.

Use deterministic tests with a controllable clock. Cover worker crashes between side effects and acknowledgements, expired leases, duplicate delivery, concurrent claims, invalid transitions, recovery, retry exhaustion, deadline/cancellation races, non-cooperative handlers, reverse cleanup, and compensation failure. Cleanup preserves the original cause and has an independent deadline.

Do not use real third-party services in tests. Model activities with deterministic fakes and explicit failure injection.

Keep `testdata/history` fixtures portable with UTC timestamps and normalized identities. Inspect fixture changes as compatibility changes; ordinary tests must not rewrite them. Migration fixtures cover every prior repository schema and in-progress phases.

## Documentation boundaries

Keep human setup, examples, and durability guarantees in `README.md`. Keep agent operating rules here. Record changes to execution semantics in an architecture decision and update `ROADMAP.md` when milestone scope changes.
