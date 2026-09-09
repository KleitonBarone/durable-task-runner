# AGENTS.md

## Project orientation

Durable Task Runner explores how multi-step jobs survive retries, cancellation, and worker failure. Correct state transitions matter more than feature count.

Read `README.md` and `ROADMAP.md` before changing scope. Milestones 0 and 1 are implemented. Never describe milestone 2 or later work as implemented.

## Product rules

- Persist workflow intent before scheduling work.
- Treat activities as at-least-once. Require idempotency or an explicit deduplication strategy.
- Keep workflow state transitions explicit, append-only where practical, and tested.
- A worker heartbeat extends ownership. It must not silently turn into permanent ownership.
- Cancellation is a state transition, not a process kill. Define what happens to in-flight activities.
- Compensation is explicit workflow behavior. Do not pretend rollback is automatic.
- Workflow version changes must not corrupt executions already in progress.

## Keep the execution model understandable

Start with one scheduler, worker processes, and one durable database. Do not add a broker, distributed lock service, or workflow DSL until the existing model cannot express a required behavior.

The runtime is Go 1.24 or later with PostgreSQL 17 and `pgx`. Keep the single-module shape described in `docs/decisions/0001-runtime-database-and-repository.md`. Prefer types that make invalid workflow and activity states difficult to represent.

## Verification

Run the relevant checks before handing off changes:

```sh
gofmt -w .
go vet ./...
go vet -tags=integration ./...
go test ./...
DURABLE_TEST_DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable' go test -tags=integration ./...
```

`make check` runs the non-mutating formatting check, static analysis, unit tests, and integration tests after `make deps-up` starts PostgreSQL.

Use deterministic tests with a controllable clock. The current milestone tests must cover worker crashes between side effects and acknowledgements, expired leases, duplicate delivery, concurrent claims, invalid transitions, and recovery. Add cancellation races, retry exhaustion, and compensation failure tests when milestone 2 implements those behaviors.

Do not use real third-party services in tests. Model activities with deterministic fakes and explicit failure injection.

## Documentation boundaries

Keep human setup, examples, and durability guarantees in `README.md`. Keep agent operating rules here. Record changes to execution semantics in an architecture decision and update `ROADMAP.md` when milestone scope changes.
