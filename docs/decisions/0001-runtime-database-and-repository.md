# ADR 0001: Runtime, database, and repository shape

Status: accepted, 2026-08-27

## Context

The first two milestones need to prove durable claiming and crash recovery without hiding the state model behind infrastructure. The repository had no runtime or database choice.

## Decision

Use Go 1.24 or later and PostgreSQL 17. Use `pgx` for PostgreSQL access. Keep migrations embedded in the Go binary and expose an explicit migration command through `Engine.Migrate`.

Keep one Go module with these parts:

- The root `durable` package owns models, transitions, persistence, workers, and the scheduler.
- `migrations/` owns ordered SQL migrations.
- `cmd/example/` contains the deterministic example workflow.
- `docs/decisions/` records changes to execution semantics.

PostgreSQL is the source of truth and the coordination mechanism. A worker claims work with row locks and `SKIP LOCKED`. The scheduler finds expired leases in the same database. There is no message broker, distributed lock service, workflow DSL, or separate control-plane service.

The database stores a fully expanded linear activity plan when a workflow starts. Workflow definition code remains in the Go process. This keeps recovery independent of re-running workflow code after a process crash.

## Consequences

Starting a workflow requires a database transaction that writes the execution, every planned activity, and history before the first activity becomes visible to workers.

PostgreSQL availability limits progress. This is acceptable for the first release because adding another durable coordinator would create a second source of truth.

Definitions are not versioned yet. Operators must keep a registered name compatible while its executions are in progress. Milestone 3 will add a durable versioning rule.
