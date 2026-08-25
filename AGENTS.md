# AGENTS.md

## Project orientation

Durable Task Runner explores how multi-step jobs survive retries, cancellation, and worker failure. Correct state transitions matter more than feature count.

Read `README.md` and `ROADMAP.md` before changing scope. This repository is in planning, so never describe a planned capability as implemented.

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

The initial runtime has not been selected. Record the decision before scaffolding. Prefer types that make invalid workflow and step states difficult to represent.

## Verification

There are no build or test commands yet. Add real commands here when the toolchain exists. Do not add placeholder scripts that always pass.

Use deterministic tests with a controllable clock. Cover worker crashes between side effects and acknowledgements, expired leases, duplicate delivery, cancellation races, retry exhaustion, and compensation failure.

Do not use real third-party services in tests. Model activities with deterministic fakes and explicit failure injection.

## Documentation boundaries

Keep human setup, examples, and durability guarantees in `README.md`. Keep agent operating rules here. Record changes to execution semantics in an architecture decision and update `ROADMAP.md` when milestone scope changes.
