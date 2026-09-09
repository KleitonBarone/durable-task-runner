# Roadmap

This roadmap builds the execution state machine before orchestration features.

## 0. Foundation

- [x] Define workflow, activity, attempt, and lease terminology
- [x] Record the runtime, database, and repository-shape decisions
- [x] Add formatting, static checks, focused tests, and CI
- [x] Document the workflow state machine and failure model

Decisions live in [ADR 0001](docs/decisions/0001-runtime-database-and-repository.md) and [ADR 0002](docs/decisions/0002-state-machine-and-failure-model.md). CI checks formatting, `go vet`, unit tests, and PostgreSQL integration tests.

## 1. Durable execution

- [x] Register a workflow definition
- [x] Start a workflow with typed input
- [x] Persist workflow and activity state
- [x] Claim activities through expiring leases
- [x] Record heartbeats and recover abandoned activities
- [x] Expose an execution timeline

The implemented workflow shape is a deterministic linear activity plan. Claims use PostgreSQL row locks and expiring lease tokens. Recovery abandons the old attempt and creates no hidden retry policy; a new claim creates the next attempt.

## 2. Control and recovery

- [ ] Add retry policies and bounded timeouts
- [ ] Support cancellation requests
- [ ] Add idempotency keys for activity attempts
- [ ] Support explicit compensation steps
- [ ] Make retry exhaustion visible and actionable

Milestone 2 must define cancellation races, timeout ownership, idempotency-key lifetime, and compensation failure before adding those transitions.

## 3. Safe evolution

- [ ] Version workflow definitions
- [ ] Keep in-progress workflows compatible with code changes
- [ ] Add replay tests for recorded histories
- [ ] Add traces, metrics, and structured logs
- [ ] Publish crash-recovery demonstrations

## Later, if justified

- [ ] Child workflows
- [ ] Signals and external events
- [ ] Scheduled workflows
- [ ] Multiple worker task queues

## Explicitly out of scope for the first release

- A visual workflow builder
- Arbitrary untrusted code execution
- Multi-region consensus
- Exactly-once activity side effects
