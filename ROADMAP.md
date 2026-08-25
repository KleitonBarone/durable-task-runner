# Roadmap

This roadmap builds the execution state machine before adding orchestration features.

## 0. Foundation

- [ ] Define workflow, activity, attempt, and lease terminology
- [ ] Record the runtime, database, and repository-shape decisions
- [ ] Add formatting, static checks, focused tests, and CI
- [ ] Document the workflow state machine and failure model

## 1. Durable execution

- [ ] Register a workflow definition
- [ ] Start a workflow with typed input
- [ ] Persist workflow and activity state
- [ ] Claim activities through expiring leases
- [ ] Record heartbeats and recover abandoned activities
- [ ] Expose an execution timeline

## 2. Control and recovery

- [ ] Add retry policies and bounded timeouts
- [ ] Support cancellation requests
- [ ] Add idempotency keys for activity attempts
- [ ] Support explicit compensation steps
- [ ] Make retry exhaustion visible and actionable

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
