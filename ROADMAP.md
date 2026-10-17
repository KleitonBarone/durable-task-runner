# Roadmap to v1

This is the implementation plan for the first complete release. Milestones 0 and
1, 1A, 1B, milestone 2, and 3A are implemented. Milestone 3B is next.
Every unchecked item below is planned work.

See the [delivery plan](docs/delivery-plan.md) for ordered commits, branches,
verification before each push, draft PR checkpoints, and release publication.

The release dependency scan was moved ahead of 3B after finding affected driver,
text, and toolchain versions. ADR 0008 sets the patched Go 1.26.9 baseline and
adds the reproducible `vuln` check. Final release scanning remains required.

## What completion means

Durable Task Runner should let a Go application run a sequence of background
activities, survive process failure, and explain what happened to each execution.
The application supplies Go definitions and handlers. PostgreSQL stores the plan,
progress, ownership, and history. One scheduler and worker processes drive it.

The primary user is a Go developer who already operates PostgreSQL and needs jobs
such as imports, account provisioning, or service automation to survive restarts.
Integration should require no extra control service or workflow language.

For v1, "100% implemented" means all release gates in this document pass:

- A separate Go application can install a tagged module, register typed steps,
  start a workflow, and inspect its result using public APIs.
- Linear plans support prior-step results, bounded retries and deadlines,
  cooperative cancellation, stable idempotency keys, and explicit compensation.
- Executions survive worker and scheduler restarts without losing committed intent.
  Stale workers cannot acknowledge work after losing ownership.
- Definition and handler versions keep in-progress executions safe during upgrades.
- Operators can identify exhausted, cancelled, timed-out, and compensation-failed
  executions, inspect their history, and follow a documented recovery procedure.
- A clean checkout has one reliable verification command, including real process
  crash tests against PostgreSQL. CI uses the same verification path.
- Setup, integration, upgrades, and operating limits are documented and verified.

Delivery remains at least once. An idempotency key helps an application deduplicate
an effect; it cannot make an arbitrary external service execute it exactly once.

The v1 boundary is finite. Child workflows, signals, recurring schedules, multiple
task queues, parallel graphs, dynamic branching, a UI, hosted execution,
cross-language SDKs, and a public HTTP control service are outside it. A visual
builder, arbitrary untrusted code execution, multi-region consensus, and exactly-once
external side effects remain out of scope. These exclusions do not prevent v1.

## Current state

### 0. Foundation, complete

- [x] Define workflow, activity, attempt, and lease terminology.
- [x] Choose Go, PostgreSQL, `pgx`, embedded migrations, and one module.
- [x] Add formatting, static analysis, unit and integration tests, and CI.
- [x] Document the state machine and failure model.

See [ADR 0001](docs/decisions/0001-runtime-database-and-repository.md) and
[ADR 0002](docs/decisions/0002-state-machine-and-failure-model.md).

### 1. Durable execution, complete

- [x] Register definitions and start executions with typed input.
- [x] Persist the complete linear plan before work becomes claimable.
- [x] Claim activities with expiring leases and fence stale acknowledgements.
- [x] Heartbeat leases and recover abandoned attempts.
- [x] Record transitions and expose an ordered execution timeline.

The original crash test simulates disappearance by withholding acknowledgement,
advancing a manual clock, and calling recovery. Milestone 1A adds real worker
process crashes, scheduler restart, graceful shutdown, and controlled loop tests.

### Gaps found in the current implementation

| Area | Evidence in the repository | Work required before v1 |
| --- | --- | --- |
| Installation | Canonical module is `github.com/KleitonBarone/durable-task-runner`, verified from a separate module | Implemented in 1B; candidate-tag verification remains in milestone 4 |
| Local loop | `cmd/dev` runs from PowerShell or POSIX shells and selects Docker through WSL when needed | Implemented in 1A; keep CI and local commands aligned |
| Database checks | Explicit integration runs require a reachable database; `check` provisions its own | Implemented in 1A; preserve failure on missing dependencies |
| Test control | `ManualClock` controls timestamps and runtime tickers | Implemented in 1A; extend deterministic coverage with new transitions |
| Process recovery | Separate worker/scheduler processes cover crashes before/after effects, shutdown, restart, and stale tokens | Implemented in 1A; extend the harness for retries, cancellation, and cleanup |
| Worker behavior | Workers filter compatible contracts, continue after task failures, and bound reconnect/shutdown | Implemented in 1B; persisted retry/deadline policies added in 2A |
| Failure acknowledgement | Explicit error presence controls failure, including empty messages and handler panics | Implemented in 1B; preserve this distinction in later terminal outcomes |
| Integration | Typed activities, stable step/effect keys, keyed starts, prior/final results, and external consumer | Implemented in 1B; tagged-module qualification remains in milestone 4 |
| Evolution | Versioned workflow/handler/cleanup contracts, migration guards, history fixtures, and restored pending work | Implemented in 3A; retain these upgrade checks for releases |
| Operations | Queries fetch one execution and unbounded activity/history lists | Bounded inspection, attempt details, diagnostics, and retention controls |

## Implementation order

Each row is a gate. Finish it before depending on it in the next row. Deliver
focused PRs, each with a runnable acceptance scenario. Do not combine the remaining
state-machine changes into one rewrite.

| Order | Milestone | Result |
| --- | --- | --- |
| 1 | 1A. Development and verification, complete | Reproducible checks, deterministic loop tests, process crash test |
| 2 | 1B. Integration contract, complete | Installable module, typed activities, usable results, resilient worker loop |
| 3 | 2A. Retry and timeout control | Persisted policies, due times, finite attempt budgets, bounded ownership |
| 4 | 2B. Cancellation | Transactional cancellation with defined races and cooperative handler shutdown |
| 5 | 2C. Compensation | Durable reverse-order cleanup and visible cleanup failures |
| 6 | 3A. Safe evolution | Versioned definitions and handlers, migration and history compatibility |
| 7 | 3B. Operations | Inspection, logs, metrics, traces, deployment and recovery guidance |
| 8 | 4. Release qualification | External integration, upgrades, reliability matrix, tagged v1 |

## 1A. Establish the development loop first

This gate is complete. `go run ./cmd/dev check` runs the full local/CI loop and
`go run ./cmd/dev race` checks concurrency. Keep this gate passing while extending
the runtime. See [CONTRIBUTING.md](CONTRIBUTING.md) for commands and measured timings.

### Deliverables

- [x] Add a small `cmd/dev` command using the standard library. Check Go and
  Docker/Compose prerequisites, report actionable failures, and support Docker
  inside WSL on Windows. Avoid requiring Make or a POSIX shell for the main path.
- [x] Provide the following commands, available in the current checkout.

| Command prefix: `go run ./cmd/dev` | Contract |
| --- | --- |
| `doctor` | Check prerequisites, selected Docker endpoint, and conflicts; change no configuration |
| `up` / `down` | Start or stop the named development database; preserve its data on shutdown |
| `test` | Run focused unit tests without Docker; accept package/test selection |
| `integration` | Run uncached PostgreSQL tests; fail on missing or unreachable database |
| `e2e` | Build example processes and run process scenarios in an isolated database |
| `check` | Create isolated PostgreSQL, wait for health, check formatting and vet, run unit/integration/e2e checks, then clean up |
| `race` | Run concurrency-sensitive unit/integration tests with the race detector on a supported toolchain |

- [x] Make existing Make targets thin aliases where practical. Keep direct `go`
  commands documented for debugging and preserve a fast unit-test path.
- [x] Give each full verification run a unique Compose project and temporary data,
  allocate available ports, and clean up only resources that run created. Keep it
  separate from `up` and from a caller-supplied database. No automatic volume reset.
- [x] Require a database URL for explicit tagged integration runs. Check connectivity
  before testing and use `-count=1`. Missing Docker or a missing database must fail,
  rather than produce a green run with skipped persistence tests.
- [x] Reuse the per-test schema helper. Bound setup, queries, waits, and cleanup;
  register cleanup immediately after resource creation. Preserve useful diagnostics
  when a scenario fails.
- [x] Add a small controllable timer/ticker dependency for worker and scheduler loops.
  Keep persisted-time and waiting behavior explicit. Use channels or barriers to
  coordinate tests; wall-clock deadlines detect hung tests.
- [x] Add external-package tests using only public APIs. Exercise `Worker.RunOnce`,
  `Worker.Run`, and `Scheduler.Run`, heartbeat loss, handler failure, and shutdown.
  Assert workflow, activity, attempt, lease, and history consistency together.
- [x] Add the process harness below. Keep test-only barriers and failure injection
  in test/example binaries rather than the public runtime API.
- [x] Make CI use the same checks, fail if required suites do not execute, enforce
  job timeouts, and retain sanitized process logs and timelines on failure. Run race
  checks on Linux and native Windows command/setup checks. Test the declared minimum
  Go version as well as the selected development version.

### First end-to-end scenario

Use a deterministic local fake effect stored separately from engine state. A parent
test launches a scheduler and worker as separate OS processes.

1. Start a two-step workflow through public APIs and wait for the first claim.
2. Wait for worker A to commit the fake effect, then block it before acknowledgement.
3. Kill only worker A. Keep PostgreSQL and the scheduler running.
4. Start worker B. Observe lease expiry and recovery through the real scheduler loop.
5. Assert attempt one is abandoned, attempt two completes, and the next step runs.
6. Assert both attempted effects are observable and the timeline explains recovery.
   Replay A's saved claim through a helper and reject its stale token.
7. Restart the scheduler and reconnect a new engine instance. Assert the final state
   and history remain readable.

This process test uses short, bounded real leases because the processes use real
time. In-process tests cover exact expiry boundaries with the manual clock. Do not
use sleeps as evidence of a transition; wait for barriers or bounded state predicates.

### Exit gate

- [x] From a clean checkout, one command runs all required checks and cleans up on
  success and failure. Record cold/warm durations in contributor docs; target a warm
  unit loop under five seconds and a warm full check under two minutes.
- [x] A missing dependency, failed child process, or unmet state assertion returns
  nonzero with enough information to reproduce the failure.
- [x] Concurrent claims and recovery pass 20 targeted repetitions locally; use
  bounded repetitions in CI. Process tests pass repeatedly without timing guesses
  or orphan processes.
- [x] README instructions succeed on a clean environment. Contributor docs give
  commands for focused runs, the full gate, failure reproduction, and cleanup.

Suggested PRs: command runner and dependency lifecycle; controllable loop tests;
process harness and crash recovery; CI parity and contributor instructions.

## 1B. Make the engine straightforward to integrate

This gate is complete. The example runs both as a local demo and as a standalone
consumer with separate application, worker, and scheduler processes. Remote
installation is verified against pushed commits without local replacements;
candidate-tag verification remains part of milestone 4.

### Deliverables

- [x] Correct module/import paths to the canonical repository location. Build a
  separate consumer module locally, then verify the pushed commit through normal
  module resolution. Advertise installation only when it works without `replace`.
- [x] Define a typed activity registration tying a stable name to input and output
  types. Use the same definition in plans and handlers. Validate duplicate step keys,
  malformed payloads, and incompatible registrations before execution where possible.
- [x] Give each planned step a stable key distinct from its activity type. Persist
  result identity. Add typed access to completed earlier-step output from handlers;
  reject reads of future/incomplete steps. Keep plans linear and precomputed, with
  no replay of arbitrary Go workflow code and no expression language.
- [x] Expose workflow status, final output, individual attempts, and stable errors
  such as not-found and lease-lost. Define output/payload limits, JSON encoding,
  and invalid-input behavior. Use the last forward step's output as the successful
  workflow result. Preserve causes for `errors.Is` and `errors.As`.
- [x] Add an optional caller-provided workflow start key. A unique constraint makes
  concurrent starts with one key return the same execution; conflicting inputs
  produce an error. Document scope, retention, and ambiguous commit outcomes. A
  completed execution does not restart under its original key.
- [x] Supply handler metadata including execution ID, step key, attempt number, and
  a stable effect key. The effect key stays the same across deliveries and retries
  of one logical step. Attempt numbers and lease tokens change.
- [x] Filter claims by supported activity identities. Missing handlers leave work
  pending with a diagnostic rather than fail it after claiming.
- [x] Separate recorded task failures from fatal runtime/configuration failures.
  One failed workflow must not stop a healthy worker processing other workflows.
  Classify transient database errors, bound reconnect backoff, report health, and
  define shutdown and lease-loss behavior. Start with one active handler per worker
  instance; use more instances/processes for concurrency.
- [x] Represent success and failure independently of error text. An error with an
  empty message must still fail the attempt. Define handler-panic behavior explicitly
  and verify that its attempt is either durably failed or recovered after process exit.
- [x] Provide validated defaults and document pool ownership, migration invocation,
  startup order, signal handling, and shutdown. Cancel handler contexts on lost
  ownership; this cannot forcibly stop a handler that ignores its context.
- [x] Add a complete integration example with application, worker, and scheduler
  entry points, plus a deduplicated fake-effect example. Use only public APIs.

### Exit gate

- [x] A standalone consumer starts a job, uses an earlier result in its next step,
  runs separate workers/scheduler, and reads the final result without internal SQL.
- [x] Duplicate starts, bad payloads, missing handlers, duplicate effect delivery,
  empty-message errors, handler panics, and a failing job beside a successful one
  have focused tests.
- [x] The README routes readers to a runnable example. The integration guide explains
  application responsibilities without requiring agent instructions.

Suggested PRs: module identity and consumer fixture; typed steps and results;
start/effect keys; worker lifecycle and claims; complete integration example.

## 2A. Add bounded retries and timeouts

Record new transitions and timing rules in an ADR before implementation. Persist
policy in the plan so a deployment cannot silently change it.

### Deliverables

- [x] Define retryable/permanent failures, maximum attempts, capped backoff, and
  deterministic jitter testing. Default handler-error retries off unless the caller
  opts in. Establish a separate finite total claim budget so abandonment can cause
  redelivery without enabling handler-error retries. Document bounded defaults as
  a prerelease change from today's unlimited crash recovery.
- [x] Persist the next eligible time and retry decision with the failed/abandoned
  attempt. No in-memory sleep implements durable backoff. Recovery and retry
  scheduling must be idempotent across scheduler restarts.
- [x] Define which failures consume the budget. For v1, every claimed attempt
  consumes one, including abandonment; expose this in history. A finite budget
  must stop repeated crash-and-reclaim cycles.
- [x] Support per-attempt execution limits and an overall workflow deadline.
  Distinguish deadlines from lease expiry, which detects lost ownership. Heartbeats
  cannot extend attempts or workflows beyond configured deadlines.
- [x] Establish one production time authority across processes, preferably PostgreSQL
  time sampled after acquiring relevant locks. Keep test time injectable. Cover
  stale pre-lock timestamps, clock skew, and boundary equality.
- [x] On timeout, fence the attempt before another delivery becomes eligible and
  cancel the handler context cooperatively. Late external side effects still need
  the application's deduplication strategy.
- [x] Expose exhaustion and timeout outcomes, attempt details, next-eligible time,
  and terminal reason. Keep failed executions terminal; later manual work starts
  a new execution linked to the old one.

### Exit gate

- [x] Manual-clock tests cover due boundaries, backoff caps, error classification,
  exhaustion, abandonment consuming a budget, heartbeat/deadline races, scheduler
  restart, stale completion, and completion racing with timeout.
- [x] Process tests prove restart during backoff preserves the due time, a hanging
  handler times out despite heartbeats, and another job still progresses.

Suggested PRs: policy/state ADR; durable due times and scheduling; deadlines;
exhaustion queries and process scenarios.

## 2B. Add cooperative cancellation

### Deliverables

- [x] Persist cancellation with actor/reason before acting on it. Repeated requests
  are safe. Define terminal-state behavior and serialize cancel, claim, completion,
  and timeout using a consistent database lock order.
- [x] A committed request prevents new forward claims. Pending/blocked steps become
  cancelled. An in-flight step receives context cancellation on its next control
  check/heartbeat and loses acknowledgement rights under the recorded transition.
- [x] Resolve completion races by transaction order. If final completion commits
  first, the execution stays completed. If cancellation commits first, late success
  cannot resume it. Record the outcome of each active attempt.
- [x] Distinguish cancellation requested from cancellation settled. Cancellation
  promises neither that external work stopped nor that side effects were undone.
  Never wait forever for a non-cooperative handler.
- [x] Add cancel/query APIs and an example. Integrate deadline expiry with the same
  stopping rules while preserving its distinct reason.

### Exit gate

- [x] Test cancellation before claim, during a handler, during retry wait, after
  completion, repeated requests, and races with claim/completion/expiry.
- [x] Cancel a live process execution, restart the worker, and prove no remaining
  forward step runs. A non-cooperative handler cannot change fenced engine state.

Suggested PRs: transition ADR and persisted request; worker cooperation and races;
public API and process cancellation.

## 2C. Add explicit compensation

### Deliverables

- [x] Allow optional compensation per forward step. Persist compensation inputs or
  references to committed results, handler identity, and retry/deadline policy before
  making compensation claimable.
- [x] On failure/cancellation, stop forward scheduling and compensate known completed
  steps in reverse order. Persist the trigger and compensating state. Reuse attempts,
  leases, fencing, and history for cleanup deliveries.
- [x] Give compensation stable effect keys distinct from forward effect keys.
  Cleanup is also delivered at least once and needs idempotency.
- [x] Define the ambiguous crash window. An external effect without a committed
  forward result is not automatically known to be compensatable. Document application
  reconciliation using stable keys or deduplication records.
- [x] Bound cleanup retries/timeouts. Preserve the original failure and record cleanup
  failures separately. For v1, stop on exhausted cleanup, expose `compensation_failed`,
  and require an application reconciliation run. Do not claim automatic rollback or
  silently reopen a terminal execution.
- [x] Give cleanup its own deadline so an expired forward-work deadline cannot
  disable required cleanup. Successful cleanup preserves the original failed or
  cancelled outcome and exposes compensation success separately. Settle cancellation
  after required cleanup finishes or exhausts its budget.
- [x] Define cancellation during compensation. Repeated cancellation does not cancel
  cleanup already required by the workflow's compensation policy.

### Exit gate

- [x] Test reverse order, absent compensation, crash after cleanup side effect,
  restart between cleanup steps, duplicate delivery, cancellation during cleanup,
  cleanup timeout, and exhaustion.
- [x] A process example fails its last forward step, restarts during compensation,
  and demonstrates successful cleanup and an injected cleanup failure.

Suggested PRs: semantics and persisted compensation; execution and recovery;
failure inspection and example.

## 3A. Support safe upgrades

### Deliverables

- [x] Persist workflow and activity handler versions, payload schema identity, and
  compensation versions. Include versions in claim capabilities. Never route an
  old execution to an incompatible new handler.
- [x] Keep old/new registrations available during draining. Unsupported versions
  remain pending with visible diagnostics. Document deploy, drain, and removal rules.
- [x] Continue executing the stored plan. Test history compatibility by replaying
  recorded transition fixtures into expected state, not rerunning workflow code.
  Include retry, cancellation, and compensation fixtures.
- [x] Test migrations from milestone 1 data and each supported released schema.
  Preserve applied files, add new migrations, detect incompatible schema versions,
  and test concurrent and interrupted migrations. Document backup/restore before
  destructive changes.
- [x] Define binary/schema rollback compatibility. Prefer additive migrations;
  document forward repair when an older binary would be unsafe.

### Exit gate

- [x] Start an old definition, deploy an incompatible new version, and finish each
  execution with its matching handlers. Recovery does not rerun definition code.
- [x] Migration/fixture tests preserve active leases, retry waits, cancellations,
  cleanup, and timeline interpretation across supported upgrades.
- [x] Restore a snapshot into an isolated database and finish pending work. Document
  that restoring older data can redeliver effects performed after the backup.

Suggested PRs: version identity/routing; coexistence example; migration/history
fixtures; upgrade and restore rehearsal.

## 3B. Make executions operable

### Deliverables

- [x] Add bounded cursor queries for workflows, attempts, leases, and timelines.
  Filter status/definition and expose last error, age, retry due time, cancel reason,
  version mismatch, and compensation state. Ordinary diagnosis needs no raw SQL.
- [x] Add a small CLI for migration, status, attempts, timeline, and cancellation.
  Definitions stay in application binaries; avoid a generic server or code loader.
- [ ] Emit structured logs with execution, step, attempt, worker, and version IDs.
  Add opt-in metrics/tracing hooks and an optional OpenTelemetry adapter. Keep the
  dependency outside the default runtime path if practical.
- [ ] Measure claim latency, ready-work age, handler duration, lease loss, recovery,
  retries, exhaustion, cancellation, and cleanup failure. Use low-cardinality metric
  labels. Hide payloads/effect keys by default and make redaction explicit.
- [ ] Document health, PostgreSQL connection budgets, polling/batches, clock
  assumptions, TLS/credentials, and graceful deployment. Keep one scheduler as the
  operating model and test brief overlap safely.
- [ ] Add opt-in terminal-execution retention with bounded deletion batches. Never
  delete active work. Remove related history intentionally under a documented policy,
  and preserve start-key deduplication for its promised lifetime.
- [ ] Add repeatable database benchmarks for claims, contention, recovery batches,
  and timeline growth. Publish workload/hardware; use measurements to document
  capacity and tune indexes, not to invent a scale claim.
- [ ] Publish runnable crash/recovery, retry/exhaustion, cancellation, and cleanup
  demonstrations with actual timeline output.

### Exit gate

- [ ] An operator can explain why a job waits or stops without database access.
- [ ] Test pagination under documented snapshot rules, redaction, retention racing
  with reads, and deduplication lifetime.
- [ ] A bounded load run has no duplicate active leases or unexplained stuck work.
  Diagnostics identify slow handlers, absent versions, and database outages.

Suggested PRs: queries/CLI; logs/telemetry; operating guide/retention;
benchmarks/demonstrations.

## 4. Qualify and release v1

- [ ] Run the matrix below through the same local/CI entry points. Keep one
  authoritative scenario per guarantee, supported by focused boundary tests.
- [ ] Verify an external consumer against the candidate tag without local replacements.
  Run its quickstart and process recovery scenario.
- [ ] Finish Go API documentation and examples. Keep the README short and link to
  integration, development, guarantees, operations, and upgrades. Compile examples
  and exercise runnable documentation commands in CI.
- [ ] Publish supported Go/PostgreSQL/OS versions, deployment assumptions, payload
  limits, retention/deduplication rules, and measured verification durations.
- [ ] Review transitions, SQL constraints, transaction/history atomicity, lock order,
  time authority, handler panics, and shutdown. Fix correctness defects before adding
  convenience features.
- [ ] Run dependency/vulnerability checks and document defect reporting.
- [ ] Prepare a changelog, versioned notes, installation instructions, and compatibility
  policy. Publish a prerelease first. Tag v1 only when every gate passes. Publishing
  remains a separate release action.

### Release verification matrix

| Guarantee | Required evidence |
| --- | --- |
| Durable start/progression | Failure injection leaves no partial plan or premature next step; committed work survives reconnect/restart |
| Current ownership | Concurrent workers, heartbeat/recovery races, exact expiry, duplicate acknowledgement, and stale tokens |
| At-least-once effects | Kill before effect and after effect/before acknowledgement; demonstrate duplicate delivery and application deduplication |
| Retry/deadline policy | Boundary/budget tests, restart during wait, and hung handler with healthy heartbeats |
| Cancellation | Transaction-order races, cancellation of a live execution, and prevention of later forward work |
| Compensation | Reverse order, process crash during cleanup, and visible cleanup exhaustion |
| Safe upgrades | Handler coexistence, previous-schema fixtures, migration interruption, and backup restore |
| Runtime resilience | Scheduler absence/restart/overlap, database outage/reconnect, shutdown, handler panic, and non-cooperative handler |
| Easy integration | Tagged standalone consumer, typed result flow, idempotent starts, runnable docs, no package internals |
| Reliable verification | Missing dependency fails, required suites execute uncached, race checks pass, cleanup survives injected failures |

Unit tests own pure transitions and policy arithmetic. PostgreSQL tests own
constraints, locks, and transactions. A small process suite owns startup, shutdown,
failure windows, and public integration. Use deterministic fakes instead of live
external services. Coverage percentage cannot replace evidence for each guarantee.

## Keeping the plan accurate

Check off each slice only after its acceptance scenario passes. Update the
state-machine ADR, SQL constraints, tests, and user docs together when semantics
change. Introduce verification before the feature it must verify, then extend it
at each milestone.

The next action is milestone 3B after the safe-evolution PR merges.
Operations and release qualification remain.
