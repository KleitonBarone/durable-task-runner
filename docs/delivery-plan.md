# Delivery plan through v1

This is the commit and push sequence for [ROADMAP.md](../ROADMAP.md). The roadmap
defines scope and acceptance criteria; this document defines how to deliver that
scope. Commits 00 through 06 implement the planning baseline and milestone 1A on
`build/dev-loop`, merged in [PR #1](https://github.com/KleitonBarone/durable-task-runner/pull/1)
at `e85ffb6` with user approval. Commits 07 through 15 implement milestone 1B on
`feat/integration-api`, merged in [PR #2](https://github.com/KleitonBarone/durable-task-runner/pull/2)
at `87dd370`. Milestone 2A is in [PR #3](https://github.com/KleitonBarone/durable-task-runner/pull/3).

## Planning baseline

- Base branch: `main`.
- Base local HEAD: `b49b246`, `feat: add durable execution with lease recovery`.
- Remote: `origin`, `https://github.com/KleitonBarone/durable-task-runner.git`.
- Pending documentation: `README.md`, `ROADMAP.md`, `CONTRIBUTING.md`,
  `docs/integration.md`, and this delivery plan.
- Implemented scope: milestones 0 and 1. The proposed development commands,
  process harness, and later runtime features are not implemented.

These facts describe the checkout before commit 00. Recheck Git state at each
checkpoint and keep unrelated changes out of commits.

## Commit, push, and review rules

1. Start each batch on the branch named below, based on current `origin/main` after
   its prerequisite batch has merged. Fetch and fast-forward local `main`; preserve
   any local work before changing branches. Never reset or force-push to catch up.
2. Implement one numbered slice. Include its focused tests, relevant migration,
   state-machine decision, and accurate API docs in that commit. Tests listed in
   later rows add broader process coverage; feature commits must already prove their
   own behavior. Every commit must build and pass the checks available at that point.
3. Review the diff, stage explicit paths, and use the proposed conventional commit
   subject. Treat subjects as descriptions of the finished change. Split a slice
   when needed; do not make empty commits to satisfy the numbering.
4. Push each passing commit. The first push uses `git push --set-upstream origin
   <branch>`; subsequent pushes use `git push origin <branch>`. Use the actual branch
   name, never a wildcard or a force option. Record the resulting commit SHA.
5. Before opening a draft PR, fetch and merge latest `origin/main` into the feature
   branch, resolve conflicts, rerun the relevant checks, and push the result. Open
   the draft after the first useful passing slice so subsequent pushes run PR CI.
   Current CI does not run on arbitrary branch pushes without a PR.
6. For every pushed checkpoint, inspect the checks on the pushed SHA. A rejected
   push, failed check, or stale check is unfinished work. Fix real failures with a
   new tested commit and push it. Do not count older green checks as current proof.
7. At batch completion, run its full acceptance gate and the roadmap's exit gate.
   Update completed checkboxes in the final batch commit, push, and confirm CI on
   that SHA. Report the branch, commit range, PR, test evidence, and remaining work.
8. Merging a PR requires the user's authorization under this repository's rules.
   Keep it draft until its batch gate passes. Do not advance the dependent batch
   from stale `main` or silently create a stack to bypass review. Resume from updated
   `main` after the merge. Tag publication is a separate release action below.

For the current delivery run, the user explicitly approved all remaining work,
commits, pushes, PR merges, and release publication through completion. Apply the
verification gates and continue between batches without asking again. This does
not authorize changes to production services or live application databases.

This is eight PR batches with 44 proposed implementation/documentation commits. Prefer
complete, reviewable behavior over a fixed commit count. Extra correctness fixes
get their own tested commits and pushes. Each batch produces one draft PR by
default; split it when its changes need independent review.

## Verification used at each checkpoint

| Gate | Required checks |
| --- | --- |
| D | Review current-behavior claims, resolve relative links, run `git diff --check`, and exercise changed runnable examples when applicable |
| B | Existing formatting check, both `go vet` configurations, unit tests, uncached PostgreSQL integration tests with an explicit URL, and the example when affected |
| C | `go run ./cmd/dev check`, covering formatting, vet, unit tests, uncached PostgreSQL tests, process scenarios, and the example |
| R | `go run ./cmd/dev race`, using Linux/C tooling or the Windows container path, plus targeted repeated concurrency/recovery tests |
| X | A separate consumer module using public APIs; use a local checkout during development, a pushed commit for remote installation, and a candidate tag during release |

`C` and `R` are implemented in batch 1A. Its earlier commits used `B` plus focused
tests for new tooling. Use `D` for documentation-only commits. At a batch boundary run all
applicable gates, including race checks for runtime/concurrency changes. CI must
report the required suites actually executed; skips cannot satisfy a database gate.

Keep test databases disposable and separate from the named development database.
Do not change shared Docker configuration, delete development volumes, or use live
services to get a test passing. Capture logs and clean only task-owned resources.

## Batch 0. Save the plan and newcomer documentation

Branch: `build/dev-loop`, shared with batch 1A. Prerequisite: inspect the existing
uncommitted docs. The planning commit starts the first implementation PR; it does
not need a separate merge before work on the verification loop.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 00 | `docs: plan durable runner v1 delivery` | Commit the pending README, roadmap, contributor guide, integration guide, and delivery plan together. Gate D; preserve the already demonstrated milestone 0/1 guarantees and clearly label future work. |

Push checkpoint: push `build/dev-loop`, open a draft PR against updated `main`, and
verify its checks. Continue batch 1A on that branch.

## Batch 1A. Establish the development loop

Branch: `build/dev-loop`. Prerequisite: batch 0 committed and pushed.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 01 | `build(dev): add cross-platform development commands` | Add `cmd/dev` dispatch, prerequisite diagnostics, focused unit-test commands, and native Docker/WSL selection. Test command arguments, failure reporting, and cancellation with fake command execution. Gate B. |
| 02 | `build(dev): isolate PostgreSQL verification runs` | Add dependency lifecycle and uncached integration execution, unique project names/ports, health deadlines, cleanup, and mandatory database configuration. Test successful/failed startup, interrupted cleanup, and missing configuration. Gate B plus real isolated runs. |
| 03 | `test(runtime): make worker and scheduler timing controllable` | Introduce a small waiting dependency and deterministic tests for run loops, heartbeat loss, handler outcomes, and shutdown. Preserve current semantics. Gate B and targeted race tests. |
| 04 | `test(e2e): prove recovery across worker crashes` | Add separate-process fixtures and a bounded harness. Kill workers before and after the fake effect, recover through the scheduler loop, reject stale claims, restart the scheduler, and inspect persisted state. Gate B plus new process scenarios. |
| 05 | `ci: share local verification and race checks` | Complete `check`, `e2e`, and `race`; route Make and CI through the shared commands. Add timeouts, failure artifacts, minimum/development Go checks, Linux race coverage, and Windows setup checks. Gates C and R. |
| 06 | `docs: document the verified development loop` | Publish working commands, measured cold/warm timings, focused reproduction, cleanup, and current CI guarantees. Complete only the proven 1A checkboxes. Gates D, C, and R; repeat claim/recovery scenarios 20 times. |

Push every numbered commit after its checks. Keep the PR draft until all 1A exit
criteria pass. Deliberately break the database configuration and a child process
once to verify the full command fails and still cleans up.

This gate passes locally and in CI. Twenty targeted repetitions passed, including
all three process scenarios. Linux Go 1.24/1.26 checks, the Linux race job, and native
Windows tooling checks passed on `d00254a`. Commit 06 updates the docs and roadmap;
the PR must also be green on its final SHA before the merge checkpoint.

PR #1 passed all four checks on `a08168f` and merged with authorization at `e85ffb6`.

## Batch 1B. Finish the public integration contract

Branch: `feat/integration-api`. Prerequisite: 1A merged and its gate reproducible.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 07 | `fix(module): align imports with the repository path` | Correct `go.mod` and internal/example imports; add a separate consumer fixture. Gates C and X. After pushing, resolve the canonical module at that exact commit from a clean consumer cache. |
| 08 | `feat(api): add typed activities and stable step keys` | Tie activity names to input/output types and use stable step identity in persisted plans. Validate duplicate keys and incompatible registrations. Include migration and API tests. Gate C. |
| 09 | `feat(api): expose typed step results and workflow outcomes` | Add earlier-result access, final output, attempt inspection, stable errors, and payload/encoding limits. Reject incomplete/future result reads. Gates C and X. |
| 10 | `fix(engine): distinguish failures from empty error messages` | Separate completion/failure outcomes from message text. Add the focused empty-message failure test and define/test handler-panic behavior. Gates C and R. |
| 11 | `feat(engine): deduplicate workflow starts` | Persist optional start keys, enforce concurrent uniqueness, reject conflicting inputs, and document ambiguous commits/key lifetime. Gate C with concurrent starts and reconnect cases. |
| 12 | `feat(worker): expose stable activity effect keys` | Supply execution/step/attempt metadata and stable effect identity; add a deterministic deduplicated effect fixture. Show duplicate delivery with one logical effect. Gates C and X. |
| 13 | `fix(worker): claim only supported activities` | Filter claims by registered activity capabilities and expose unsupported-work diagnostics. Verify heterogeneous workers cannot fail each other's jobs. Gates C and R. |
| 14 | `fix(runtime): keep healthy workers running after task failures` | Separate recorded task failures from fatal errors, bound transient database reconnect, validate defaults, expose health, and define graceful shutdown. Test failing/succeeding jobs together, lost ownership, and database outages. Gates C and R. |
| 15 | `docs: add a complete application integration example` | Provide application, worker, and scheduler entry points, result flow, deduplication, and verified lifecycle docs. Gates D, C, R, and X; mark 1B complete only after its consumer scenario passes. |

Push every passing commit. Verify the installed remote commit uses no local
replacement; the development fixture may use one. Do not advertise a tagged
installation yet. Candidate-tag verification belongs to batch 4.

Implemented checkpoints 07-14 are `4574b41`, `3115547`, `343b30b`, `14333ac`,
`4546ad2`, `45b03e8`, `fe47384`, and `98e4623`. Each was checked, committed, and
pushed. Checkpoint 15 supplies the shared runnable example and completes the
acceptance gate. The standalone consumer exercises actual migrate/start/status
entry points and separate worker/scheduler processes. Crash recovery also proves
two deliveries with one deduplicated effect, alongside the original duplicate
effect demonstration. Full checks, race checks, and pushed-module installation
are required on the final batch state before requesting its merge.

Final review added two focused correctness commits: `7d1184f` preserves reusable
plan slices during concurrent starts, and `999ef6d` preserves stable
error classification for JSON that PostgreSQL rejects. Both had failing-before
and passing-after regression evidence. The final full check and race gate passed.
Concurrent keyed starts, capability filtering, reconnect, and unresponsive-handler
checks also passed ten targeted repetitions each.

## Batch 2A. Bound retries, ownership, and execution time

Branch: `feat/retries-timeouts`. Prerequisite: 1B merged.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 16 | `docs(adr): define retry and timeout semantics` | Record failure classification, finite claim budget, handler-retry opt-in, backoff, deadlines, authoritative time, lock order, and terminal outcomes before code changes. Gate D. |
| 17 | `fix(engine): evaluate lease ownership with authoritative time` | Implement production time rules after relevant locks while preserving test control. Test boundary equality, skew, stale pre-lock timestamps, and heartbeat/recovery races. Gates C and R. |
| 18 | `feat(engine): persist retry policies and due times` | Store immutable policies, attempt accounting, backoff/due times, retry transitions, and history atomically. Test concurrent scheduling, restart, capped backoff, and permanent errors. Gates C and R. |
| 19 | `feat(engine): enforce attempt and workflow deadlines` | Bound heartbeats, fence timed-out attempts, stop forward work at deadline, and cancel handler contexts. Test late acknowledgements and non-cooperative handlers. Gates C and R. |
| 20 | `feat(engine): expose retry exhaustion and timeout outcomes` | Add actionable terminal reasons/attempt details and process scenarios for restart during backoff, repeated crashes, and a hung handler with healthy heartbeats. Gates C, R, and X. |
| 21 | `docs: explain retries deadlines and exhausted work` | Update examples, guarantees, and application recovery guidance. Publish actual scenario output and close the 2A exit gate. Gates D, C, and R. |

Push each verified commit. Review the persisted-state/history consistency and finite
budget evidence before considering the batch ready to merge.

Checkpoints 16-20 are `1efbe8d`, `1b37cf1`, `ea1a62b`, `f3a7e23`, and `9629b67`.
Full checks, race checks, and the standalone consumer passed. Process tests prove
durable backoff across scheduler/worker restarts, three crashed processes exhausting
their claim budget, and a hanging handler timing out while its worker completes
another job. Ten repetitions exposed an early host-timer cancellation race; the
worker now verifies expiry with database time before cancelling. Ten repetitions
then passed. Terminal reconciliation starts a linked new execution.

Checkpoint 21 is `6004884`; all four CI jobs passed and a fresh module cache
installed that exact pushed revision successfully. PR #3 merged at `0e294e2`.

## Batch 2B. Cancel executions safely

Branch: `feat/cancellation`. Prerequisite: 2A merged.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 22 | `docs(adr): define cancellation transitions and races` | Specify requested/settled states, transaction ordering, stopping/fencing rules, terminal requests, and interaction with deadlines and future cleanup. Gate D. |
| 23 | `feat(engine): persist and settle cancellation requests` | Add request/query APIs, state constraints, atomic claim prevention, idempotent requests, and consistent lock order. Cover cancel/claim/complete/expiry races. Gates C and R. |
| 24 | `feat(worker): honor durable cancellation requests` | Deliver cooperative cancellation through control checks/heartbeats, bound settlement, and reject late acknowledgements. Test retry waits and non-cooperative handlers. Gates C and R. |
| 25 | `test(e2e): verify cancellation across worker restarts` | Add live-process cancellation/restart examples, assert no later forward step runs, document guarantees, and complete 2B. Gates D, C, R, and X. |

Checkpoints 22 and 23 are `97037e2` and `2ab5bbd`, in
[PR #4](https://github.com/KleitonBarone/durable-task-runner/pull/4).
The worker already uses ownership loss to cancel handlers and bound their grace
period. Checkpoint 24 therefore needs focused tests rather than another control
mechanism; it ships with checkpoint 25's process scenario and documentation.
Ten repetitions passed for cancellation racing with claim/completion/recovery
and for cooperative/non-cooperative handlers. The process scenario cancels after
an effect, observes context cancellation, restarts both runtime processes, fences
the saved stale token, and proves no successor claim. A history-write failure
rolls back cancellation metadata, attempt closure, and ownership together.

Push each passing commit. Preserve transaction-order evidence in tests and ensure
cancellation never claims to stop an external effect that already happened.

## Batch 2C. Compensate explicitly

Branch: `feat/compensation`. Prerequisite: 2B merged.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 26 | `feat(engine): persist explicit compensation plans` | Add the compensation ADR, typed declarations, result/input references, and distinct cleanup policy/deadline/effect identity. Test validation and atomic persistence. Gate C. |
| 27 | `feat(engine): execute compensation with recoverable leases` | Stop forward work and claim reverse-order cleanup through attempts/leases/history. Test restart between steps, missing cleanup declarations, duplicate delivery, and cancellation during cleanup. Gates C and R. |
| 28 | `feat(engine): report compensation outcomes and exhaustion` | Preserve the original failure, expose cleanup success/failure, enforce independent cleanup deadlines/budgets, and settle cancellation. Test exhausted cleanup and reconciliation boundaries. Gates C and R. |
| 29 | `test(e2e): verify compensation after process failure` | Kill a cleanup worker after its effect, recover it, and demonstrate successful cleanup and injected cleanup failure. Document ambiguous forward effects and close 2C. Gates D, C, R, and X. |

Push every passing commit. Do not mark cleanup complete unless its persisted
outcome and original workflow failure both remain inspectable.

The compensation ADR is `8f0dd3c`; checkpoints 26-28 share `a4770fd` in
[PR #5](https://github.com/KleitonBarone/durable-task-runner/pull/5).
Persistence, routing, and terminal outcome handling ship together so a public
cleanup declaration works in the commit that introduces it. Full checks and race
checks pass. Focused tests cover reverse order, engine restart, typed forward
results, stale/duplicate acknowledgements, absent declarations, independent
deadlines, exhaustion, cancellation during cleanup, and atomic history failure.
Checkpoint 29 adds separate-process cleanup crash/restart and failure scenarios,
the compiled application declaration/handler, and documentation.

PR #4 passed all four checks on `2d817bf`, installed from a fresh external module
cache, and merged at `a2f20b7` before this batch started.

## Batch 3A. Keep running executions safe during upgrades

Branch: `feat/safe-evolution`. Prerequisite: 2C merged.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 30 | `feat(engine): persist workflow and handler versions` | Record the compatibility ADR, durable workflow/activity/cleanup versions, payload identity, and a backfill for existing executions. Test old data. Gate C. |
| 31 | `feat(worker): route work to compatible handler versions` | Claim matching versions, expose missing-version diagnostics, and demonstrate old/new handler coexistence and draining. Gates C, R, and X. |
| 32 | `test(history): preserve recorded execution semantics` | Add history/state fixtures for normal runs, recovery, retries, cancellation, and cleanup. Verify compatibility without rerunning arbitrary workflow code. Gate C. |
| 33 | `test(migrations): verify supported upgrade paths` | Add prior-schema fixtures, startup compatibility checks, and tests for concurrent/interrupted migration. Preserve applied migrations and document rollback constraints. Gates C and R. |
| 34 | `test(e2e): rehearse upgrades and backup restoration` | Upgrade active executions and restore an isolated snapshot, then finish pending work. Document possible redelivery after restore and complete 3A. Gates D, C, R, and X. |

Push each verified commit. The batch gate includes in-progress work, not just
migration of an empty database.

## Batch 3B. Make deployments diagnosable

Branch: `feat/operations`. Prerequisite: 3A merged.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 35 | `feat(cli): inspect executions and their attempts` | Add bounded cursor queries and an operator CLI for migration, status, attempts, timelines, and cancellation. Test filtering, pagination, and command failures. Gates C and X. |
| 36 | `feat(observability): add structured runtime diagnostics` | Add correlated logs and metrics/tracing hooks for work age, leases, retries, deadlines, and cleanup. Test redaction, health, and low-cardinality labels. Gate C. |
| 37 | `feat(observability): add an optional telemetry adapter` | Add the optional OpenTelemetry integration without requiring it in ordinary application setup. Test with an in-memory exporter and no external service. Gates C and X. |
| 38 | `feat(storage): retain terminal executions by policy` | Add opt-in bounded deletion and start-key deduplication retention. Test active-work protection, deletion/read races, and key lifetime. Gates C and R. |
| 39 | `perf(engine): add repeatable contention and recovery benchmarks` | Measure claims, recovery, timeline growth, and bounded load; record workload/hardware. Include constraint and stuck-work checks. Gates C and R plus the benchmark run. |
| 40 | `docs: publish operating and recovery procedures` | Cover health, connection budgets, deployment, credentials/TLS, retention, upgrades, restore, and runnable failure demonstrations. Validate the procedures and complete 3B. Gates D, C, R, and X. |

Push every passing commit. Keep default installation small; optional diagnostics
must not require another runtime service.

## Batch 4. Qualify the release

Branch: `release/v1`. Prerequisite: 3B merged and all earlier gates still passing.

| Commit | Proposed subject | Contents and evidence before push |
| --- | --- | --- |
| 41 | `test(release): qualify public integration and upgrades` | Automate the roadmap release matrix, external-consumer scenarios, example compilation, and runnable documentation checks. Verify supported Go/PostgreSQL/OS versions. Gates C, R, and X. |
| 42 | `ci: enforce release compatibility and dependency checks` | Add reproducible dependency/vulnerability checks and verify published support policy against CI. Review transaction atomicity, constraints, lock order, time, panics, and shutdown. Commit any resulting code fixes separately with focused tests. Gates C and R. |
| 43 | `docs: prepare v1 release notes and integration guides` | Finalize changelog, compatibility policy, installation procedure, API docs, guarantees, measured limits, and defect reporting. Keep README aimed at newcomers. Gates D, C, R, and X against the pushed commit. |

Push each passing commit and get the release PR's latest SHA green. All roadmap
implementation checkboxes need test evidence before the release PR is ready.

## Tag and publication checkpoints

These are planned release actions, not extra empty commits. Publishing requires
the user's release authorization and a merged, green release commit.

1. Record the exact merged commit SHA. Rerun C, R, and the complete release matrix
   against it, then create the annotated candidate tag `v1.0.0-rc.1` and push that
   explicit tag with `git push origin v1.0.0-rc.1`.
2. In a fresh external consumer, install the candidate with normal Go module
   resolution, without a replacement or warm local module cache. Run its integration
   and process recovery examples against isolated PostgreSQL. Verify docs and artifacts
   describe that candidate exactly. Record results against the tag and commit SHA.
3. Fix candidate defects with ordinary tested commits and pushes through a draft
   PR, then issue `rc.2` if needed. Never move or overwrite a published tag.
4. When qualification passes and release publication is authorized, create the
   annotated `v1.0.0` tag on the qualified commit and push only that tag using
   `git push origin v1.0.0`. Publish its release notes. No production deployment
   belongs to this plan.
5. Verify `go get`/the documented install path at `v1.0.0` from a clean consumer,
   check the quickstart and recovery scenario, and record the release URL/SHA.
   Check off release publication only after this verification succeeds.

## Progress record

For each numbered slice, record evidence in its draft PR description or execution
handoff using its ID. Keep roadmap completion checkboxes in the corresponding
code/documentation commits; record their resulting SHAs after committing.

```text
ID:
Commit SHA:
Branch and push confirmed:
Checks and acceptance scenario:
Draft PR:
Latest CI SHA and result:
Merged SHA, when authorized:
```

At the end of a batch, also record its measured verification duration and any
remaining limitations. If a checkpoint fails, leave it incomplete and resume at
that checkpoint. Never label v1 complete because all commits were pushed; the
roadmap's behavior and release gates determine completion.
