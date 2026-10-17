# Release qualification

The bounded v1 scope in [ROADMAP.md](../ROADMAP.md) is complete.
[v1.0.0](https://github.com/KleitonBarone/durable-task-runner/releases/tag/v1.0.0)
and [v1.0.0-rc.1](https://github.com/KleitonBarone/durable-task-runner/releases/tag/v1.0.0-rc.1)
both resolve to `91837eae7beeaab3c8067b9b5daf265a974bc911`. They are immutable
annotated tags; no production deployment accompanied publication.

## Publication evidence, 2026-10-17

- Operations PR #8 passed five CI checks on `0cc1cdf` and merged at `df898c2`.
- Release qualification PR #9 passed five CI checks on `fd34cf6` and merged at
  `91837ea`. [Merged-commit CI](https://github.com/KleitonBarone/durable-task-runner/actions/runs/39046326768)
  passed full Go 1.26.9/1.27 checks, race detection, native Windows, and security.
- The merged SHA passed local full verification in 92.7 s and race verification
  in 63.0 s, run concurrently on the Windows/WSL machine in the benchmark guide.
  Ordinary and tagged-test vulnerability scans both reported no vulnerabilities.
- Each tag installed into a new external consumer module with a fresh module
  cache and no replacement. Separate worker/scheduler processes recovered the
  abandoned owner's activity, recorded abandoned/completed attempts, deduplicated
  two deliveries into one account effect, and completed the successor/result flow.
- The operator CLI installed at each tag into isolated `GOBIN` and module caches
  and ran successfully. Its PostgreSQL commands are covered by CLI integration CI.
- Fresh candidate and final-tag checkouts ran the README's `up`, `example`, and
  `down` commands. Each printed nine events ending in `workflow.completed` and the
  typed sent-email result. The task-created development volumes were then removed.

Public installation and remote peeled-tag SHAs were checked after publication.
All roadmap gates are satisfied; the matrix below identifies the tests to retain.

## Evidence owned by each suite

Run `go run ./cmd/dev check`, `go run ./cmd/dev race`, and
`go run ./cmd/dev vuln`. The full check compiles GoDoc examples and runs unit,
PostgreSQL, real-process, standalone-consumer, and snapshot-restore scenarios.
The race command repeats the unit/PostgreSQL code under the detector. Keep the
following scenarios authoritative rather than adding another smoke-test wrapper.

| Guarantee | Authoritative scenario and boundaries |
| --- | --- |
| Atomic start and progression | `TestKeyReservationAndPurgeAreAtomic`, `TestProgressionRollsBackWithHistoryFailure`; injected history failure rolls back rows and ownership, then the same claim can complete |
| Current ownership | `TestConcurrentClaimsCreateOneAttemptAndLease`, `TestHeartbeatExtendsLeaseUntilItReallyExpires`, `TestOwnershipTimeIsSampledAfterLockWait`; exact expiry and stale tokens are fenced |
| At-least-once effects | `TestProcessCrashRecovery`; actual kills before/after effects, duplicate delivery, application deduplication, successor, stale acknowledgement |
| Retry and deadlines | `TestRetryDueTimeSurvivesRestartAndExhaustion`, `TestProcessRetryBackoffSurvivesSchedulerRestart`, `TestProcessHangingHandlerTimesOutWhileAnotherJobProgresses` |
| Cancellation | `TestCancellationRacesClaimAndCompletion`, `TestProcessCancellationSurvivesWorkerAndSchedulerRestart`; committed cancellation prevents later forward work |
| Compensation | `TestCompensationReverseOrderSurvivesEngineRestart`, `TestProcessCompensationRecoversAfterCleanupEffect`; reverse cleanup, lost acknowledgement, exhaustion, original cause |
| Safe upgrades | `TestProcessHandlerUpgradePreservesOldExecution`, `TestMigrateEveryPriorSchemaPreservesActiveOwnership`, `TestMigrationPreservesWaitingCancellationAndCleanup`, `TestConcurrentAndInterruptedMigrations`, recorded-history fixtures, `cmd/dev restore` |
| Runtime resilience | `TestWorkerReconnectsAfterTransientConnectionFailure`, `TestRuntimeReconnectBudgetUsesControlledBackoff`, `TestUnresponsiveHandlerCannotOverlapAnotherInvocation`, `TestHandlerFailuresRemainFailures`, process shutdown and scheduler restarts |
| Bounded operations | `TestInspectionPagesBoundFilterAndRedact`, operator CLI integration, `TestRetentionRacesKeyedStartsAndBoundedReads`, `TestBoundedLoadRecoversLostAcknowledgementsWithSchedulerOverlap` |
| Public integration | `cmd/dev consumer` installs a fresh external module and runs separate worker/scheduler recovery using public APIs; GoDoc examples compile in ordinary tests |
| Reliable verification | `TestExplicitIntegrationRequiresDatabase`, isolated lifecycle/cancellation tests, `TestProcessHarnessReportsChildFailure`, `TestRestoreFailureCleansBothOwnedDatabases` |

## Source review

The release review inspected the state transition tables and PostgreSQL constraints,
workflow-first lock order across claim/acknowledgement/heartbeat/recovery/cancel/
retention, PostgreSQL time after ownership locks, and transaction/history atomicity.
It also inspected retry budgets, independent cleanup deadlines, panic and Goexit
handling, lost acknowledgements, bounded reconnect/shutdown, migration checksums,
capability routing, and retained start keys. No new runtime defect was found.
The late-history rollback scenario above closes a verification gap.

The operating assumptions and limits are in [SUPPORT.md](../SUPPORT.md),
[operations](operations.md), and [upgrades](upgrades.md). Failure demonstrations
contain [captured timelines](demonstrations.md). [Benchmarks](benchmarks.md) report
the workload and hardware rather than a production capacity claim.

## Publish and verify immutable tags

After the release PR passes CI and merges, rerun the full check, race suite, and
vulnerability scans on the merged SHA. Create an annotated `v1.0.0-rc.1` tag on
that SHA and push only that tag. Run the external consumer with normal module
resolution and a fresh module cache:

```powershell
$env:DURABLE_CONSUMER_VERSION = 'v1.0.0-rc.1'
go run ./cmd/dev consumer
Remove-Item Env:DURABLE_CONSUMER_VERSION
```

The equivalent POSIX command is
`DURABLE_CONSUMER_VERSION=v1.0.0-rc.1 go run ./cmd/dev consumer`.
The command owns an isolated database and cleans its temporary consumer/cache.
Repeat with `v1.0.0` after publishing the final annotated tag and release notes.
Verify the documented CLI install at the tag with an isolated `GOBIN` too.

Never move a published tag. Candidate fixes go through another tested PR and
receive another candidate tag. Publication is complete only after the candidate
and final consumer recovery pass and the remote tag SHAs match the qualified SHA.
