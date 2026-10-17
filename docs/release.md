# Release qualification

The bounded v1 scope is defined in [ROADMAP.md](../ROADMAP.md). Its implementation
is complete through milestone 3B, merged at `df898c2` after five green CI checks
on `0cc1cdf`. Candidate-tag and final-tag installation remain release gates.

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
