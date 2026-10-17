# Retry policy

Each claimed delivery consumes one attempt, even when its worker disappears.
The default budget is three attempts. Handler errors are terminal by default;
abandoned ownership may redeliver within that finite budget.

Opt into handler-error retries on a planned step:

```go
step, err := activity.Step("import", input, durable.WithRetry(durable.RetryPolicy{
    MaxAttempts: 5,
    RetryErrors: true,
    InitialInterval: time.Second,
    MaximumInterval: time.Minute,
    JitterFraction: 0.2,
}))
```

The plan stores this policy. Backoff doubles and caps at the maximum; optional
jitter is deterministic from activity identity and attempt number. The attempt's
retry decision and next eligible time commit with its outcome. The scheduler
promotes due `retry_wait` steps. Restarting it does not recalculate the due time.
Zero initial interval means immediate redelivery, still bounded by the budget.

`durable.Permanent(err)` stops retries and preserves its wrapped cause. Handler
panics also stop retries. A budget cannot exceed 1000 claims. Database reconnects
are runtime behavior and do not opt a handler into retries.

Inspect `Activity.AvailableAt`, `RetryPolicy`, and `LastError`, or each attempt's
`RetryDecision` and `RetryDueAt`. Terminal workflows include `TerminalReason` and
`LastError`; `attempt_budget_exhausted` distinguishes exhausted delivery from a
non-retryable handler failure. Terminal executions do not reopen.

This is a prerelease change from milestone 1's unlimited abandoned redelivery.
Migration 005 gives existing activities the same finite default. Keep handlers
idempotent: a failed or abandoned attempt may already have performed its effect.

## Execution limits

Each attempt defaults to five minutes; workflows default to 24 hours overall.
Use `durable.WithAttemptTimeout(30*time.Second)` on a step and
`durable.WithWorkflowTimeout(time.Hour)` on `Start` to choose positive limits.
The database stores absolute deadlines. Heartbeats cannot extend ownership beyond
either one. Production ownership decisions use PostgreSQL time sampled under
workflow/ownership locks; injected clocks control exact boundaries in tests.

At equality with a deadline, completion is fenced. Timeout closes active attempts
as `timed_out`, expires leases, and prevents forward scheduling. An attempt timeout
can retry only under the opted-in error policy and remaining budget. An overall
deadline always stops forward work. Terminal status is `timed_out`, with a reason
distinguishing the workflow deadline, attempt deadline, or exhausted claim budget.
Queries expose workflow and attempt deadlines. Handler metadata includes its
attempt deadline.

Recovery classifies an expired delivery by the first limit reached. A lease that
expired before its attempt deadline remains abandonment even if the scheduler
returns after that deadline. If both timestamps are equal, the attempt timed out.
The current workflow or cleanup deadline still takes precedence and stops its
phase. Scheduler downtime does not reset deadlines or restore consumed attempts.

Workers cancel handler contexts on deadline, even when heartbeats are healthy.
Handlers must cooperate and bound their own external calls. A timed-out external
effect may still finish; engine fencing cannot undo it. Non-cooperative handlers
stop reuse of that worker instance after its shutdown grace period.

## Reconcile terminal work

Read the workflow's terminal reason and last error, then inspect attempts and
the timeline before deciding which effects need reconciliation. Do not assume a
failed or timed-out handler performed no effect. A new start can include
`durable.WithPredecessor(oldID)` to record the terminal execution it repairs.
It receives new activity/effect identities. Choose application business keys
when an effect must deduplicate across separate executions.

## Reproduce the process scenarios

These commands create isolated PostgreSQL and clean up their containers:

```sh
go run ./cmd/dev e2e -run TestProcessRetryBackoffSurvivesSchedulerRestart .
go run ./cmd/dev e2e -run TestProcessRepeatedCrashesExhaustClaimBudget .
go run ./cmd/dev e2e -run TestProcessHangingHandlerTimesOutWhileAnotherJobProgresses .
```

Observed output from these scenarios:

```text
scheduler/worker restart preserved due=2026-10-16 21:27:56.356324 -0300 -03; second attempt started=2026-10-16 21:27:56.371727 -0300 -03
three crashed worker processes exhausted the persisted claim budget
healthy heartbeats did not extend the hanging attempt; same worker completed another workflow
```

The retry timeline contains `activity.retry_scheduled`, then `activity.ready`,
then the replacement claim. A timed-out attempt records `attempt.timed_out` and
the execution ends with `workflow.timed_out`. Tests assert persisted outcomes;
their bounded polling waits are not the evidence of success.
