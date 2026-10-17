# 0004. Retries, deadlines, and authoritative time

Status: accepted for milestone 2A.

All mutations of an execution first lock its workflow row, then its activities,
attempts, and leases. Batch scheduling locks workflow rows in ID order and skips
already locked workflows. Acknowledgements and heartbeats use the same order.
This serializes progression, scheduling, and later cancellation per workflow.

Production `SystemClock` uses PostgreSQL `clock_timestamp()` sampled after the
relevant locks. Candidate selection may use an earlier time, but the mutation
rechecks eligibility and ownership after locking. Test clocks remain injectable.
Host clock skew cannot extend a production lease. Equality with an expiry or
deadline means ownership has ended.

Every claim consumes one persisted attempt, including abandoned deliveries.
The default budget is three claims per activity. Handler-error retries default
off; `RetryPolicy.RetryErrors` opts in. `Permanent(err)` and handler panics never
retry. Abandoned ownership may redeliver without opting into handler-error
retries, within the same total budget. Budgets cannot exceed 1000 claims.

Policy is immutable in the stored plan. Exponential backoff doubles from the
configured initial interval, capped by the maximum. Optional bounded jitter is
deterministic from activity identity and attempt number. The chosen due time is
stored with the failed or abandoned attempt. Restart never recomputes it.
Zero initial backoff permits immediate redelivery and preserves the original
crash demonstration. `retry_wait` is explicit when the due time is in the future;
the scheduler promotes due steps to `ready`. No in-memory timer implements a
durable retry.

Each attempt defaults to a five-minute execution limit, and each workflow to a
24-hour overall deadline. Start and step options can choose other positive
limits. These finite defaults are a prerelease change from unlimited recovery
and heartbeat ownership. Existing rows receive these limits in additive
migrations. An already consumed claim is never erased by an upgrade.

Claim expiry and heartbeat extension are capped at both deadlines. An expired
workflow stops pending work and fences all active ownership, becoming
`timed_out`. An attempt timeout consumes its claim, records `timed_out`, and may
retry only under its persisted error-retry policy and remaining claim budget.
Otherwise the workflow becomes `timed_out`. Budget exhaustion and handler
failure retain distinct terminal reasons. Deadline settlement and history are
atomic. A late acknowledgement cannot turn a timed-out execution into success.

The worker cancels handler contexts on deadline or ownership loss. Contexts are
cooperative; an external effect can still finish late. Stable effect keys and
application deduplication remain necessary. Terminal executions never reopen;
reconciliation starts a new execution and may record its predecessor.

Cancellation and compensation are specified separately in milestones 2B/2C.

## Current transitions

```text
workflow: running -> completed | failed | timed_out
activity: blocked -> ready -> running -> completed | failed | timed_out
          running -> retry_wait -> ready
          running -> ready                 abandoned immediate redelivery
          blocked | ready | retry_wait -> timed_out
attempt:  running -> completed | failed | abandoned | timed_out
lease:    active -> released | expired
```

Terminal workflows cannot claim or acknowledge work. No transition resets the
attempt count. A retry closes its old attempt and lease before making a later
claim possible. At most one active lease per activity remains database-enforced.
