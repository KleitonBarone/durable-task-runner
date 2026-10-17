# 0006. Explicit, recoverable compensation

Status: accepted for milestone 2C.

A forward step may declare `WithCompensation(cleanupStep)`. The typed cleanup
step supplies immutable input, handler identity, and its own retry/attempt policy.
The forward row stores that declaration before any work becomes claimable.
Cleanup may read committed forward results through the existing typed `Result`
API. No runtime callback rebuilds a cleanup plan after a restart.

On terminal forward failure, timeout, or cancellation settlement, the workflow
lock serializes stopping forward work and creating cleanup activities for known
completed steps, in reverse forward order. Steps without a declaration are
skipped. An activity without a committed successful result is not automatically
compensated, even if its external effect may have happened. Applications must
reconcile that ambiguity using their own effect keys and deduplication records.

Cleanup activities use the existing attempts, leases, claim filtering, retry
waits, fencing, and history. Each has a persisted logical identity and stable
effect key distinct from its forward activity. Cleanup is also at least once.
Only one cleanup step is ready at a time. Completing one makes the next ready
in the same transaction. A stopped workflow never resumes forward work.

The workflow becomes `compensating` and stores its original outcome/reason/error.
Cleanup has its own overall deadline, defaulting to 24 hours from activation,
independent of the expired forward deadline. `WithCompensationTimeout` can choose
a positive limit at start. Each cleanup step has the same finite default claim
budget and attempt limit as other activities; error retries remain opt-in.

Successful cleanup ends in the original `failed`, `timed_out`, or `cancelled`
state, with compensation status `completed`. Cleanup failure or exhaustion stops
remaining cleanup, fences active ownership, and ends in `compensation_failed`.
Its separate error/status preserves the original cause for reconciliation.
Cancellation settles only when required cleanup completes or fails. No terminal
execution reopens. A reconciliation run must be a new linked execution.

Repeated cancellation during cleanup does not cancel required cleanup or replace
the original trigger. An unresponsive old forward handler can still produce late
external effects, even after cleanup began. Engine fencing does not undo them.
Compensation is application behavior, never an automatic database rollback.

```text
running -> compensating -> failed | timed_out | compensation_failed
running -> cancel_requested -> compensating -> cancelled | compensation_failed
```

When no completed step declares cleanup, existing terminal behavior is preserved.
Forward outputs remain inspectable throughout cleanup. The workflow output is
still defined only by successful completion of its last forward step.
