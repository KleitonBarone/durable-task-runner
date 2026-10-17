# ADR 0002: State machine and failure model

Status: accepted, 2026-08-27

For the consolidated v1 model, see [current execution states](../state-machine.md).

This document records the milestone 1 baseline. [ADR 0004](0004-retries-deadlines-and-time.md)
extends it with finite claim budgets, `retry_wait`, `timed_out`, and database time
sampled after workflow-first locks. Those rules supersede unlimited recovery and
terminal-handler-error behavior where a step opts into retries.

## Terms

A workflow execution is one durable run of a registered definition and typed input.

An activity is one planned unit of side-effecting work. Activities in milestones 0 and 1 run in a fixed linear order.

An attempt is one delivery of an activity to a worker. Recovery creates a new attempt. It does not revive the old one.

A lease is time-bounded ownership of one attempt. The lease token fences heartbeat and completion writes from stale workers.

## Workflow state machine

```text
running -> completed
running -> failed
```

`completed` and `failed` are terminal. A workflow starts as `running`. It becomes `completed` in the same transaction that completes its last activity. A handler error moves the current activity, attempt, and workflow to `failed` because retry policy belongs to milestone 2.

## Activity state machine

```text
blocked -> ready -> running -> completed
                       |  \
                       |   -> failed
                       -> ready
```

Only the first activity starts as `ready`; later activities start as `blocked`. Completing an activity moves the next one to `ready` in the same transaction. Claiming moves `ready` to `running`. Lease-expiry recovery moves `running` back to `ready` and leaves the activity's attempt count intact.

## Attempt and lease state machines

```text
attempt: running -> completed | failed | abandoned
lease:   active  -> released  | expired
```

An activity claim creates a fresh running attempt and active lease together. Successful or failed acknowledgement releases the lease. Recovery expires the lease and abandons its attempt before making the activity ready.

## Invariants

- At most one active lease exists for an activity. PostgreSQL enforces this with a partial unique index.
- A worker may heartbeat or finish only through its opaque lease token.
- Heartbeat and finish reject a lease at or after `expires_at`, even before the scheduler records expiry.
- Each claim increments the activity attempt number. Attempt numbers are unique within an activity.
- Workflow intent and the initial claimable activity commit atomically.
- Completion, release, scheduling of the next activity, and their history records commit atomically.
- History is append-only. Mutable state tables are current projections used for safe claims and queries.

Heartbeats update the lease's `heartbeat_at` and `expires_at` fields. They do not append history rows because a healthy long-running activity may produce thousands of heartbeats. Claim, recovery, completion, and failure remain in the timeline.

## Failure model

Activity delivery is at least once. The engine does not provide exactly-once side effects.

If a worker disappears before it performs the side effect, the lease expires and another worker runs the activity.

If a worker performs the side effect and disappears before acknowledgement, the lease still expires and another worker runs the activity. The side effect can happen twice. Activity handlers therefore need an idempotency or deduplication strategy supplied by the application. Milestone 2 will add engine-level idempotency keys.

If the old worker returns after lease expiry, its heartbeat and completion fail with `ErrLeaseLost`. The replacement attempt owns the only valid token.

A heartbeat extends expiry but cannot renew an already expired lease. Worker process shutdown does not alter database state; the scheduler recovers it after expiry.

The scheduler is expected to run as one process. Recovery uses row locks and remains safe if two scheduler loops briefly overlap during deployment.
