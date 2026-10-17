# Execution states

This is the consolidated v1 state reference, including cancellation and
compensation. The [models](../model.go) define the stored states, and
[transition validators](../state.go) define the allowed transitions below.
[ADR 0002](decisions/0002-state-machine-and-failure-model.md) records the historical
milestone 1 baseline.

## Workflow states

| State | Meaning | Terminal |
| --- | --- | --- |
| `running` | Forward work is active, including waits for workers or retries | No |
| `cancel_requested` | Cancellation committed and forward work is fenced; scheduler settlement is pending | No |
| `compensating` | Declared cleanup is active under its independent deadline | No |
| `completed` | The last forward step succeeded; its output is the workflow result | Yes |
| `failed` | Forward work failed, with no required cleanup left | Yes |
| `timed_out` | An execution limit stopped forward work, with no required cleanup left | Yes |
| `cancelled` | Cancellation settled, with no required cleanup left | Yes |
| `compensation_failed` | Required cleanup failed or exhausted its budget or deadline | Yes |

| From | Allowed next states |
| --- | --- |
| `running` | `completed`, `failed`, `timed_out`, `cancel_requested`, `compensating` |
| `cancel_requested` | `cancelled`, `compensating` |
| `compensating` | `failed`, `timed_out`, `cancelled`, `compensation_failed` |

On forward failure, timeout, or cancellation settlement, declared cleanup for
completed steps runs in reverse order. The workflow enters `compensating` before
its original outcome becomes terminal. Successful cleanup preserves that original
`failed`, `timed_out`, or `cancelled` outcome. Inspect `OriginalState`,
`TerminalReason`, and `LastError` alongside `CompensationState` and
`CompensationError` to distinguish the forward cause from a cleanup failure.

Terminal workflows never reopen. Reconciliation starts a new execution and can
link it with `WithPredecessor(oldID)`. A committed cancellation prevents new
forward claims and late acknowledgements; it does not kill external work.

## Activity states

`blocked` waits for a preceding step. `ready` can be claimed by a compatible
worker. `running` has an attempt and lease. `retry_wait` waits for its persisted
due time and scheduler promotion. Forward and compensation activities share these
states and transitions.

| From | Allowed next states |
| --- | --- |
| `blocked` | `ready`, `timed_out`, `cancelled` |
| `ready` | `running`, `failed`, `timed_out`, `cancelled` |
| `running` | `ready`, `retry_wait`, `completed`, `failed`, `timed_out`, `cancelled` |
| `retry_wait` | `ready`, `timed_out`, `cancelled` |

`completed`, `failed`, `timed_out`, and `cancelled` are terminal activity states.
Redelivery closes the old attempt before returning the activity to `ready` or
`retry_wait`. Every claim consumes the persisted budget, including abandonment.
Handler-error retries require an explicit policy; abandoned work can redeliver
within the same finite budget. Attempt counts never reset.

## Attempts and leases

| Model | From | Allowed next states |
| --- | --- | --- |
| Attempt | `running` | `completed`, `failed`, `abandoned`, `timed_out`, `cancelled` |
| Lease | `active` | `released`, `expired` |

All destination states in this table are terminal. A replacement claim creates a
new attempt; recovery never revives the old one. PostgreSQL permits at most one
active lease per activity. Heartbeats can extend current ownership within execution deadlines;
they cannot renew expired ownership. At equality with a lease expiry or execution
deadline, acknowledgement is fenced. Old tokens cannot complete replacement work.

## Persistence and external effects

Start commits the full plan and initial history before any activity is claimable.
Completing a step and making its successor ready commit together. State changes
and their history events share a transaction. Heartbeats update lease timing
without adding an event on every beat. History is append-only during execution;
explicit [terminal retention](operations.md#retention-and-start-keys) can remove it.

Delivery is at least once. A handler can perform an external effect and disappear
before recording success, so its replacement can repeat the effect. Stable effect
keys and application deduplication remain necessary. Cancellation and compensation
cannot undo arbitrary external effects.

See [retries and deadlines](retries.md), [cancellation](cancellation.md), and
[compensation](compensation.md) for policy, timing, races, and recovery examples.
