# 0005. Durable cancellation

Status: accepted for milestone 2B.

Cancellation is an explicit request, independent of worker process shutdown.
`Engine.Cancel(ctx, id, actor, reason)` locks the workflow before all child rows,
using the same order as claims, acknowledgements, and deadline settlement. Actor
and reason are application-supplied diagnostic text, not authentication. The
application must authorize the caller before invoking the library.

The first request changes `running` to `cancel_requested`, records actor/reason
and database request time, and appends `workflow.cancel_requested`. In that same
transaction it cancels blocked, ready, retry-waiting, and running forward steps,
closes running attempts as `cancelled`, and expires active leases. Completed
steps and their results remain. No new forward claim or late acknowledgement
can succeed after the request commits.

The scheduler changes `cancel_requested` to terminal `cancelled`, records settled
time, and appends `workflow.cancelled`. Settlement never waits for a handler.
With no scheduler, the request stays visible and work is already fenced. A worker
observes lost ownership on its next heartbeat/control operation, cancels its
handler context, and applies its existing bounded shutdown grace period.

Repeated requests preserve the first actor, reason, and timestamp without adding
history. Requests against terminal executions return the existing execution and
do not change it. Missing executions return `ErrNotFound`. A response lost after
commit can be resolved by repeating the request or querying the workflow.

Final completion that commits first wins. If cancellation commits first, success
is fenced. An already passed overall deadline is settled under the workflow lock
before accepting a request, preserving the timeout reason. Expired ownership may
be cancelled without redelivery if the workflow has not already become terminal.
Cancellation, recovery, and completion cannot reopen terminal executions.

Cancellation guarantees no further committed forward progress. It does not prove
that external work stopped or undo effects. Non-cooperative handlers can keep
running outside engine ownership. Application deduplication and reconciliation
remain necessary. Compensation will handle only known completed steps under its
own policy; repeated cancellation will not stop required cleanup.

```text
workflow: running -> cancel_requested -> cancelled
activity: blocked | ready | retry_wait | running -> cancelled
attempt:  running -> cancelled
lease:    active -> expired
```

The existing completed, failed, and timed-out states remain terminal. Cancellation
metadata is queryable separately from terminal reason and handler error text.
