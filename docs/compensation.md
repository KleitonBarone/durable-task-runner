# Declare cleanup explicitly

Attach a typed cleanup step to a forward step in its plan:

```go
cleanup, err := deleteAccount.Step("delete-account", accountInput{AccountID: input.AccountID},
    durable.WithRetry(durable.RetryPolicy{MaxAttempts: 3, RetryErrors: true}),
    durable.WithAttemptTimeout(30*time.Second))
if err != nil {
    return nil, err
}
account, err := createAccount.Step("account", accountInput{AccountID: input.AccountID},
    durable.WithCompensation(cleanup))
```

Register `deleteAccount.Handle(...)` with workers that run cleanup. Its input,
name/type contract, retry policy, and attempt limit are stored with the forward
plan before any claim. Runtime cleanup gets a generated key derived from its
forward activity UUID and a distinct activity/effect identity. A cleanup handler
can use the existing `createAccount.Result(ctx, "account")` API to read its
committed forward result. Nested compensation is rejected.

If a workflow fails, times out, or settles cancellation, the engine schedules
cleanup for completed steps in reverse order. It skips absent declarations.
Activities without committed success are excluded, even if their external effects
may have happened. Required cleanup survives process restarts and uses the same
lease fencing and durable retry mechanism as forward work.

Cleanup is at least once. Use its metadata effect key to deduplicate cleanup in
the external system. A crash after cleanup's effect and before acknowledgement
can repeat that cleanup. A handler that ignores cancellation may also produce a
late forward effect after cleanup has begun. The application must reconcile these
ambiguous effects; compensation does not promise automatic rollback.

## Outcomes and limits

The workflow remains `compensating` while cleanup runs. Its `OriginalState`,
`TerminalReason`, and `LastError` preserve the forward cause. `CompensationState`,
`CompensationError`, and `CompensationDeadlineAt` expose cleanup progress.

| Cleanup outcome | Workflow state | Compensation state |
| --- | --- | --- |
| All required cleanup succeeds | Original `failed`, `timed_out`, or `cancelled` | `completed` |
| Cleanup fails or exhausts its budget/deadline | `compensation_failed` | `failed` |
| No completed step declares cleanup | Original terminal state | `none` |

Each cleanup defaults to three total claims and a five-minute attempt limit.
Handler-error retries remain opt-in. The overall cleanup deadline defaults to
24 hours from activation. Set `durable.WithCompensationTimeout(duration)` on
`Start` to change it. An expired forward deadline cannot disable cleanup.

Cancellation settles after required cleanup finishes or fails. Repeated requests
during cleanup preserve the original actor/reason and leave required cleanup
running. Cleanup failure stops later cleanup. Inspect the original cause and
cleanup attempts before starting a linked reconciliation execution with new
business intent and a new start key. Terminal executions never reopen.

See [ADR 0006](decisions/0006-compensation.md) for transaction and failure rules.
