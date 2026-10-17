# 0010. Delayed recovery preserves the first ownership limit

Status: accepted for post-v1 correctness fixes.

An expired lease and an attempt deadline represent different failures. A lease
expiring first means the worker lost ownership. That delivery is abandoned and
may redeliver within its persisted claim budget, even without handler-error
retries. A deadline expiring first means the attempt timed out and follows the
persisted error-retry policy. Equality is a timeout because deadlines cap leases.

Recovery compares the stored lease expiry with the stored attempt deadline after
locking ownership rows. Its execution time cannot change which limit ended
ownership first. In particular, scheduler downtime past the attempt deadline must
not make an earlier abandoned delivery terminal. This applies to forward and
compensation activities without rerunning their definitions.

The current phase deadline still takes precedence. An expired workflow deadline
stops forward progress; an expired compensation deadline stops cleanup. Recovery
does not extend either deadline or refund a consumed claim. Old tokens remain
fenced, and recovery state changes and history remain one transaction.

No schema or history format changes are needed. Manual-clock tests cover delayed
recovery after both limits, a replacement delivery, stale acknowledgement,
idempotent reconciliation, and successful completion of forward and cleanup work.
Existing tests retain equality, phase expiry, and timeout-retry coverage.
