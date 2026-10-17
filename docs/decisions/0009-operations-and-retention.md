# 0009. Bounded inspection, diagnostics, and retention

Status: accepted for milestone 3B.

Add cursor pages for workflows, activities, attempts, leases, and history. Default
to 50 rows and cap requests at 500. Bind a cursor to its query scope, filters, and
redaction choice. Use stable increasing keys and capture an upper bound on the
first page. Later inserts above that bound belong to a new scan.

Pages are a bounded live view, not a database snapshot across requests. A concurrent
transaction can commit an older reserved sequence after a page has passed it.
Rows can change state or disappear under retention. Pages do not repeat keys.
Use a new scan when completeness across concurrent writes matters, or export a
database snapshot using PostgreSQL's own tools.

Ordinary inspection omits payloads, start/effect keys, handler error text, and
cancellation actor/reason. An explicit sensitive-data choice exposes diagnostic
text and history details. Lease ownership tokens never appear in inspection.
The application-facing existing result/raw queries retain their documented behavior.

The CLI runs migrations and bounded inspection/cancellation against a configured
database. Definitions and handler code remain in application binaries. No generic
runtime server or code loader is introduced.

Optional observers report committed transitions and runtime timing without payloads,
effect keys, lease tokens, or arbitrary error text. Logging can correlate IDs;
metric labels contain only finite event/outcome/phase values. Observers must be
concurrent, quick, and nonblocking. The runtime does not require a telemetry service.
An optional OpenTelemetry package adapts these hooks, with local in-memory tests.
Claim-ready age uses persisted eligibility time. When a blocked successor becomes
ready, set its eligibility time in that same transaction, excluding blocked time.
Retry due times remain the eligibility authority for redeliveries. Additive history
detail fields preserve earlier recorded transition interpretation.

Terminal retention is opt-in, bounded, and serialized with workflow mutations.
Active work is never deleted. Deletion intentionally removes activities, attempts,
leases, payloads, and history. Keyed starts keep an independent deduplication
record containing the original workflow identity and JSON input, preserving exact
PostgreSQL JSON equality without a lossy hash. The retained record contains input
and must be included in the application's data policy. Reusing a purged key cannot
start new work: matching input reports the original expired execution ID; different
input conflicts. Keys last until an explicit future key-expiration policy exists.

Benchmarks and a bounded local load scenario measure claims, contention, recovery,
and history pages. Record workload and hardware; make no fleet-scale claims from
these measurements. Retain one scheduler as the deployment model, while testing
brief overlap with the existing database lock/fencing rules.
