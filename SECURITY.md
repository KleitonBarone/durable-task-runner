# Report a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/KleitonBarone/durable-task-runner/security/advisories/new)
for security-sensitive defects. Include the affected revision, deployment shape,
sanitized reproduction, and impact. Do not include database credentials, customer
payloads, start/effect keys, or lease tokens. Use public issues for ordinary defects.

The latest v1 release is the supported security target. Fixes are best effort;
there is no promised response SLA. Keep Go, PostgreSQL, and dependencies patched.
Run `go run ./cmd/dev vuln` before proposing a dependency update or release.

Handlers, workflow definitions, observers, and database operators are trusted
application code. The engine does not sandbox untrusted code. Database credentials
and ownership tokens grant control over work. Apply TLS, least privilege, network
restrictions, and access control as described in [operations](docs/operations.md).
Default diagnostics redact payloads and arbitrary text; explicit sensitive queries
and raw result APIs can reveal application data.

Duplicate external delivery after failure is part of the at-least-once model.
Report broken ownership fencing, partial state/history commits, incompatible
handler routing, or unexpected sensitive-data disclosure with a focused reproduction.
