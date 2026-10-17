# 0008. Patched dependencies and supported Go versions

Status: accepted before milestone 3B.

The release scan found reachable symbols in pgx 5.7.5, `golang.org/x/text` 0.24.0,
and the development Go 1.26.5 toolchain. The existing behavioral checks passed,
but that is not evidence that dependencies are free of known vulnerabilities.
Move the planned vulnerability gate ahead of further features.

Use pgx 5.9.2 and `x/text` 0.41.0 or patched successors. The pgx advisory concerns
simple-protocol sanitization; the runtime uses parameterized queries with the
default protocol. Keep the fixed driver rather than depending on that exposure
distinction. See [GO-2026-5004](https://pkg.go.dev/vuln/GO-2026-5004) and
[GO-2026-6629](https://pkg.go.dev/vuln/GO-2026-6629).

These dependencies require newer Go versions than the original Go 1.24 baseline.
Set the minimum to Go 1.26.9, the patched older supported release as of this
decision. Test Go 1.26 and 1.27 using their current patch releases. Keep production
builds on patched supported toolchains. See the
[Go downloads](https://go.dev/dl/) and
[TLS advisory](https://pkg.go.dev/vuln/GO-2026-6607).

This supersedes ADR 0001's minimum Go version. PostgreSQL 17, pgx, the single
module, repository shape, and execution semantics remain as chosen there. No
database migration is needed. This is a prerelease integration requirement change.

`go run ./cmd/dev vuln` runs pinned govulncheck 1.9.0 against ordinary packages and
tagged integration/process code, including test files. CI runs it as a required
security check. Its vulnerability database stays current, so a new advisory can
fail an otherwise unchanged commit. Review actual symbol reachability
and upgrade the affected dependency/toolchain. Do not suppress findings to publish.

Keep the race container and external consumer's Go requirement aligned with the
module. Do not upgrade the machine's global Go installation or Docker configuration
as part of this repository change.
