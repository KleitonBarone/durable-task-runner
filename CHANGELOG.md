# Changelog

## Unreleased

- Preserve abandoned delivery recovery when the scheduler returns after an attempt
  deadline but ownership expired earlier. Forward and cleanup work retain their
  finite claim budgets and phase deadlines.
- Distinguish embedded fields in unnamed struct contracts for worker routing and
  typed results. Affected v1.0.0 plans must drain with their original binary before
  upgrading; see the [upgrade guide](docs/upgrades.md). Stored plans are preserved.

## v1.0.0

The first complete bounded release, qualified through a prerelease before final
publication. See [release qualification](docs/release.md) for verification and
[support](SUPPORT.md) for compatibility, environments, and limits.

- Installable Go module with typed inputs, handlers, earlier-step results, and
  workflow output. Static linear plans commit before any work becomes claimable.
- PostgreSQL workflow, activity, attempt, and expiring lease models; heartbeats,
  fenced acknowledgements, abandoned-worker recovery, and committed timelines.
- Finite claim budgets, opt-in handler retries, persisted backoff, attempt/phase
  deadlines, cooperative cancellation, and explicit reverse-order compensation.
- Immutable definition/handler/schema identities and compatible capability routing.
  Migration checksums, prior-schema fixtures, history compatibility, and real
  backup restoration cover upgrades with pending work.
- Stable start/effect keys for application deduplication. Terminal retention removes
  execution details while preserving keyed-start reservation and original input.
- Bounded redacted operator APIs and CLI; optional structured logs, metrics, and
  OpenTelemetry adapter with application-owned providers.
- Cross-platform development commands, isolated PostgreSQL checks, actual worker
  process kills, standalone consumer recovery, race checks, and vulnerability CI.

Delivery is at least once. External effects require application idempotency.
Cancellation does not kill external work, and compensation is declared cleanup,
not automatic rollback. There is one scheduler, worker processes, and one database.
Dynamic graphs, signals, child workflows, recurring schedules, multiple queues,
a UI, and hosted or cross-language infrastructure are outside v1.

The Go baseline is 1.26.9 after the prerelease dependency/security review. The
module uses PostgreSQL 17 and pgx 5.9.2. There are no earlier published v1 releases.
