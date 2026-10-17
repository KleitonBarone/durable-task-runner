# Support and compatibility

Durable Task Runner's v1 scope is an installable Go library and operator CLI for
persisted linear plans. Your application owns handlers, worker processes, one
scheduler, PostgreSQL, credentials, supervision, and application effect deduplication.
There is no hosted control plane or production deployment in this repository.

## Verified environments

| Component | Verification and policy |
| --- | --- |
| Go | Minimum 1.26.9, full CI at that exact version, plus current Go 1.27 patches; use patched toolchains |
| PostgreSQL | Major 17; local and CI use `postgres:17-alpine`, measured as 17.11 for qualification |
| Linux amd64 | Full database/process/consumer/restore suite and race detector in CI |
| Windows amd64 | Native unit/vet/tooling CI; full checks and process recovery also verified locally through WSL Ubuntu Docker |
| Other environments | Not qualified for v1; no unsupported portability claim |

CI's security job scans ordinary packages and tagged integration/process tests
with pinned govulncheck and the current advisory database. Dependency changes must
pass the same check. Security updates can raise the required toolchain patch.
Keep PostgreSQL on its current maintenance patch too.

The MIT license applies. Maintenance is best effort with no response-time or
availability SLA. Report ordinary defects through
[GitHub issues](https://github.com/KleitonBarone/durable-task-runner/issues), with
the module revision, Go/PostgreSQL versions, sanitized reproduction, and timeline.
Follow [SECURITY.md](SECURITY.md) for sensitive reports.

## Compatibility promises

Versioned releases use semantic versioning for the exported Go integration API.
Patch releases fix defects; minor releases add compatible functionality. Callers
must tolerate additive JSON fields, history detail fields, and diagnostic metadata.
Use `errors.Is` or `errors.As` for documented errors rather than matching text.
CLI error wording and telemetry timing are diagnostic, not durable protocols.

Workflow definitions and handler payload identities remain application-owned
immutable contracts. New behavior requiring incompatible inputs, outputs, or
semantics needs a new version. Keep old forward and cleanup handlers until their
work drains. Changing a named Go type does not create a new durable identity.

Database migrations are forward-only immutable files with checksum guards. Back
up before migrating and follow [upgrades](docs/upgrades.md). A binary rejects a
catalog containing unknown migrations even if the release's Go API is compatible.
Rollback therefore requires a binary that understands the installed schema and
all pending handler versions; otherwise forward repair. Prior repository schemas
are covered by migration fixtures. The v1.0.0 catalog is the first release baseline.

History records committed transitions until explicit retention. Existing event
meanings and stored plans must remain interpretable across supported upgrades.
New event types are additive; consumers should preserve unknown events and update
their projector when adopting a release that adds transitions. Cursor strings are
opaque, query-bound, and intended for a live scan rather than durable bookmarks.
Pages are not a cross-request transaction snapshot.

## Execution and storage limits

- Plans are static and linear. No dynamic branching, parallel graph, child workflow,
  signal, recurring schedule, multiple queue, UI, or cross-language SDK is in v1.
- Delivery is at least once. Stable effect keys need application deduplication.
  Cancellation is cooperative; cleanup covers only committed completed steps with
  declarations. Ambiguous unacknowledged effects require application reconciliation.
- Inputs and outputs are limited to 1 MiB encoded JSON each. PostgreSQL JSON and
  UTF-8 rules apply. Error text is normalized and bounded to 16 KiB.
- Default budgets are three total claims, five minutes per attempt, and 24 hours
  for forward execution. Handler-error retries are opt-in. Cleanup has its own
  24-hour default deadline. Heartbeats cannot extend execution deadlines.
- Configured attempts are 1-1000. Attempt/phase timeouts accept one microsecond
  through 365 days. Retry delays cap at 24 hours. These are validation bounds,
  not practical deployment recommendations.
- Pages default to 50 rows, maximum 500. Purges default to 100 terminal workflows,
  maximum 500. Related-row cost depends on each plan and history size.
- Start keys are scoped by definition name/version and database/schema. They and
  original input remain reserved indefinitely, including after terminal purges.
  There is no v1 key-expiration API. Effect deduplication has its own business lifetime.
- PostgreSQL is the durable time authority. Database availability, synchronized
  host time, durable storage settings, and backups belong to the operator.

See [operations](docs/operations.md) for connection budgets, TLS, bounded reconnects,
shutdown, and retention. [Benchmarks](docs/benchmarks.md) publish local measurements
and hardware. Verification timings are in [CONTRIBUTING.md](CONTRIBUTING.md).
