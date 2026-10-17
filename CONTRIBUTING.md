# Development

[ROADMAP.md](ROADMAP.md) records implemented behavior and remaining acceptance gates.

## Start with one command

Install Go 1.26.9 or later and Docker with Compose, then run:

```sh
go run ./cmd/dev check
```

This checks formatting, runs static analysis for unit/integration/process code,
runs uncached unit and PostgreSQL tests, exercises real process recovery, and runs
the example and a standalone consumer with separate worker/scheduler processes.
It also dumps and restores a snapshot into a second isolated PostgreSQL instance
and finishes its unacknowledged activity with application deduplication.
It provisions its own PostgreSQL 17 container with an available
loopback port and temporary data, then removes that container on success or failure.
It does not use the named development database or a configured application URL.

Make is optional. Its targets are aliases for these Go commands.

Check prerequisites with:

```sh
go run ./cmd/dev doctor
```

The output shows Go, the Docker endpoint, daemon/Compose versions, and containers
publishing the development port, 54329. The first run needs network access to fetch
Go dependencies and Docker images.

## Fast loop

Unit tests do not need Docker. Pass ordinary Go test arguments for selection:

```sh
go run ./cmd/dev test
go run ./cmd/dev test -run TestStateTransitions .
go run ./cmd/dev fmt
go run ./cmd/dev lint
```

The non-mutating formatting check is `go run ./cmd/dev fmt-check`. Direct
`gofmt -w .`, `go vet ./...`, and `go test ./...` remain useful when debugging.

## Keep a local example database

```sh
go run ./cmd/dev up
go run ./cmd/dev example
go run ./cmd/dev down
```

`up` uses `compose.yaml` and the named `durable-task-runner` project on
`127.0.0.1:54329`. `down` preserves its named volume. The example prints nine events,
including `workflow.completed`, followed by its typed final result. Both activity
handlers are local fakes with application-owned effect-key deduplication.

`DATABASE_URL` overrides the example's local default. The full `check` command
always supplies its own temporary database. Do not reset development volumes as
routine test cleanup.

## Focus database tests

Explicit integration runs require `DURABLE_TEST_DATABASE_URL`. A missing URL or
unreachable database fails instead of skipping tests. Use a disposable local
database; the role must be able to create and drop schemas.

In PowerShell, after starting the development dependency:

```powershell
$env:DURABLE_TEST_DATABASE_URL = 'postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go run ./cmd/dev integration -run TestConcurrentClaims -count=20 .
```

In a POSIX shell:

```sh
export DURABLE_TEST_DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go run ./cmd/dev integration -run TestConcurrentClaims -count=20 .
```

The equivalent direct command is `go test -count=1 -tags=integration ./...`.
Each test owns a unique schema and drops only that schema.

## Process tests and race detection

```sh
go run ./cmd/dev e2e
go run ./cmd/dev e2e -run TestProcessCrashRecovery -count=3 .
go run ./cmd/dev e2e -run TestProcessHandlerUpgradePreservesOldExecution .
go run ./cmd/dev restore
go run ./cmd/dev race
```

`e2e` provisions an isolated database. A parent test starts worker and scheduler
processes, synchronizes on persisted barriers, and kills a worker before or after
its fake side effect. It verifies abandoned/completed attempts, duplicate delivery,
stale-token rejection, the next step, scheduler restart, and persistence through a
new connection. A third scenario checks graceful shutdown during an activity.
A fourth kills a worker after a deduplicated effect, verifies two deliveries share
one effect key, and observes one committed effect.

The harness also injects a failed child to verify failure reporting. Subprocesses
have deadlines and cleanup; their output is attached to a failed test. Precise
expiry/heartbeat boundaries belong to manual-clock tests, not timing sleeps.

`race` also creates an isolated database. Linux needs a C compiler. On Windows it
uses a disposable `golang:1.26.9-bookworm` container with read-only source and Go
module cache mounts, so a Windows C compiler is unnecessary. Host Go downloads and
verifies dependencies first. All test effects are local deterministic fakes.

History fixtures live in `testdata/history`. Their recorded events are interpreted
without invoking workflow code. To update them after reviewing an intentional
compatibility change, set `DURABLE_UPDATE_HISTORY_FIXTURES=1` and run
`go run ./cmd/dev e2e '-tags=integration' '-run=TestRecordedHistoriesMatchExecutionState' .`.
Inspect every fixture diff, then clear the variable. Ordinary checks never rewrite
fixtures. Timestamps use UTC so fixtures are independent of the host time zone.

For the milestone 1A stress gate, this combines selected database and process cases
in one isolated run. Quoting the complete flags works in both shells:

```sh
go run ./cmd/dev e2e '-tags=integration,e2e' '-run=TestConcurrentClaims|TestWorkerCrash|TestProcessCrashRecovery|TestWorkerLeaseLoss|TestSchedulerRun' -count=20 -timeout=5m .
```

## Windows and WSL

When Windows Docker is absent, commands use Docker in WSL Ubuntu. Set
`DURABLE_WSL_DISTRO` to select another distribution. Paths stay individual
arguments, including repository paths containing spaces.

Verification keeps a scoped foreground WSL process alive while native Windows Go
runs, then releases it after cleanup. This avoids WSL stopping Docker's systemd
service during a long native test run. For a persistent manual development session,
keep a WSL terminal open if your distribution otherwise stops idle services.

No command changes Docker contexts, daemon settings, or WSL configuration.
Test databases use the existing bridge network and temporary loopback ports.

## CI and failure diagnosis

CI runs the same `check` command on Linux with Go 1.26 and 1.27. It repeats process
recovery and runs a separate race job with repeated claim/recovery cases. Native
Windows checks formatting, static analysis, unit tests, and command/WSL selection.
The security job runs `go run ./cmd/dev vuln`, with pinned govulncheck and a current
advisory database. Run it after dependency changes and before release. Keep build
toolchains on current patch releases of the supported Go versions. See
[ADR 0008](docs/decisions/0008-supported-runtime-and-security-checks.md).

To retain local command output in ignored `artifacts/`:

```powershell
$env:DURABLE_TEST_ARTIFACT_DIR = 'artifacts'
go run ./cmd/dev check
```

For POSIX shells, prefix the command with `DURABLE_TEST_ARTIFACT_DIR=artifacts`.
CI uploads these logs when a job fails. Process failures include child output and
successful recovery scenarios print the ordered timeline.

For the persistent dependency, inspect `docker compose ps` and
`docker compose logs postgres` through your selected Docker installation. For test
failures, use a focused selector and inspect the assertion before changing timeouts.
Never point these checks at a live application database.

## Verification timings

On Windows amd64 with Go 1.26.5 and Docker through WSL, the warm unit loop took
2.5 seconds and a warm full check took 23.4 seconds. The first Linux Go 1.26 CI
check took 42 seconds after setup-go reported no dependency/build cache. The
targeted stress gate passed 20 repetitions in 149 seconds,
including all three process scenarios per repetition. These are observed timings,
not performance guarantees. Docker image and Go module downloads add to first-run
time. CI provides independent clean-run timing for both supported Go versions.

After milestone 1B, the same Windows full check took 39.2 seconds, including the
additional deduplication crash case and standalone consumer processes.

After milestone 3A, a warm Windows/WSL full check took 75.5 seconds, including the
separate consumer's abandoned-owner recovery and two-database snapshot rehearsal.
The Linux race suite's root-package tests took 15.0 seconds. These are local
verification timings, not runtime throughput measurements.

## Database measurements

For repeatable isolated database measurements, use `go run ./cmd/dev bench`.
See [benchmarks](docs/benchmarks.md) for workload selection, recorded hardware and
results, and the bounded recovery load test. Benchmarks are opt-in; correctness
tests run in the normal `check` and `race` paths.

## Verify integration from another module

```sh
go run ./cmd/dev consumer
```

This copies the example's Go source into a temporary consumer module, installs the
library, starts separate worker and scheduler processes, and checks keyed starts,
prior-step results, final output, attempts, and fake effects. Its SQL reads only
application-owned effects; all engine inspection uses public APIs. Local checks
replace the dependency with the checkout. To install a pushed commit with normal
module resolution and a fresh module cache, use PowerShell:

```powershell
$env:DURABLE_CONSUMER_VERSION = (git rev-parse HEAD)
go run ./cmd/dev consumer
Remove-Item Env:DURABLE_CONSUMER_VERSION
```

In a POSIX shell use
`DURABLE_CONSUMER_VERSION=$(git rev-parse HEAD) go run ./cmd/dev consumer`.
Push the commit first. No release tag is implied by this check.

## Changing behavior

Keep transition code, SQL constraints, history events, and focused tests consistent.
Record execution-semantics changes in an ADR. Complete roadmap checkboxes only
after their acceptance scenarios pass. Keep the README aimed at people evaluating
and trying the project.
