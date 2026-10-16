# Development

The repository currently implements milestones 0 and 1. Read
[ROADMAP.md](ROADMAP.md) before starting a feature. Milestone 1A is the next gate:
make local verification reproducible and add real process crash coverage before
extending the execution model.

This page describes commands available today. The proposed `cmd/dev` commands in
the roadmap do not exist yet.

## Requirements

- Go 1.24 or later.
- Docker with Compose for PostgreSQL integration tests and the example.
- Make and a POSIX shell if using the Make targets. Make is optional.

Start the local PostgreSQL 17 dependency:

```sh
docker compose up -d --wait
```

It publishes port 54329 and uses a named development volume. If that port is busy,
inspect the conflict before starting another instance. Tests should use a disposable
local database, never a live application database. The test role needs permission
to create and drop schemas.

If Docker lives in WSL rather than Windows, run Compose from the repository's WSL
directory. For example:

```powershell
wsl -d Ubuntu --cd /mnt/c/Users/admin/repos/durable-task-runner --exec docker compose up -d --wait
```

Run the Go commands below from PowerShell. Replace the WSL distribution and
repository path to match your machine.

## Fast loop

Unit tests do not need PostgreSQL:

```sh
go test ./...
go test -run TestStateTransitions -count=1 .
```

Before handing off code changes, format and analyze both build configurations:

```sh
gofmt -w .
go vet ./...
go vet -tags=integration ./...
```

`gofmt -l .` should print nothing afterward. `make fmt-check` checks formatting
without changing files.

## Database checks

In a POSIX shell:

```sh
export DURABLE_TEST_DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go test -count=1 -tags=integration ./...
go test -count=20 -tags=integration -run 'TestConcurrentClaims|TestWorkerCrash' .
```

In PowerShell:

```powershell
$env:DURABLE_TEST_DATABASE_URL = 'postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go test -count=1 -tags=integration ./...
go test -count=20 -tags=integration -run 'TestConcurrentClaims|TestWorkerCrash' .
```

The integration helper currently skips database tests if the URL is missing. Set
it explicitly; a green tagged run without it does not prove persistence works.
Use `-v` to see individual test outcomes when diagnosing an unexpected skip.

Each test creates and drops a unique schema. Tests use the real database for
locking and transactions, with a manual clock for exact lease boundaries.
They do not use third-party services.

## Full current check

With Make and a POSIX shell, after starting PostgreSQL:

```sh
make check
make example
```

`make check` checks formatting, runs vet for both build configurations, then runs
unit and database tests. It does not provision PostgreSQL or test separate processes.
It uses the Make `DATABASE_URL` setting for integration tests, defaulting to the
local Compose database.

Without Make, run the formatting check, both vet commands, unit tests, and the
uncached tagged tests above. Then run the example in PowerShell:

```powershell
$env:DATABASE_URL = $env:DURABLE_TEST_DATABASE_URL
go run ./cmd/example
```

In a POSIX shell, use `DATABASE_URL="$DURABLE_TEST_DATABASE_URL" go run ./cmd/example`.
The example should print nine events ending with `workflow.completed`.

For concurrency changes, on a toolchain with race-detector support and a C compiler:

```sh
go test -race -count=1 -tags=integration ./...
```

CI currently runs formatting, vet, unit tests, and PostgreSQL integration tests on
Linux. An automated race job, native Windows setup checks, and process end-to-end
tests are planned in milestone 1A; they are not part of today's CI gate.

## Failure diagnosis and cleanup

For database startup failures, inspect `docker compose ps` and
`docker compose logs postgres`. Add `-v -count=1` and a focused `-run` selector to
reproduce a failing test. Test clock advancement should drive expiry assertions;
do not add sleeps to make a race disappear.

Docker's `all predefined address pools have been fully subnetted` error means it
cannot allocate a Compose network. Report that environment failure before changing
daemon settings or removing shared networks; it is not a failed engine assertion.

Stop the local dependency with:

```sh
docker compose down
```

This preserves the development volume. Do not delete that volume as routine test
cleanup. The forthcoming isolated verification runner must own and clean up its
own resources without resetting this database.

## Changing behavior

Keep transition code, SQL constraints, history events, and their tests consistent.
Record execution-semantics changes in an ADR. Update the roadmap only when the
acceptance scenario passes, and keep the README focused on evaluating and trying
the project.
