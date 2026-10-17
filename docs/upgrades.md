# Upgrade an application

Workflow plans, retry policies, cleanup declarations, and payload identities are
stored before work becomes claimable. An existing execution continues this stored
plan after a restart. Definition code runs only when starting an execution.

## Unreleased anonymous struct correction

The post-v1.0.0 fix changes contracts for unnamed struct activity inputs or
outputs, including those inside pointers, arrays, slices, or maps. v1.0.0 omitted
field embedding from these identities: `struct{ Payload }` and
`struct{ Payload Payload }` could match despite incompatible JSON shapes.
Named payload types and their containers keep their existing identities.

Before upgrading an affected application, pause new starts and drain its old
forward work, retry waits, and required cleanup using the original binary and
payload declarations. Keep that binary for typed reads of old results. Then
deploy the fixed library with new affected activity and workflow versions and
resume starts. The fix requires no database migration.

New workers leave ambiguous v1 unnamed-struct contracts pending as unsupported;
they consume no attempts. New typed result reads reject those contracts with
`ErrTypeMismatch`. `Engine.Workflow` still exposes the stored output JSON for
application inspection. Deadlines continue during draining. Do not rewrite rows
or advertise the old contract from a new handler: its original shape cannot be
inferred safely. See [ADR 0011](decisions/0011-anonymous-struct-contracts.md).

## Declare immutable identities

```go
createV1 := durable.DefineActivity[InputV1, OutputV1]("create").
    WithVersion("1", "create-input/1", "create-output/1")
createV2 := durable.DefineActivity[InputV2, OutputV2]("create").
    WithVersion("2", "create-input/2", "create-output/2")
definitionV2 := durable.Define("provision", planV2).
    WithVersion("2", "provision-input/2")
```

Versions and schema identities are application-owned strings, not inferred JSON
schemas. A changed named Go type can keep the same Go identity, so declare a new
version when its payload or behavior becomes incompatible. Keep a name/version
pair immutable. The engine checks name, version, Go contract, and both payload
identities before a built-in worker claims an activity. Typed result reads check
the same identities. Cleanup declarations carry their own identities too.

Existing unversioned declarations and old database rows use version `1` and schema
`legacy`. Workflow start keys are scoped by workflow name and version. A key in
version 2 can start a distinct execution from the same key in version 1.

## Deploy and drain

1. Back up the database and rehearse restoring it into an isolated database.
2. Stop prerelease binaries from before schema guards were introduced. They cannot
   detect the new routing rules. Later guarded binaries reject unknown migrations.
3. Run `Engine.Migrate` once before runtime startup, with a bounded context.
   Concurrent migration calls serialize through a PostgreSQL advisory transaction
   lock. Cancellation rolls back pending DDL and catalog writes together.
4. Deploy workers with both `createV1.Handle(oldHandler)` and
   `createV2.Handle(newHandler)`. They can share a worker instance or use separate
   processes. Deploy compatible cleanup handlers at the same time.
5. Start new executions using the new workflow definition version.
6. Keep old handlers until old forward work, retry waits, and required cleanup have
   drained. `Worker.UnsupportedActivities` reports ready contracts a given worker
   cannot claim. It describes that worker, not fleet-wide availability.

Removing an old handler early leaves its work pending without consuming attempts.
Overall execution deadlines still apply. Reintroduce the matching handler to drain
it. Never rename persisted rows or reroute them to a new handler to unblock them.

## Schema and rollback rules

`Open` accepts a known older migration prefix so its owner can migrate it. Runtime
mutations and query APIs require the current schema. `CheckSchema` provides an
explicit readiness check. `New` does no I/O; its first operation performs the check.

Applied migration files are immutable. The catalog stores SHA-256 checksums with
CRLF normalized to LF. Unknown files, gaps, and changed checksums fail with
`ErrIncompatibleSchema`. An older catalog without checksums adopts the known file
checksums on its first upgrade. Historical edits before that adoption cannot be
detected. Checksums prevent accidental divergence, not malicious database changes.

Schema changes are additive where possible, but an old binary still needs to know
every applied migration and every handler version it will execute. Roll back only
to a compatible binary that retains the required handlers. Otherwise forward-repair
the deployment. Do not edit migration rows or applied SQL to bypass a guard.

A restored older snapshot can redeliver effects performed after its backup,
including effects whose acknowledgement was lost. Keep application deduplication
records for the business lifetime, independently of engine snapshot recovery.

The PostgreSQL suite upgrades all earlier repository schemas and preserves active
leases, retry waits, cancellations, cleanup, and history. It also interrupts blocked
DDL and retries concurrent migration calls. The fixtures include prerelease
schemas; the v1.0.0 migration catalog remains the published baseline.

## Rehearse snapshot recovery

```sh
go run ./cmd/dev restore
```

This command creates two disposable PostgreSQL instances. A short-lived owner
claims a welcome step, commits its fake effect, and exits without acknowledgement.
The command uses `pg_dump` and `psql` to restore that snapshot into the other
instance, then starts separate worker and scheduler processes against the restore.
It verifies an abandoned attempt, a completed replacement, one account effect,
and a completed successor. Both databases are removed even if restoration fails.
`check` includes this rehearsal. It never selects the development database.

The process upgrade test additionally kills a v1 worker after its effect, deploys
an incompatible v2 worker, and proves each version finishes with its matching
handler. The replacement v1 worker has no old workflow definition registered.

Five recorded history fixtures preserve normal completion, abandonment, retries,
cancellation, and cleanup. Unit tests project their events into expected states;
integration tests compare current histories and persisted outcomes with those
fixtures. This tests history interpretation, not replay of arbitrary Go code.
