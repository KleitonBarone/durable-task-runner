# 0007. Versions, schema compatibility, and upgrades

Status: accepted for milestone 3A.

Definitions and handlers have explicit immutable versions and payload schema
identities. `Definition.WithVersion(version, inputSchema)` selects a workflow
version. `ActivityDefinition.WithVersion(version, inputSchema, outputSchema)`
selects a handler contract. Versions are application-owned opaque identifiers.
Changing incompatible code or payloads requires a new version; the engine cannot
infer semantic compatibility from Go types or JSON shapes.

Unversioned declarations and existing rows use version `1` and schema identity
`legacy`, preserving the milestone 1/2 contract. Stored Go type contracts continue
to participate in compatibility checks. Cleanup declarations store their versions
and schema identities too, before forward work is claimable.

Registries allow distinct versions of one name. Start keys are scoped by workflow
name and version. A worker advertises name, version, type contract, and input/output
schema identities. It can register old and new handlers together. Unsupported
work remains pending with a diagnostic, without consuming an attempt.

The stored plan drives progression and recovery. No definition code reruns for
an existing execution. History fixtures check recorded transition interpretation;
this is not replay of arbitrary Go code. Deploy new handlers before starting new
versions. Keep old handlers until their forward and cleanup work drains, including
retry waits. Removing a version early leaves pending work; it cannot reroute to a
different version. External effect keys still require application deduplication.

Migrations remain ordered, additive files. Applied SQL is immutable. The migration
catalog records SHA-256 checksums with CRLF normalized to LF. Known legacy catalog
rows acquire checksums on their first supported upgrade. An unknown migration,
changed recorded checksum, or non-prefix history is incompatible and fails.
One advisory transaction lock serializes migrations; cancellation rolls back all
pending DDL and catalog writes. A later invocation can retry the migration.

Opening an engine permits a known older schema so its owner can migrate it.
Execution operations require the current schema. Startup/operation checks reject
unknown/newer or tampered schemas before work is claimed. Use the migration
command before starting worker and scheduler loops. Prerelease binaries predating
these guards must be drained and stopped before this upgrade.

Take and rehearse restoring a database backup before upgrades. Additive schema
does not imply arbitrary binary rollback is safe. A binary must recognize every
applied migration and every required handler version. Keep both handler versions
when rolling application code back, or forward-repair with a compatible binary.
Do not remove migration rows or edit applied files to bypass a guard.

Restoring an older engine snapshot can redeliver effects performed after that
snapshot, including effects whose acknowledgement was lost. Application effect
deduplication must survive for the required business lifetime. Restore rehearsals
use isolated databases and local fake effects, never production services.
