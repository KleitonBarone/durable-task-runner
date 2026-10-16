# 0003. Public integration contract

Status: accepted for milestone 1B.

The canonical Go module is `github.com/KleitonBarone/durable-task-runner`.
Verification builds a temporary, separate consumer module. Local checks replace
the module with the checkout; remote checks install an exact pushed revision into
a fresh module cache without a replacement.

`DefineActivity[Input, Output]` creates a shared declaration for `Step` and `Handle`.
Each step has a key unique within its workflow, independent of activity name.
The plan and its Go type identities are persisted before any work is claimable.
Conflicting declarations in one registry are rejected. Named Go type identities
are compatibility declarations, not schema validation or automatic versioning.
Changing a named type's fields requires application compatibility discipline until
milestone 3A adds explicit versions. JSON uses the standard Go encoding rules.

The original `NewActivity` and `Handle` API remains available for raw integrations.
Legacy plans receive positional keys `step-0`, `step-1`, and so on. Migration 002
backfills those keys without changing execution state. Typed and raw declarations
of the same activity name cannot be mixed in one registry.
