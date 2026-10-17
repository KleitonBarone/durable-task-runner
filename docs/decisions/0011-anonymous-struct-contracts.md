# 0011. Distinguish embedded fields in anonymous struct contracts

Status: accepted for post-v1 correctness fixes.

The v1.0.0 type contract omitted whether a struct field was embedded.
`struct{ Payload }` and `struct{ Payload Payload }` therefore shared an identity,
despite JSON encoding the first flat and the second nested. A worker could claim
an incompatible input, or a typed result read could silently decode zero values.

All unnamed struct identities now use a `struct/v2{` marker and record each
field's embedding flag alongside its name, package, type, and quoted tag. The
format applies recursively through pointers, arrays, slices, maps, and unnamed
struct fields. Named types retain their existing opaque identity. Changing a
named type's fields still requires an application version/schema change.

Both embedding forms must use the new format. Updating only embedded fields
would let a new non-embedded handler claim an old embedded input. The old stored
contract contains insufficient information to disambiguate those shapes, so
automatic fallback or rewriting would be unsafe.

There is no SQL migration and no stored plan rewrite. Before upgrading affected
applications, pause new starts and drain old forward work, retries, and required
cleanup with the original binary and payload declarations. Preserve that binary
for typed reads of archived results. New workers leave old unnamed-struct
contracts unsupported without consuming attempts; typed reads return
`ErrTypeMismatch`. Low-level workflow inspection still returns stored JSON.
Named payload types, scalars, and their containers retain their v1 identities.

After draining, give affected activity and workflow definitions new versions
before starting new plans. Keep original binaries for any remaining old work;
changing schema labels alone does not repair an ambiguous Go contract. Deadlines
continue to apply during draining. The exported Go API remains unchanged.

Regression tests exercise distinct flat/nested JSON, recursive type identities,
result rejection, separate worker registries, forward and cleanup routing, and
draining frozen v1 contracts with their original handler registration. The tests
check that unsupported deliveries consume no attempts and never rewrite identity.
