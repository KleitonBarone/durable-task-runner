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

Each workflow input, step input, and output is limited to 1 MiB of encoded UTF-8
JSON before persistence. NUL strings and numbers outside Go's float64 decoding
range are rejected to keep validation consistent with the supported jsonb path.
Encoding and decoding errors preserve their causes through Go error wrapping.
`ErrInvalidInput` and `ErrPayloadTooLarge` identify validation failures.

`activity.Result(ctx, key)` reads only a completed earlier step in the handler's
execution. It checks persisted activity/type identity. It cannot read itself or
a future step. `activity.WorkflowResult(ctx, engine, id)` returns the last step's
typed output only after successful workflow completion. Raw status includes that
same output. Running and failed workflows have no successful final output.
Missing executions/steps, unavailable results, incompatible types, and lost
leases have stable errors usable with `errors.Is`. `Engine.Attempts` exposes
ordered attempts without requiring application SQL.

Success/failure is represented by the presence of an error, independent of its
message. An empty-message error still fails the attempt and workflow. A panic or
`runtime.Goexit` in the handler goroutine becomes `HandlerPanicError` and follows
the same durable failure path. This does not recover panics in application-created
goroutines or undo effects already performed. If acknowledgement fails or ownership
is lost, recovery may deliver the activity again.

Optional start keys are unique by definition name within one engine schema.
Insertion uses a database uniqueness constraint in the same transaction as the
full plan and initial history. Concurrent duplicate callers wait for that
transaction and read its committed execution. PostgreSQL jsonb equality defines
same input. Conflicting input returns `ErrStartConflict`; terminal executions are
never restarted by reusing their key. Keys last as long as their execution rows.
No retention is implemented. Callers resolve ambiguous commit errors by repeating
the same keyed request. Unkeyed starts intentionally create independent executions.

Handler context exposes workflow/step/activity identity and attempt metadata.
The effect key is `durable:<activity UUID>`, derived from the persisted logical
activity identity, independent of attempt number and ownership token. Application
deduplication must commit its key with its effect in the effect's own system.
The process fixture kills a worker after that commit and proves two deliveries
use one key and produce one effect. A separate fixture still demonstrates two
effects when the application does not deduplicate. Delivery remains at least once.

Built-in workers filter claim selection by the pair of persisted activity name
and Go type contract. Unsupported work stays ready without creating an attempt.
Each process may register a different handler set. A bounded diagnostic query
groups unsupported ready work from one worker's perspective. Low-level unfiltered
claiming remains available for custom runtimes, which must enforce their own
capability rules.

A successfully recorded handler failure returns `TaskError` from `RunOnce`.
`Run` continues to unrelated executions and treats cooperative lease loss as a
normal ownership outcome. Runtime connection/shutdown/transaction errors use five
bounded exponential reconnect waits, from 100 ms to 1600 ms, with a six-failure
limit. Successful operations reset that budget. Persistent SQL/configuration
errors return immediately. No handler retry policy is introduced here.

Each worker instance invokes at most one handler at a time. Shutdown and lease
loss cancel its context and wait a bounded grace period. An unresponsive handler
stops the loop and blocks reuse of that instance until it returns. Late results
are discarded; ownership is recovered through leases. Database operations and
connection establishment have time bounds. Worker and scheduler health describes
their local Run loops and exposes runtime errors without storing task failures
as infrastructure failures. Zero options select documented defaults; negative
or lease-incompatible durations are rejected.
