# Durable Task Runner

Durable Task Runner is a small working execution engine for linear, multi-step jobs. It persists workflow intent before workers can claim it, uses expiring PostgreSQL leases, and recovers activities left behind by dead workers.

Milestones 0 and 1 are implemented. Activity delivery is at least once. The engine does not provide exactly-once side effects.

## Current execution model

- One PostgreSQL database stores workflows, activities, attempts, leases, and append-only history.
- One scheduler recovers expired leases.
- Any number of identical worker processes can claim activities.
- A workflow definition expands typed Go input into a linear activity plan at start time.
- The first activity becomes ready in the same transaction that persists the workflow and full plan.
- Completing one activity makes the next ready in the same transaction.
- A handler error fails the workflow. Retries are milestone 2 work.

See [ADR 0001](docs/decisions/0001-runtime-database-and-repository.md) for the runtime and repository choices and [ADR 0002](docs/decisions/0002-state-machine-and-failure-model.md) for the state machine and crash windows.

## Run locally

Requirements are Go 1.24 or later, Docker with Compose, and Make. Start PostgreSQL with one command:

```sh
make deps-up
```

Run every check, including PostgreSQL integration tests:

```sh
make check
```

Run the deterministic two-step example:

```sh
make example
```

The example registers typed `welcomeInput`, persists `create_account` and `send_welcome_email`, runs both, and prints the ordered execution timeline.

Without Make, use these commands from PowerShell:

```powershell
docker compose up -d --wait
$env:DURABLE_TEST_DATABASE_URL = 'postgres://durable:durable@localhost:54329/durable?sslmode=disable'
go test -tags=integration ./...
$env:DATABASE_URL = $env:DURABLE_TEST_DATABASE_URL
go run ./cmd/example
```

Stop the dependency without deleting its volume:

```sh
make deps-down
```

## Register and start a typed workflow

```go
type ImportInput struct {
    ObjectKey string `json:"object_key"`
}

var importWorkflow = durable.Define("import_object", func(input ImportInput) ([]durable.ActivitySpec, error) {
    fetch, err := durable.NewActivity("fetch_object", input)
    if err != nil {
        return nil, err
    }
    parse, err := durable.NewActivity("parse_object", input)
    if err != nil {
        return nil, err
    }
    return []durable.ActivitySpec{fetch, parse}, nil
})

registry := durable.NewRegistry()
if err := durable.Register(registry, importWorkflow); err != nil {
    return err
}
execution, err := durable.Start(ctx, engine, importWorkflow, ImportInput{ObjectKey: "imports/42.json"})
```

`Start` accepts the input type attached to the definition. It serializes the input and planned activity inputs as JSON before opening the persistence transaction.

Workers register typed handlers with `durable.Handle`. `Worker.RunOnce` claims one activity and heartbeats until the handler returns. `Worker.Run` polls continuously. `Scheduler.Run` polls for expired leases; `Scheduler.RunOnce` is useful for deterministic operation and tests.

## Crash recovery and duplicate delivery

Suppose a worker performs an external side effect and crashes before `CompleteActivity` commits:

1. Its lease reaches `expires_at` because heartbeats stopped.
2. The scheduler marks the lease `expired` and its attempt `abandoned`.
3. The scheduler moves the activity from `running` to `ready`.
4. A replacement worker claims a new attempt and may repeat the side effect.
5. The old token can no longer heartbeat or complete the activity.

The integration test `TestWorkerCrashAfterSideEffectRecoversWithAtLeastOnceDelivery` performs the side effect twice, rejects the stale acknowledgement, and completes through the replacement attempt. Use an application-level idempotency key or deduplication record for real side effects until milestone 2 adds first-class keys.

## Timeline

`Engine.Timeline` returns history in database sequence order. A recovered activity produces events like these:

```text
workflow.started
activity.scheduled
activity.claimed
attempt.abandoned
activity.recovered
activity.claimed
activity.completed
workflow.completed
```

History rows are append-only. Current-state tables remain mutable so workers can claim efficiently. State changes and their history events share a transaction.

## Verification

```sh
gofmt -w .
go vet ./...
go vet -tags=integration ./...
go test ./...
DURABLE_TEST_DATABASE_URL='postgres://durable:durable@localhost:54329/durable?sslmode=disable' go test -tags=integration ./...
```

The integration suite uses a unique PostgreSQL schema per test and covers concurrent claims, heartbeat extension, lease expiration, a worker disappearing after its side effect, duplicate delivery, stale-token fencing, and recovery. Unit tests cover typed registration, typed handlers, the controllable clock, and invalid state transitions.

## Not implemented yet

Milestone 2 owns retry policies, bounded timeouts, cancellation, engine-managed idempotency keys, compensation, and retry-exhaustion reporting. Multiple queues, a UI, brokers, distributed lock services, child workflows, and workflow versioning are also out of scope.

## License

[MIT](LICENSE)
