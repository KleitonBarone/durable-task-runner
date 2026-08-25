# Durable Task Runner

Durable Task Runner will execute multi-step jobs without losing progress when workers restart or individual steps fail.

It is an exploration of durable execution at a deliberately small scale. Workflows will persist their state, retry failed steps, support cancellation, and resume after a process crash.

> Status: planning. There is no working release yet. See the [roadmap](ROADMAP.md) for the intended build order.

## What problem does it solve?

Long-running jobs rarely fit inside one request. They call external services, wait, retry, and sometimes need to undo earlier work. Keeping that logic in ad hoc queue handlers makes recovery difficult to reason about.

The first release will provide:

- Persisted workflow and step state
- Worker heartbeats and abandoned-work recovery
- Retries, timeouts, and cancellation
- Idempotent activity execution
- Compensation for completed steps
- An execution timeline for debugging

## Design position

This is not intended to compete with mature workflow platforms. The goal is to make the durability model small enough to study, test, and explain.

The first implementation will use one database as the source of truth. Distributed consensus, arbitrary code execution, and visual workflow authoring are out of scope.

## License

[MIT](LICENSE)
