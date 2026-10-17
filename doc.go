// Package durable runs persisted linear workflows using PostgreSQL and
// application-owned Go workers and a scheduler.
//
// Declare typed steps with DefineActivity, build a deterministic plan with Define,
// and Register it before calling Start. Start commits the complete plan before
// any step becomes claimable. New uses an application-owned pgx pool; Open owns
// its pool. Apply Migrate explicitly before running workers.
//
// A Worker executes one handler at a time and heartbeats its expiring lease.
// A Scheduler recovers abandoned attempts and promotes persisted retry waits.
// Production ownership uses PostgreSQL time through SystemClock. ManualClock
// controls persisted time and runtime tickers in deterministic tests.
//
// Delivery is at least once. A handler can perform an external effect and crash
// before acknowledgement. Use Metadata's stable effect key with application-owned
// idempotency. Cancellation fences engine progress and requests cooperative handler
// shutdown. Compensation runs explicitly declared cleanup for completed steps;
// neither operation promises to undo arbitrary external effects.
//
// Keep workflow and handler name/version/schema identities immutable. Existing
// executions continue their stored plans. ListWorkflows and the other List methods
// provide bounded, redacted live pages. Raw result/query methods can expose payloads.
// PurgeTerminal removes terminal details while preserving keyed-start deduplication
// and its original input indefinitely.
//
// See the repository's integration and operations guides for a complete application,
// supported environments, deployment, and recovery procedures.
package durable
