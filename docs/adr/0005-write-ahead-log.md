# ADR-0005: Use an Append-Only Write-Ahead Log

## Status

Accepted

## Context

Until v0.7, all queue state exists only in memory.

A successful publish followed by a JVM crash causes the message to disappear.

The queue now needs a durability model that allows state to be reconstructed after process restart.

## Decision

Introduce an append-only Write-Ahead Log (WAL).

Durable state transitions are recorded in the WAL before the corresponding in-memory transition is reported as successful.

Recovery replays WAL records to reconstruct queue state.

## Initial Durability Contract

For the first durable implementation:

A publish operation is considered successful only after its WAL record has been written according to the configured durability policy.

The initial policy will force WAL records before returning success.

## Write Ordering

For durable publish:

1. Create message.
2. Append PUBLISH record to WAL.
3. Force the WAL record according to the durability policy.
4. Add message to in-memory READY state.
5. Return success.

This ordering prevents a successful publish from existing only in volatile memory.

## Recovery Model

On startup:

1. Open WAL.
2. Read records in order.
3. Replay each valid record.
4. Reconstruct the in-memory queue state.

The in-memory structures are therefore treated as a projection of durable history.

## Alternatives Considered

### Persist after updating memory

Rejected because a crash between the in-memory mutation and WAL write could lose an operation that had already become visible.

### Use PostgreSQL or another database

Deferred because the project intends to understand durability mechanisms directly rather than delegate them to an external database.

### Snapshot-only persistence

Rejected because snapshots alone would require frequent full-state writes and do not naturally capture incremental transitions.

### Event streaming system

Rejected because introducing an external broker would hide the durability mechanics this project is intended to explore.

## Consequences

Positive:

- successfully persisted operations can survive process restart
- recovery logic is explicit and testable
- storage semantics remain visible in the project
- future snapshots and compaction can build on the WAL

Negative:

- append and force operations may add latency
- WAL size grows without compaction
- recovery time grows with WAL length
- corruption and partial-record handling must be defined

## Performance Status

Forcing every WAL record is expected to have a performance cost, but this project has not yet measured that cost.

This is currently a hypothesis.

Future benchmarks should compare:

- buffered append
- force per operation
- group commit
- periodic force

## Non-Goals

This ADR does not yet define:

- replication
- multi-process WAL access
- WAL compaction
- snapshots
- cross-machine durability
- filesystem corruption recovery
- group commit

## Revisit When

Durability semantics are stable and benchmark data is available.