# ADR 0023: Bound Retained-Message Admission

## Status

Accepted for v0.24.0.

## Context

The local engine previously admitted every publish until memory, filesystem,
or WAL failure became the effective limit. That made overload an accidental
storage failure and allowed one queue to consume an unbounded share of a node.

Rate limiting alone does not solve this problem: a low publish rate can still
exhaust storage when consumers are slower or unavailable. The admission
decision must therefore use retained queue state.

## Decision

Each local partition enforces three positive limits:

- maximum UTF-8 bytes in one message payload;
- maximum retained messages;
- maximum retained payload bytes.

Retained state includes READY, IN_FLIGHT, DELAYED, and DEAD_LETTER messages.
Only a successfully WAL-persisted ACK releases retained count and bytes.

Payload size is checked before locking because it depends only on immutable
request data. Aggregate limits are checked under the queue's mutation lock so
concurrent publishes cannot oversubscribe capacity. Rejection occurs before
message identity allocation and before WAL append.

Recovery derives counters from the authoritative recovered message states.
Configuration may be reduced below recovered usage; startup remains available
for draining, but new publishes are rejected until usage falls within limits.

The queue-node maps oversized payloads to HTTP 413 and exhausted retained
capacity to HTTP 429 using distinct Problem Detail types.

## Consequences

- rejection cannot leave a WAL record or visible message;
- queue capacity represents retained payload, not WAL file allocation;
- moving a message among non-terminal states does not change capacity;
- DLQ retention consumes capacity until a future explicit removal mechanism;
- limits are currently node-wide configuration applied to every local queue;
- byte accounting uses UTF-8 payload bytes and excludes object, frame, index,
  snapshot, and filesystem overhead;
- the queue protects itself from unbounded logical retention, but this is not
  a filesystem free-space reservation or tenant quota system.
