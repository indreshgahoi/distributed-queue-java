# ADR-0004: Use One Coarse-Grained ReentrantLock

## Status

Accepted

## Context

The queue state spans multiple structures:

- READY
- IN_FLIGHT
- DELAYED
- DEAD_LETTER

Operations such as receive(), ack(), nack(), lease expiry, and delayed promotion perform multi-step transitions across these structures.

Thread-safe collections alone do not make these transitions atomic.

## Decision

Use a single ReentrantLock to protect all queue state transitions.

## Rationale

The lock protects the queue state machine rather than any individual collection.

For example:

READY -> IN_FLIGHT

must be observed atomically by concurrent callers.

The current priority is correctness and simple reasoning.

## Alternatives Considered

### Concurrent collections only

Rejected because atomicity across multiple collections would still not be guaranteed.

### Fine-grained locking

Deferred because it increases complexity and has no demonstrated need yet.

### Lock striping

Deferred until measurement shows contention.

### Lock-free design

Rejected for the current phase because it would substantially increase correctness complexity.

## Consequences

Positive:

- easy-to-reason-about atomic state transitions
- protects invariants across multiple collections
- simple concurrency model

Negative:

- state-changing operations are serialized
- the lock may become a throughput bottleneck under contention

## Measurement Status

Lock contention has not yet been benchmarked.

Any performance limitation is currently a hypothesis rather than a measured result.

## Revisit When

Benchmarking shows lock contention is a meaningful bottleneck.