## v0.7 — Coarse-Grained Locking

### Decision
Use a single `ReentrantLock` to protect all queue state transitions.

### Why
It provides a simple correctness model for transitions spanning multiple
data structures.

### Trade-off
All state-changing operations are serialized through one lock.

This may limit concurrency under contention, but the performance impact
has not yet been measured.

### Deferred alternatives
- Fine-grained locking
- Concurrent data structures
- Lock striping

### Revisit when
Benchmarks show lock contention is a meaningful bottleneck.

## v0.8 — Force WAL on Every Durable Write

### Decision
Force each committed WAL record before reporting the operation successful.

### Why
This provides a simple durability contract for the first persistent version.

### Trade-off hypothesis
Per-operation forcing may increase latency and reduce throughput.

This has not yet been measured.

### Deferred alternatives
- Buffered WAL writes
- Group commit
- Periodic fsync
- Asynchronous durability

### Revisit when
Durability semantics are stable and benchmarks are available.

## v0.10.0 — CRC32C WAL Integrity

### Decision

Add CRC32C to every complete WAL frame.

### Gain

Recovery can detect structurally complete records whose payload bytes have
been corrupted.

### Cost

Each frame gains 4 bytes and requires checksum computation on write and
verification during recovery.

The performance impact has not yet been measured.

### Important distinction

Incomplete final frame:
recoverable torn-tail condition.

Complete frame with checksum mismatch:
corruption; fail recovery.

## v0.11.1 — Durable Delivery Leases

### Decision

Persist a `LEASE_STARTED` WAL record before exposing a delivery to a consumer.

A lease contains:

    messageId
    receiptHandle
    attempt
    leaseUntil

Active leases are reconstructed across queue restart.

### Benefit

The queue can preserve externally visible delivery ownership across restart.

Without durable lease creation:

    receive
        |
        v
    queue restart
        |
        v
    message becomes READY prematurely

With durable lease creation:

    receive
        |
        v
    persist lease
        |
        v
    queue restart
        |
        v
    restore IN_FLIGHT

This prevents restart itself from changing delivery semantics.

### Cost

`receive()` now includes durable storage work.

Every delivery attempt adds another WAL record.

This may:

- increase receive latency;
- increase WAL write volume;
- increase WAL growth rate;
- increase recovery work.

These effects have not yet been benchmarked.

### Alternative — Immediately Requeue After Restart

Gain:

- simple recovery;
- no need to persist delivery ownership.

Cost:

- restart becomes implicit lease cancellation;
- potential premature duplicate delivery;
- existing consumer ownership is lost.

Rejected.

### Alternative — Preserve Lease Time but Not Receipt Handle

Gain:

- prevents premature redelivery.

Cost:

- old consumer cannot ACK/NACK after queue restart;
- successfully processed messages may later be redelivered.

Rejected.

### Selected Trade-off

Preserve the complete lease.

This makes the semantic model stronger and more internally consistent at the
cost of additional durable writes on the receive path.

### Revisit When

Revisit this decision if measurements show that durable lease creation is a
meaningful throughput or latency bottleneck.

Any optimization must explicitly state whether it preserves the same durability
contract.