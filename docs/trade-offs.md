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