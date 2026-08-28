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

## v0.12.1 — Snapshot WAL Position: Segment ID + Offset

### Decision

Represent snapshot recovery position as:

    WalPosition(segmentId, offset)

The current single-file WAL uses:

    segmentId = 0

### Why

A bare byte offset works for a single WAL file but becomes ambiguous once the
WAL is segmented.

Using a segment ID preserves direct physical replay semantics while allowing
the WAL to evolve toward:

    segment-N.wal

without changing the snapshot-position contract.

### Gain

- Direct mapping to FileChannel seeking.
- Clear physical recovery location.
- Future WAL segmentation fits naturally.
- Avoids prematurely introducing logical sequence numbering.
- Snapshot recovery remains simple.

### Cost

- Position is coupled to physical storage layout.
- Segment lifecycle rules will eventually be required.
- Compaction may need position translation or segment-retention rules.
- Does not solve logical ordering across replicated nodes.

### Alternative — Bare Byte Offset

Simpler today:

    offset = 12480

Rejected because the value becomes ambiguous with multiple WAL files.

### Alternative — Logical Sequence Number

Example:

    sequence = 10948

Advantages:

- independent of physical WAL layout;
- potentially useful for replication and distributed ordering.

Costs:

- requires sequence allocation semantics;
- requires mapping sequence numbers back to physical storage;
- introduces machinery not needed for current local recovery.

Deferred until the architecture requires logical-log identity.

### Current Constraint

The implementation currently supports one WAL segment only.

Therefore:

    segmentId = 0

No segmentation behavior should be added merely because the abstraction
contains a segment identifier.

### Concurrency Trade-off

The snapshot state and WalPosition must be captured consistently.

Current choice:

    briefly hold the queue state lock
    capture state + durable WalPosition
    release lock
    write snapshot outside the lock

This may briefly pause queue mutations during snapshot capture.

The duration and contention impact have not yet been measured.

### Revisit When

Revisit when:

- snapshot copying becomes expensive;
- lock contention is measured;
- WAL segmentation is implemented;
- dedicated WAL writer/group commit is introduced;
- distributed replication requires logical log indexes.

## v0.12.5 — Segmented WAL

### Decision

Replace unbounded single-file WAL growth with a sequence of WAL segments.

The highest authoritative `.wal` segment is active.

All lower segments are sealed and immutable.

### Motivation

Snapshot-based recovery makes older WAL history logically redundant.

A single WAL file makes physical prefix reclamation difficult because removing
old bytes would require rewriting the remaining active log.

Segments allow future compaction to delete whole immutable files.

### Segment Size Policy

Segment size is a target rather than a hard limit.

A record that causes the target to be exceeded remains entirely in the current
segment.

Rotation occurs before the following append.

### Gain

- Sealed WAL history becomes immutable.
- Frames never cross physical files.
- Future history reclamation can delete whole segments.
- Snapshot WalPosition naturally maps to `(segmentId, offset)`.
- Active-tail crash repair remains localized to the newest segment.
- No separate manifest or ACTIVE flag is currently required.

### Cost

- More files must be discovered and validated.
- Segment continuity becomes an invariant.
- Recovery spans multiple files.
- Rotation introduces additional failure points.
- Atomic filesystem rename is part of correctness.
- Sealed and active segments require different torn-tail policies.

### Authority Trade-off

We derive segment state from filenames:

    highest *.wal = active
    lower *.wal   = sealed
    *.tmp         = non-authoritative

This avoids maintaining separate mutable lifecycle metadata.

The trade-off is tighter dependence on filesystem naming and directory
discovery.

### Recovery Trade-off

Active segment:

    torn tail -> truncate/recover

Sealed segment:

    torn tail -> fail

This is intentionally stricter for sealed history because the existence of a
newer segment proves the old segment should already have been complete before
rotation.

### Rotation Failure Policy

Before atomic promotion:

    old segment remains authoritative.

After atomic promotion:

    new segment is authoritative even if the current process fails to open or
    activate it.

In the latter case, the current WAL instance must stop writing and restart.

### Append Failure Policy

A failed append may leave a partial frame.

The current WAL instance is poisoned so another complete frame can never be
written behind the torn frame.

Restart repairs the active tail before writes resume.

### Deferred

Not included in this version:

- physical deletion of sealed segments;
- segment manifest;
- distributed replication;
- logical record sequence numbers;
- multiple concurrent WAL writers;
- dedicated writer thread;
- group commit.

These should only be introduced when the corresponding requirement appears.