# ADR-0010: Represent Snapshot WAL Position as Segment ID and Byte Offset

## Status

Accepted

## Context

Snapshot recovery needs to identify the exact WAL position represented by a snapshot.

The current WAL implementation uses a single append-only local file.

A bare byte offset would be sufficient for the current implementation:

    offset = 12480

However, a byte offset is meaningful only within a specific physical WAL file.

The WAL is expected to evolve toward segmentation:

    wal/
      segment-000000.wal
      segment-000001.wal
      segment-000002.wal

In that model, the following is ambiguous:

    offset = 12480

because the same offset may exist in multiple WAL segments.

The snapshot therefore needs a recovery position that identifies both:

1. the WAL segment;
2. the byte position within that segment.

## Decision

Represent WAL recovery positions using:

    WalPosition(
        long segmentId,
        long offset
    )

Example:

    WalPosition(
        segmentId = 7,
        offset = 12480
    )

The current implementation uses:

    segmentId = 0

because only one WAL segment exists today.

The `offset` identifies the first byte NOT represented by the snapshot.

Therefore, if a snapshot contains all state produced through byte 12479:

    snapshot.walPosition()
        = WalPosition(0, 12480)

Recovery starts reading at exactly that position.

## Position Semantics

A valid `WalPosition` must point to a WAL frame boundary.

It must never point inside:

    [length][payload][checksum]

Correct:

    [frame 1][frame 2]|
                       ^
                    position

Incorrect:

    [length][payload....|....][checksum]
                       ^
                    position

The position represents a physical recovery boundary.

## Snapshot Consistency Invariant

A snapshot contains:

    SnapshotState S
    WalPosition P

The following invariant must hold:

> S represents exactly the effects of WAL history before P.

For example:

    WAL:

    R1 PUBLISH M1
    R2 LEASE_STARTED M1
    R3 PUBLISH M2
    R4 ACK M1
                     ^
                     P

The snapshot associated with P must represent the logical state produced by
R1 through R4.

Recovery then replays records beginning at P.

## Current Implementation

Only one physical WAL segment exists.

Therefore:

    segmentId = 0

for every `WalPosition`.

The segment field is intentionally present in the abstraction even though
segment rotation is not implemented yet.

## Future Segmented WAL

The expected future structure is:

    segment-000007.wal
    segment-000008.wal
    segment-000009.wal

A snapshot may contain:

    WalPosition(7, 12480)

Recovery would then conceptually perform:

    segment 7:
        replay from byte 12480

    segment 8:
        replay from beginning

    segment 9:
        replay from beginning

The snapshot contract therefore remains valid when WAL segmentation is
introduced.

## Why Not Store Only a Byte Offset?

A byte offset is sufficient only while there is exactly one physical WAL file.

Once WAL rotation or segmentation exists:

    offset = 12480

does not identify the correct source file.

Using `(segmentId, offset)` avoids making the snapshot format depend on the
single-file limitation.

## Why Not Introduce a Logical Sequence Number?

A logical sequence number could provide:

    recordSequence = 105291

This may eventually be useful for:

- replication;
- globally ordered log records;
- distributed consensus;
- logical references independent of physical layout.

However, the current problem is different.

Snapshot recovery needs to know:

> Where should physical WAL replay begin?

`segmentId + offset` directly answers that question.

Introducing sequence numbers now would also require defining:

- when sequence numbers are allocated;
- whether gaps are allowed after failed appends;
- how sequence numbers survive restart;
- how logical sequence maps back to a physical WAL location.

That machinery is not currently required.

## Why Segment ID Instead of Generation?

`generation` could represent WAL replacement epochs, but the expected
architecture is specifically segmented WAL storage.

`segmentId` communicates the physical concept more directly.

Example:

    segmentId = 17

naturally maps to:

    segment-000017.wal

This makes the abstraction easier to understand and debug.

## Concurrency Model

The queue may be accessed concurrently.

A snapshot's logical state and `WalPosition` must represent the same atomic
point in queue history.

In the current architecture this is achieved using the queue's existing
coarse-grained lock:

    acquire queue lock
        |
        v
    capture logical queue state
        |
        v
    capture current durable WalPosition
        |
        v
    release queue lock
        |
        v
    persist snapshot outside the lock

The lock protects the snapshot cut, not the full snapshot file I/O.

## Future Concurrency Evolution

The position abstraction remains applicable as the architecture evolves:

    coarse-grained queue lock
        ↓
    dedicated ordered WAL writer
        ↓
    group commit
        ↓
    segmented WAL
        ↓
    partitioned queue state

Each committed WAL append can eventually produce:

    WalPosition(segmentId, endOffset)

A snapshot can then identify the exact committed position represented by its
state.

## Consequences

### Positive

- Byte offset remains directly seekable using FileChannel.
- Position is unambiguous once WAL segmentation is introduced.
- Snapshot format does not need redesign simply to support WAL rotation.
- No logical sequence-number subsystem is required yet.
- Current implementation remains simple with segmentId = 0.
- Makes future WAL segmentation visible in the storage abstraction.

### Negative

- Snapshot metadata is coupled to the physical WAL layout.
- Segment IDs must eventually have lifecycle and ordering rules.
- WAL compaction must preserve or translate snapshot-position semantics.
- Physical positions alone are not sufficient for future distributed-log
  ordering or replication semantics.
- The abstraction introduces a field that is not yet operationally meaningful
  because only segment 0 currently exists.

## Revisit When

Revisit this decision when introducing:

- replicated logs;
- consensus;
- logical log indexing;
- cross-node replay;
- major WAL compaction that rewrites record positions.

At that point a logical sequence/index may be introduced in addition to,
rather than necessarily replacing, the physical `WalPosition`.

## Mental Model

    segmentId
        =
    "Which WAL file?"

    offset
        =
    "Where inside that WAL file?"

Together:

    WalPosition
        =
    "Where should physical recovery continue?"

This is distinct from a future logical log index, which would answer:

    "Where are we in logical history?"