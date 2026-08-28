# ADR-0011: Use Immutable WAL Segments with Atomic Rotation

## Status

Accepted

## Context

The original durable queue used a single append-only WAL file.

This established core durability semantics:

- WAL-first state transitions
- length-prefixed frames
- CRC32C integrity
- torn-tail recovery
- versioned WAL headers
- durable positions
- snapshot + WAL suffix recovery

However, a single WAL file grows without bound.

Snapshots make older WAL history logically redundant, but reclaiming a prefix
from a single active file would require rewriting the remaining suffix while
new records may still be appended.

This introduces unnecessary complexity and risk.

We therefore need a storage layout that allows old durable history to become
immutable and eventually removable without rewriting the active log.

## Decision

Store WAL history as a sequence of segment files:

    segment-000000.wal
    segment-000001.wal
    segment-000002.wal

The highest authoritative `.wal` segment is active.

All lower authoritative segments are sealed and immutable.

Temporary rotation candidates use:

    segment-000003.tmp

Temporary files are never authoritative.

## Segment Authority

No separate ACTIVE or SEALED metadata is persisted.

Authority is derived from filesystem structure:

    highest valid *.wal
        =
    active segment

All lower valid segments are sealed.

This intentionally reduces the number of mutable metadata facts that must be
coordinated during rotation.

## Rotation Policy

Segment size is a rotation target, not a hard frame boundary.

If an append causes a segment to exceed the target size, the entire frame
remains in that segment.

Rotation occurs before the next append.

Example:

    target = 64 MB
    current = 63.9 MB
    next frame = 500 KB

Result:

    current segment = 64.4 MB

The following append triggers rotation.

A WAL frame is never split across segments.

## Crash-Safe Rotation Protocol

For active segment N:

    1. Determine N + 1.
    2. Create segment-(N+1).tmp.
    3. Initialize its WAL header.
    4. Force the candidate.
    5. Atomically rename candidate to segment-(N+1).wal.
    6. Open the promoted segment for append.
    7. Switch in-memory active state.
    8. Close the previous append channel.

The atomic rename is the durable authority transition.

Before rename:

    segment N remains authoritative.

After rename:

    segment N+1 is authoritative.

If the process crashes after promotion but before in-memory state changes,
restart discovers N+1 as the highest authoritative segment.

## Failure Semantics

### Failure before promotion

Examples:

- candidate initialization failure
- candidate force failure
- atomic promotion failure

Result:

    segment N remains authoritative.

The candidate `.tmp` file may be discarded or ignored.

Future rotation attempts must not be permanently blocked by stale temporary
files.

### Failure after promotion

If promotion succeeds but opening or activating the new segment fails:

    segment N+1 is already authoritative on disk.

The current WAL instance must not continue writing to segment N.

The WAL instance becomes unusable/poisoned and requires restart.

Restart derives the correct active segment from durable filesystem state.

## Recovery Policy

Segments are replayed in ascending segment ID order.

Sealed segments use strict recovery:

    torn final frame -> corruption -> fail

Active segment uses crash-tail recovery:

    torn final frame -> truncate to last complete frame

Checksum mismatch, invalid frame length, or logical decoding failure are
corruption in both active and sealed segments.

## WalPosition

A WAL position is:

    WalPosition(segmentId, offset)

The offset is the first byte not represented by the corresponding snapshot.

`readFrom(position)`:

    - starts at that frame boundary in the referenced segment;
    - skips earlier segments;
    - replays the remainder of that segment;
    - then replays all later segments in ascending order.

Persisted positions are validated against actual frame boundaries before use.

## Append Failure

If an append may have partially written a frame and then fails, the WAL writer
is poisoned.

No subsequent record may be appended behind a potentially torn frame.

Recovery after restart may repair a torn tail only in the active segment.

## Consequences

### Positive

- WAL growth is divided into bounded physical files.
- Sealed history is immutable.
- Old segments can later be deleted safely after snapshot coverage.
- Rotation does not require rewriting the active WAL.
- `WalPosition(segmentId, offset)` maps naturally to segmented storage.
- Crash recovery can derive active state without a separate manifest.
- Corruption policy is stronger for sealed history.

### Negative

- Recovery must discover and validate multiple files.
- Segment ordering and continuity become correctness concerns.
- Rotation becomes a durability protocol.
- More filesystem operations and failure modes exist.
- Atomic rename support is required for strong rotation semantics.
- Directory durability semantics remain filesystem-dependent.

## Alternatives Considered

### Continue with one WAL file

Rejected for long-term storage because prefix reclamation would require
rewriting the active file or retaining unbounded history.

### Split frames across segment boundaries

Rejected.

A frame belongs entirely to one segment so each segment remains independently
parseable.

### Persist ACTIVE/SEALED flags

Rejected for now.

The same state can be derived from authoritative segment ordering, avoiding
additional mutable metadata.

### Introduce a manifest

Deferred.

Current segment lifecycle can be derived safely from exact `.wal` filenames.

A manifest may become useful later for richer metadata such as compaction
generations, logical sequence ranges, replication state, or segment
replacement.

## Revisit When

Revisit when introducing:

- physical segment deletion;
- concurrent/multiple WAL writers;
- replication;
- logical log indexes;
- segment compaction or replacement;
- group commit;
- filesystem layouts where directory discovery becomes expensive.