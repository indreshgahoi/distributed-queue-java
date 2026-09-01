# ADR 0013: Durable Filesystem Authority Transitions

## Status

Accepted for v0.15.0.

## Context

The queue uses forced file contents and atomic rename to publish snapshots and
WAL segments. These mechanisms protect bytes and make namespace transitions
atomic to observers, but they do not necessarily make the parent-directory
entry durable across power loss.

This distinction matters because filenames carry authority:

- `snapshot.dat` is the committed recovery checkpoint;
- the highest `.wal` segment is active;
- absence of reclaimed segment files records storage-lifecycle progress.

## Decision

Every authority-changing filesystem operation must force its containing
directory before reporting success:

    durable file contents
        -> atomic create/rename/delete
        -> force parent directory
        -> report success

Snapshot directory-force failure is reported as save failure, so the attempt
cannot authorize compaction. Because rename may already have occurred, the
visible snapshot is indeterminate across power loss and is resolved by normal
recovery after restart.

WAL directory-force failure after segment promotion poisons the current
writer. Continuing could acknowledge records whose segment name is not known
durable. Restart discovers the authoritative files and forces their directory
before opening the active segment.

Segment reclamation forces the directory after each deletion. Failure stops
further deletion; already deleted segments were snapshot-covered and remain
safe whether the deletion survives power loss or not.

## Consequences

### Positive

- the durability contract includes filesystem namespace metadata;
- snapshot/WAL authority publication has an explicit final boundary;
- post-promotion failures are modeled honestly rather than as rollback;
- reclamation cannot race ahead of durable deletion progress.

### Negative

- lifecycle operations incur additional synchronous I/O;
- directory forcing is not uniformly supported across all filesystems;
- tests require a separate failure-injection seam for metadata durability;
- post-rename failure remains an indeterminate outcome, though it is safe.

## Alternatives Considered

### Rely on atomic rename alone

Rejected because atomic visibility and power-loss durability are different
properties.

### Silently ignore unsupported directory forcing

Rejected because it would weaken the durability contract based on deployment
environment without making that change visible to callers.

### Force once after deleting an entire segment batch

Deferred. Per-delete forcing is slower but gives the simplest ordering and
failure model. Batching can be revisited with explicit crash tests.

## Revisit When

- lifecycle I/O measurements show directory forces are a bottleneck;
- Windows or non-POSIX storage becomes a supported target;
- storage operations move behind a pluggable durability abstraction;
- replicated storage becomes the source of authority.
