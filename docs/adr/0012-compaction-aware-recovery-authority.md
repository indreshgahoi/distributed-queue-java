# ADR 0012: Compaction-Aware Recovery Authority

## Status

Accepted for v0.14.0.

## Context

Before v0.13.0, the WAL retained complete history. A missing or corrupt
snapshot could therefore be ignored and recovery could replay the WAL from its
beginning.

v0.13.0 made whole WAL segments reclaimable after snapshot promotion. After
that deletion, neither artifact is independently complete:

    authoritative snapshot + retained WAL suffix = recoverable queue state

The previous fallback policy could replay a retained suffix as though it were
complete history, producing a plausible but incomplete queue. The in-memory
compaction boundary also disappeared on restart, permitting a stale snapshot
to replace a newer recovery point.

## Decision

Treat the first authoritative WAL segment as durable evidence of history
completeness:

- first segment is 0: WAL-only fallback remains valid;
- first segment is greater than 0: a snapshot is mandatory.

Recovery fails closed when a mandatory snapshot is missing, unreadable, or
invalid. A valid snapshot position must identify an available segment and a
real frame boundary before the queue becomes available.

Snapshot/compaction coordination reconstructs its monotonic boundary from the
latest committed snapshot during initialization. Every candidate position is
validated against the current WAL before snapshot promotion.

## Why No Manifest Yet

Segment IDs begin at zero, increase monotonically, and reclaimed segments are
deleted only as a prefix. Therefore an earliest retained segment greater than
zero already proves that WAL-only recovery is incomplete. A separate manifest
would duplicate this fact without yet solving another required problem.

A manifest should be reconsidered if segment IDs can be rebased, restored from
remote storage, rewritten, or associated with richer queue-generation
metadata.

## Consequences

### Positive

- A corrupt or missing mandatory snapshot cannot cause silent partial state.
- Snapshot monotonicity survives coordinator restart.
- Invalid candidate positions cannot replace the previous recovery point.
- Pre-compaction deployments retain their existing WAL-only fallback.
- No WAL or snapshot binary format change is required.

### Negative

- Some failures that previously allowed startup now deliberately make the
  queue unavailable.
- Candidate validation may scan WAL frames to prove an offset boundary.
- Segment numbering now carries recovery-completeness meaning.
- This validates structural compatibility, not yet a unique queue lineage.

## Alternatives Considered

### Always require a snapshot

Rejected because it would unnecessarily remove WAL-only recovery before any
history has been reclaimed.

### Continue falling back to the earliest retained segment

Rejected because the suffix does not contain state represented only by the
lost snapshot.

### Introduce a durable manifest immediately

Deferred because current segment numbering already proves whether a prefix
was reclaimed. A manifest becomes justified when it must carry additional
authority such as queue identity, generations, or remote-restoration state.

## Revisit When

- storage artifacts may be copied or restored independently;
- queue-generation identity is introduced;
- segment IDs may be rewritten or rebased;
- remote snapshots or replicated storage become authoritative.
