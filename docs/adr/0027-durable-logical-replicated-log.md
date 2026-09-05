# ADR 0027: Persist Logical Index and Term in the Segmented WAL

## Status

Accepted for v0.28.0.

## Context

v0.26 introduced follower sequence and durable leader-epoch fencing. v0.27
transported bounded batches. The follower currently reconstructs sequence from
`records.size()` after reading complete WAL history. This cannot survive
snapshot-authorized prefix reclamation and makes duplicate lookup O(retained
history) in memory.

`WalPosition(segmentId, offset)` already provides the correct physical boundary
for local snapshot replay and whole-segment deletion. It is not a stable
replication identity: it changes with layout and cannot carry the leader term
needed for log freshness, conflict detection, or future elections.

The v0.27.1 baseline also shows that the current follower batch performs one
`force(true)` per record. At batch size 256, the measured current path delivered
approximately 153 records/s, while a benchmark-only one-force prototype
delivered approximately 26,854 records/s on the recorded host. JSON encoding
was sub-millisecond at p50 and was not the primary bottleneck.

## Decision

Persist a positive `logIndex`, positive `logTerm`, and complete `WalRecord` in
every version-3 WAL frame. Indexes are consecutive within one `StorageLineage`
and are assigned under the authoritative per-lineage append lock.

Retain `WalPosition` as a separate physical concept. Extend snapshot version 3
to bind materialized state to both `(lastIncludedIndex,lastIncludedTerm)` and
the first physical byte not represented by that state.

`lastIncludedTerm` is the immutable term of the log entry identified by
`lastIncludedIndex`. It preserves the identity of the compacted boundary after
that entry's frame is reclaimed. It is historical entry metadata and must not
be replaced by the replica's `currentTerm` or by the term in effect when the
snapshot happens to be created.

Introduce a lineage-bound, CRC32C-protected, atomically published replica
hard-state file containing `currentTerm`, `votedFor`, and `commitIndex`.
`lastApplied` is recovered from the durable snapshot boundary and replay; it is
not independently authoritative because the applied in-memory queue state does
not otherwise survive restart.

Add a durable batch append operation that:

1. validates the complete request before mutation;
2. handles an identical existing prefix as idempotent retry;
3. writes every new complete frame to one active segment;
4. calls `force(true)` once;
5. advances durable progress only after force succeeds;
6. poisons the writer after write or force ambiguity.

A failed batch may leave a recoverable complete prefix. Therefore “batch” means
one success durability boundary, not all-or-nothing transactional visibility.
Unchanged retry discovers the prefix and resumes at the first absent index.

WAL version 2, snapshot version 2, and `replica-leader-epoch.bin` are not
migrated. The learning repository has no deployed compatibility obligation,
and explicit rejection keeps recovery reasoning small and testable.

## Consequences

### Positive

- Logical identity survives restart, rotation, and snapshot-authorized prefix
  reclamation.
- A follower can detect retries and conflicts without counting from segment zero.
- Snapshots carry the term needed to compare a reclaimed boundary safely.
- Later majority commit and elections can rely on durable index/term and hard
  state rather than inventing another authority format.
- One force can durably cover a bounded batch without weakening success
  semantics.
- Physical storage layout remains independently usable for recovery and deletion.

### Negative

- All existing WAL and snapshot fixtures become intentionally incompatible.
- Startup initially scans retained frames to rebuild the in-memory logical index.
- A batch failure requires writer restart/recovery even when the exact prefix
  appears obvious in process.
- Large batches can create an oversize segment because a durability group does
  not span rotation.
- Hard state adds another artifact whose ordering and directory durability must
  be fault-tested.
- This milestone still does not make client acknowledgements majority durable.

## Alternatives considered

### Use `WalPosition` as the replication index

Rejected. It identifies local bytes, changes with physical layout, and carries
no term. It is correct for replay and reclamation but not consensus identity.

### Store logical indexes only in an external sidecar index

Rejected. A crash could make the sidecar and WAL disagree, requiring another
atomicity protocol. Entry identity belongs inside the checksummed authoritative
frame; a rebuildable side index may be added later for performance.

### Continue counting retained records and store a snapshot base count

Rejected. It couples identity to retention layout, makes corruption/gap
reasoning indirect, and still does not bind a term to every entry.

### Replace `WalPosition` with logical index in snapshots

Rejected. Logical index cannot identify an exact byte or which whole physical
segments are safe to delete. Snapshots require both logical and physical
boundaries.

### Make batch append transactionally all-or-nothing

Rejected for the append-only file format. A crash can expose complete prefix
frames before the final force result is known. A commit marker could hide the
prefix but adds recovery and reclamation complexity without being required for
safe idempotent retry. The invariant needed is “success means all durable;
failure means recover and retry,” not transactional invisibility.

### Persist `lastApplied` in hard state

Rejected as an independent authority. If queue state is lost but the marker
survives, recovery could skip committed effects. The durable snapshot proves
which effects exist; replay reconstructs the rest.

### Add node-wide group commit now

Deferred. It may improve force amortization but couples unrelated partitions'
latency and failure domains. v0.28 establishes a per-partition durable batch
primitive first. Cross-caller asynchronous coalescing requires separate
fairness, latency, shutdown, and backpressure design.

### Implement format migration

Rejected for this milestone. Migration adds dual readers and mixed-version
failure cases without serving a deployed user. The version check remains an
explicit operational guardrail.

## Required validation before acceptance

- Review the HLD and LLD approval questions.
- Add semantics and failure-scenario entries before production implementation.
- Freeze exact magic, version, integer order, bounds, and checksum coverage.
- Prove snapshot `(index,term,position)` consistency through tests.
- Fault-test every write, force, promotion, and directory-force boundary.
- Compare production batch results with the checked-in v0.27.1 baseline.

## References

- [v0.28 high-level design](../design/v0.28-durable-log-hld.md)
- [v0.28 low-level design](../design/v0.28-durable-log-lld.md)
- [Replication protocol](../architecture/appendix-b-replication-protocol.md)
- [Group commit and durability](../architecture/appendix-c-group-commit-and-durability.md)
- [v0.27.1 benchmark results](../benchmarks/v0.27.1/README.md)
