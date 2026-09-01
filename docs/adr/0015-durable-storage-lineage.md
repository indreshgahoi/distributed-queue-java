# ADR 0015: Durable Storage Lineage

## Status

Accepted for v0.17.0.

## Context

WAL framing, snapshots, segmented recovery, and reclamation validate integrity
and position, but they do not prove that two individually valid artifacts
belong to the same queue history. A copied snapshot can therefore restore
unrelated state or, more severely, authorize deletion of local WAL segments.

Future multi-queue partition placement also needs durable identity below the
logical queue API. Identity must exist before ownership transfer and
replication can safely reason about which storage history they control.

## Decision

Define storage lineage as `(queueId, generationId, partitionId)` and persist it
in WAL header version 2 and snapshot format version 2.

- `queueId` identifies the logical queue.
- `generationId` distinguishes delete/recreate or independently initialized
  histories with the same logical identity.
- `partitionId` identifies the local state-machine shard and is zero today.

The first WAL segment establishes lineage for an unconfigured reopen. All
segments, snapshots, recovery paths, and compaction authorization must match
that tuple exactly. A mismatch fails closed before state restoration or
deletion.

No v1 migration is implemented because the repository is still a learning
system without compatibility obligations.

## Consequences

### Positive

- recovery artifacts are bound to one durable history;
- compaction cannot be authorized by a foreign snapshot;
- mixed segmented-WAL directories are rejected deterministically;
- future ownership epochs and replication have a concrete identity anchor.

### Negative

- v1 WAL and snapshot files are intentionally incompatible;
- operational restore procedures must preserve and verify lineage;
- lineage identifies storage but does not itself establish current ownership.

## Alternatives Considered

### Infer identity from filesystem paths

Rejected because paths change during backup, restore, and deployment and are
not encoded in copied artifacts.

### Use only queue ID

Rejected because deleting and recreating a queue could combine histories that
share a logical name or identifier.

### Add lineage only to snapshots

Rejected because foreign WAL segments would remain indistinguishable and no
durable authority would exist when a snapshot is absent.

## Revisit When

- a control plane creates queues and generations;
- partitions move between storage owners;
- replication requires leader-term or ownership-epoch fencing;
- storage-format migration becomes a supported product requirement.
