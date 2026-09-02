# ADR 0019: Durable Node Registry and Partition Placement

## Status

Accepted for v0.20.0.

## Context

Provisioning claims prevent concurrent workers from publishing the same queue,
but v0.19 allows any worker ID to request any unclaimed queue. Metadata cannot
identify live node processes, locate authoritative partition storage, or fence
two processes configured with the same stable node ID.

## Decision

Store queue-node registrations and partition placements in PostgreSQL. A
registration has a stable node ID, endpoint, finite lease, and monotonically
increasing registration epoch. Heartbeats renew only the current unexpired
epoch.

Before granting provisioning work, atomically place the oldest unplaced queue
on a live node with the fewest existing placements. Use node ID to break ties.
The first placement has epoch one. Provisioning claims capture registration and
placement epochs, and completion revalidates them against current rows.

Do not automatically reassign an expired node's placement. Placement does not
yet grant runtime message-serving ownership.

The protocol is illustrated in the
[node registration and placement flow](../diagrams/node-registration-placement-flow.md).

## Consequences

### Positive

- queue topology becomes durable and inspectable;
- only the placed node process may materialize storage;
- stale process incarnations are fenced independently of claim tokens;
- the model separates queue generation, process incarnation, placement, and
  provisioning-attempt authority.

### Negative

- nodes must maintain leases with the metadata service;
- least-count placement is intentionally capacity-unaware;
- expired assigned nodes make queues unavailable;
- internal topology APIs remain a trusted-network boundary.

## Alternatives Considered

### Treat worker ID as node identity without registration

Rejected because identity would have no liveness or process-incarnation fence.

### Let the first polling worker assign the queue to itself

Rejected because placement policy would be an accidental race rather than a
metadata decision.

### Automatically reassign after heartbeat expiry

Rejected because local WAL storage does not move with metadata and no replica
is available as a safe recovery source.

## Revisit When

- runtime partition ownership is introduced;
- replicated storage permits safe reassignment;
- placement needs capacity, zone, or draining constraints;
- service authentication binds node identity to credentials.
