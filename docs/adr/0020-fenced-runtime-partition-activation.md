# ADR 0020: Fenced Runtime Partition Activation

## Status

Accepted for v0.21.0.

## Context

v0.20 records where a partition belongs and fences provisioning, but placement
does not prove storage was recovered or that the current process may serve it.

## Decision

Queue nodes reconcile node-specific `ACTIVE` placements into owned
`LocalMessageQueue` instances. Runtime identity contains queue lineage, node
ID, registration epoch, and placement epoch. PostgreSQL accepts runtime status
publication only while those values and the registration lease remain current.
A node closes active runtimes when registration authority is absent.

Do not add a separate runtime lease. Registration already bounds process
authority and placement epoch already fences assignment. A third token would
add expiry ordering without adding an independent guarantee.

Runtime status is observation, not authority. Stored `READY` does not extend a
lease and must be interpreted with current registration and placement metadata.

## Consequences

- `READY` means recovery completed under authority current at publication;
- stale asynchronous recovery is rejected and closed;
- metadata outage beyond the node lease causes fail-closed unavailability;
- routed message APIs can later depend on an explicit runtime boundary.

## Alternatives Considered

### Independent runtime ownership lease

Rejected because existing registration and placement fencing already compose
the required authority for this non-replicated phase.

### Placement as serving authority

Rejected because placement survives process crashes and proves neither recovery
nor process incarnation.

### READY before recovery

Rejected because clients could route to incomplete or mismatched storage.

## Deferred

Data-plane routing, producer idempotency, reassignment, storage transfer,
replication, and leader election remain outside v0.21.
