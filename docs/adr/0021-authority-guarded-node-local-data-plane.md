# ADR 0021: Authority-Guarded Node-Local Data Plane

## Status

Accepted for v0.22.0. The node-wide monitor decision is superseded by
[ADR 0022](0022-per-partition-runtime-admission.md) in v0.23.0; the guarded
callback and admission-time authority decisions remain in force.

## Context

v0.21 can recover an authoritative partition and publish READY, but no message
request can safely obtain that runtime. Returning a queue reference from the
manager would allow reconciliation to close it while a request is using it.
Treating PostgreSQL's last READY row as serving authority would also outlive
the registration lease that authorized publication.

## Decision

Expose node-local HTTP operations for publish, receive, ACK, and NACK. Resolve
queue ID only against the manager's installed active runtime and revalidate its
registration epoch and unexpired lease at request admission.

Execute each operation as a callback while holding the same lifecycle monitor
used by reconciliation and shutdown. Never return `RuntimeQueue` to callers.
Delegate mutation to `LocalMessageQueue` through a narrow adapter. Maintain a
secondary queue-ID serving index for constant-time request admission while the
full lineage-keyed map remains authoritative for lifecycle reconciliation.

## Consequences

- operation and closure have a deterministic happens-before order;
- runtime lookup does not scan all active partitions on every request;
- expired local process authority is rejected even before the next poll;
- lifecycle changes and all local data-plane operations are serialized;
- clients must address the owning node directly;
- ambiguous publish responses can create duplicates.

## Alternatives Considered

### Return the active RuntimeQueue to the controller

Rejected because the reference could escape the authority boundary and race
with close.

### Query PostgreSQL on every message operation

Rejected for this phase because it places the control-plane database in every
data-plane operation and still does not coordinate local close. Registration
is checked locally and placement changes are consumed by reconciliation.

### Reference-counted per-partition handles

Deferred. It permits concurrency across partitions and graceful draining but
adds lifecycle states and reference accounting before measurements show the
single monitor is a bottleneck.

### Add a routing gateway now

Deferred until node-local serving semantics and failures are stable. A gateway
should depend on this authority boundary, not define it implicitly.
