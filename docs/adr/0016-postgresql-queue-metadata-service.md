# ADR 0016: PostgreSQL-Backed Queue Metadata Service

## Status

Accepted for v0.18.0.

## Context

Durable storage lineage identifies one local queue history, but the system has
no authority for which customer queues exist or which lineage is current.
Constructing `LocalMessageQueue` directly cannot support an SQS-like namespace,
safe delete/recreate behavior, or coordination by multiple future service
nodes.

Queue metadata and queue-node filesystems cannot commit atomically. The model
must expose that boundary instead of reporting an active queue before storage
exists.

## Decision

Create a separate Spring Boot module and deployable service backed by
PostgreSQL. PostgreSQL owns
tenant/name uniqueness, queue ID, generation ID, lifecycle state, partition
count, and metadata version. The initial customer API supports create, get,
list, and begin-delete.

Creation atomically reserves a tenant-scoped idempotency key, inserts a
`PROVISIONING` descriptor, and records its response queue ID. Lifecycle changes
use compare-and-set SQL over queue ID, generation ID, expected state, and
metadata version.

The single partition is represented by the descriptor's queue and generation
IDs plus partition ID zero. No node placement is recorded yet.

Expose tenant-scoped REST resources using validated request/response DTOs,
standard HTTP status codes, `ProblemDetail` failures, graceful shutdown, and
Actuator health probes. Use HikariCP for connection pooling and Flyway
versioned migrations for schema authority. The durable queue engine remains in
the independent `queue-core` Maven module.

## Consequences

### Positive

- multiple tenants can own independent queue namespaces;
- create retries converge after ambiguous responses;
- stale workers cannot overwrite newer lifecycle authority;
- delete/recreate produces a new storage lineage;
- metadata is ready for a future provisioning reconciler.

### Negative

- PostgreSQL and its operational lifecycle are now required;
- control-plane availability depends on database availability;
- JDBC transaction code remains explicit inside the repository;
- Spring Boot expands the control-plane dependency and runtime surface;
- `PROVISIONING` and `DELETING` can remain stuck until reconciliation exists.

## Alternatives Considered

### Continue with a local file registry

Rejected because the selected goal is a separate shared metadata service, and
a local registry would immediately become an ownership bottleneck for multiple
service instances.

### Store queue messages in PostgreSQL

Rejected because PostgreSQL is the control-plane authority. Moving message and
lease state into it would replace rather than evolve the queue durability work.

### Mark queues ACTIVE in the create transaction

Rejected because PostgreSQL cannot prove that remote or filesystem storage was
created. `PROVISIONING` records the real distributed transaction boundary.

### Introduce consensus and placement now

Rejected because node membership, ownership epochs, and reconciliation should
build on tested metadata lifecycle semantics in the next milestone.

## Revisit When

- provisioning reconciliation is introduced;
- metadata service replicas require connection pooling and health management;
- schema changes require versioned migration tooling;
- partition placement requires leases or ownership epochs.
