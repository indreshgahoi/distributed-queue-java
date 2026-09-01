# ADR 0017: Metadata Service Domain Boundaries

## Status

Accepted for v0.18.0.

## Context

The metadata service owns queue identity and lifecycle authority. Keeping its
domain model, orchestration, HTTP boundary, and PostgreSQL code in one package
would make those responsibilities difficult to distinguish and would allow
framework and persistence details to become accidental dependencies of the
control-plane model.

The service is still small, but its next workflows will add provisioning,
reconciliation, placement, and ownership fencing. Those changes need stable
boundaries before they add more infrastructure concerns.

## Decision

Structure the metadata service around domain-driven and ports-and-adapters
boundaries:

```text
domain
  model                 queue identity and lifecycle concepts
  exception             domain failure vocabulary
application
  port.in               customer and lifecycle use cases
  port.out              required metadata persistence capability
  service               use-case orchestration
adapter
  in.web                 REST transport and HTTP error mapping
  out.postgres           JDBC implementation of the persistence port
MetadataServiceApplication
                        Spring Boot composition root
```

Dependencies point inward. The domain has no dependency on Spring, JDBC, or
HTTP. Application ports use domain types. The application service depends on
the outbound repository port, while adapters depend on the ports they drive or
implement.

Only cross-package contracts are public. Spring-managed implementation
classes, controllers, exception handlers, and constructors remain
package-private where framework proxying permits it. Domain types, use-case
ports, and the repository port are public because they are deliberate module
boundaries, not implementation exposure.

This is a structural decision only. It does not change REST resources,
PostgreSQL schema, transaction boundaries, lifecycle transitions, or metadata
semantics.

## Consequences

### Positive

- domain rules can be understood and tested without transport or database
  details;
- future provisioning and reconciliation enter through explicit use-case
  contracts;
- PostgreSQL can evolve or be replaced without changing the domain model;
- package visibility prevents direct use of Spring implementation classes;
- architectural responsibilities are visible from source layout.

### Negative

- a small service now contains more packages and interfaces;
- DTO-to-domain mapping remains explicit;
- Java package visibility cannot by itself enforce every dependency rule.

## Alternatives Considered

### Keep one package until the service is larger

Rejected because lifecycle authority is already a distinct domain and the
next milestone will introduce a distributed workflow. Delaying boundaries
would make that workflow establish dependencies accidentally.

### Split each layer into a separate Maven module

Deferred because package boundaries are sufficient at the current size.
Additional build modules would add ceremony without yet preventing a concrete
failure mode.

### Expose the service and repository implementations publicly

Rejected because callers should depend on use-case and persistence contracts,
not Spring composition details.

## Revisit When

- package-level dependency violations become difficult to prevent in review;
- a second adapter needs to invoke the same use cases;
- the metadata domain grows enough to justify separate bounded-context Maven
  modules.
