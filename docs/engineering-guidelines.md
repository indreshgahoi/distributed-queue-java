# Engineering Guidelines

This document defines the repository's default engineering standard. The goal
is not stylistic uniformity for its own sake; it is to make correctness,
ownership, failure behavior, and review decisions easy to understand.

## 1. Decision Order

For behavior-changing work, use this order:

```text
semantics
    -> invariants
    -> failure scenarios
    -> tests
    -> implementation
    -> regression verification
    -> documentation
```

State the real limitation being solved. Do not copy a production broker
feature without deriving why this system needs it.

## 2. Architecture and Dependencies

- Keep domain models independent of Spring, JDBC, HTTP, and filesystem APIs.
- Define use cases through inbound ports and infrastructure needs through
  outbound ports.
- Keep adapters replaceable and composition in the application/configuration
  layer.
- PostgreSQL is metadata authority; queue WAL and snapshots are data-plane
  durability authority. Do not blur this boundary.
- Keep transactions short. Never hold a database transaction across network or
  filesystem operations.
- Scope synchronization to the state and authority it protects. Do not hold a
  node-wide coordinator lock across queue or filesystem I/O when partitions
  have independent lifecycle and failure ownership.
- When draining multiple resources after process-wide authority loss, close all
  admission gates before waiting for any one resource to finish.
- Introduce a dependency only when its benefit exceeds its operational,
  security, build, and maintenance cost.
- Preserve package-private visibility for implementation details and test seams.

## 3. Java Formatting and Structure

- Target Java 21 and use four spaces; never use tabs.
- Keep one top-level production type per file and match file/type names.
- Use braces for every control-flow body.
- Prefer immutable records and final fields for values and dependencies.
- Validate invariants at construction boundaries.
- Keep methods focused around one level of abstraction.
- Order a class as constants, fields, constructor, public methods, private
  orchestration methods, then low-level helpers.
- Avoid unrelated formatting in behavior changes; small diffs are easier to
  audit for durability regressions.
- Run `git diff --check` before handoff.

## 4. Naming

- Name types by responsibility, not implementation accident:
  `LocalMessageQueue`, not `InMemoryMessageQueue` when storage may be durable.
- Use nouns for domain values and verbs for commands/use cases.
- Include units in names when the type does not encode them, such as
  `leaseSeconds` or `walSegmentBytes`.
- Use the established distributed-system vocabulary consistently:
  `generationId`, `partitionId`, `leaseExpiresAt`, `fencingToken`, and
  `metadataVersion` are distinct authorities and must not be conflated.
- Test names describe observable behavior and conditions, for example
  `expiredClaimIsTakenOverWithHigherFencingToken`.
- Avoid generic names such as `Manager`, `Util`, or `Helper` unless the type has
  a precise lifecycle or coordination responsibility.

## 5. Logging

### Purpose

Logs explain operational state transitions and failures. They are not a trace
of every method call and are not a replacement for metrics or invariants.

Log primarily at system boundaries:

- durable lifecycle transitions;
- distributed authority acquisition, renewal, loss, or takeover;
- startup, shutdown, and recovery decisions;
- external dependency failures and recovery;
- data corruption, lineage mismatch, or fail-closed decisions.

Do not log successful getters, list operations, empty polling loops at INFO, or
the same exception at every layer.

### Event shape

Use stable event names and structured key/value fields:

```java
log.info(
        "event=provisioning_claim_acquired queueId={} "
                + "generationId={} partitionId={} workerId={} "
                + "fencingToken={}",
        queueId,
        generationId,
        partitionId,
        workerId,
        fencingToken
);
```

Use parameterized logging. Do not concatenate runtime values before the logger
decides whether the level is enabled.

### Levels

- `ERROR`: the component cannot uphold an invariant or continue its assigned
  responsibility without intervention.
- `WARN`: degraded operation, rejected stale authority, or a retryable failure
  that operators should know about.
- `INFO`: low-volume lifecycle, authority, recovery, and configuration events.
- `DEBUG`: diagnostic retry detail and implementation-level decisions.
- `TRACE`: high-frequency events such as an empty reconciliation poll.

### Exceptions and retry loops

- Log a stack trace at the layer that owns retry, termination, or the external
  response—not at every layer through which the exception passes.
- The first failure in an outage may include its stack trace. Suppress or
  downgrade repeated identical failures, periodically summarize continued
  degradation, and emit a recovery event.
- Never catch an exception only to log and silently discard it unless the
  owning loop explicitly provides retry/isolation semantics.
- Preserve secondary failures as suppressed exceptions when they are relevant
  to the primary failure.

### Sensitive and high-cardinality data

- Never log credentials, authorization headers, idempotency keys, message
  bodies, receipt handles, or customer payloads.
- Queue, generation, partition, worker, fencing, and metadata-version fields
  are acceptable correlation dimensions in logs, but should not automatically
  become unbounded metric labels.
- Sanitize exception messages received from external systems before exposing
  them to customer responses.

Use Lombok `@Slf4j` for class loggers. Lombok is compile-time-only and must not
become part of domain semantics or generated domain behavior.

## 6. Testing

- Test observable contracts, not private implementation details.
- Every invariant needs a positive test and at least one rejection/failure test.
- For durable state, test restart and recovery—not only in-process behavior.
- For concurrent claims, use real database transactions where locking semantics
  matter.
- Use deterministic clocks for lease and expiry tests.
- Verify idempotent retries after ambiguous success responses.
- Verify stale generation, partition, worker, fencing token, version, and
  receipt-handle rejection where applicable.
- Do not assert ordering when the contract does not guarantee ordering.
- Keep unit tests fast; use integration tests only for behavior supplied by the
  real boundary, such as PostgreSQL locking or filesystem durability.
- Run `mvn clean test` before milestone handoff.

## 7. Failure Handling and Durability

- Write durable intent before exposing a successful state transition.
- Fail closed when authority, lineage, format, or integrity cannot be proven.
- Make external side effects deterministic and idempotent when transactions
  cannot span resources.
- A lease permits progress after expiry; a fencing token prevents an expired
  actor from publishing stale results. Use both where old actors can resume.
- Document the exact crash points before implementing a cross-resource flow.
- Never claim stronger filesystem durability than the performed force and
  directory-sync operations provide.

## 8. Database and API Changes

- Apply schema changes through immutable, versioned Flyway migrations.
- Enforce critical invariants in both domain validation and database
  constraints where practical.
- Use conditional updates for lifecycle transitions and return the resulting
  authoritative row.
- Public APIs use validated DTOs, standard status codes, and stable
  `application/problem+json` failures.
- Internal APIs remain explicit trust boundaries and must be documented as
  such. Authentication is required before production exposure.
- Idempotency behavior is part of API semantics and must be tested.

## 9. Documentation and Repository Hygiene

- Record significant architectural decisions in an ADR.
- Keep `README.md`, semantics, failure scenarios, trade-offs, diagrams, and the
  roadmap consistent with implemented behavior.
- Include local startup and verification instructions for deployable modules.
- Do not commit generated targets, local data, credentials, or IDE state.
- Keep commits cohesive and use an imperative subject that explains the
  engineering outcome.
- Do not tag incomplete milestones. Tag only after the full regression suite,
  documentation, and clean working-tree review are complete.

## 10. Review Checklist

Before requesting review, answer:

- What invariant is introduced or preserved?
- Where is the linearization or durable publication point?
- What happens at every external-call and crash boundary?
- Can an old actor resume, and if so, how is it fenced?
- Is retry safe after an ambiguous response?
- Are logs actionable, bounded, correlated, and free of sensitive data?
- Do tests exercise the real concurrency/durability mechanism?
- Are non-guarantees and deferred work explicit?
- Does `mvn clean test` pass?
- Does `git diff --check` pass?
