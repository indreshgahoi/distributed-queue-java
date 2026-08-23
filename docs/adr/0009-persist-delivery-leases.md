# ADR-0009: Persist Delivery Leases Before Exposing Deliveries

## Status

Accepted

## Context

The queue uses visibility leases to provide temporary delivery ownership.

A delivery currently consists of:

    messageId
    receiptHandle
    attempt
    leaseUntil

Before this decision, the READY -> IN_FLIGHT transition was created only in
memory.

The WAL persisted later outcomes such as:

- ACK
- NACK
- LEASE_EXPIRED
- DEAD_LETTER

but did not persist the creation of the delivery lease itself.

This creates a recovery gap.

Example:

    PUBLISH M1
        |
        v
    receive M1 as R1
        |
        v
    queue crashes

If recovery only sees:

    PUBLISH M1

it reconstructs M1 as READY even though a consumer may still be processing the
delivery under R1.

This violates the visibility lease contract and can cause premature
redelivery.

Snapshotting makes this gap even more important because WAL records after a
snapshot must be capable of reconstructing every externally visible queue
transition.

## Decision

Introduce a durable WAL record:

    LEASE_STARTED

A LEASE_STARTED record contains:

    messageId
    receiptHandle
    attempt
    leaseUntil

`receive()` must persist this record before transitioning the message from
READY to IN_FLIGHT and before returning the Delivery to the consumer.

Required ordering:

    select READY message
        |
        v
    generate receiptHandle
        |
        v
    determine leaseUntil
        |
        v
    WAL.append(LEASE_STARTED)
        |
        v
    durability boundary
        |
        v
    READY -> IN_FLIGHT
        |
        v
    return Delivery

## Lease Survival Across Restart

A queue restart does not terminate an active lease.

Recovery restores:

    receiptHandle
    attempt
    leaseUntil

Therefore a still-valid recovered receipt handle may continue to be used for:

    ack(receiptHandle)

or:

    nack(receiptHandle, retryDelay)

until that lease terminates.

## Rationale

The queue has already exposed delivery ownership to an external consumer.

A process restart is an implementation event inside the queue and should not
silently revoke that externally established ownership.

The lease itself already defines the ownership-expiration boundary.

Changing that boundary because the queue process restarted would introduce a
new semantic unrelated to the lease contract.

## Alternatives Considered

### 1. Make all IN_FLIGHT messages READY immediately after restart

Rejected.

This would cause premature redelivery and would make process restart an
implicit lease-revocation event.

It could increase duplicate processing even when the original consumer is
still working.

### 2. Preserve leaseUntil but invalidate the old receipt handle

Rejected.

This preserves message invisibility but destroys the consumer's ability to
complete the delivery.

A consumer that successfully processed the message could no longer ACK it
after queue restart.

This produces unnecessary redelivery after lease expiry.

### 3. Preserve the complete delivery lease

Selected.

Persist and recover:

    receiptHandle
    attempt
    leaseUntil

This preserves both invisibility and ownership semantics.

### 4. Do not persist receive() at all

Rejected.

The queue would have no durable evidence of externally visible deliveries,
making recovery dependent on volatile state.

This also prevents correct snapshot + WAL-suffix recovery.

## Consequences

### Positive

- Active leases survive queue restart.
- Premature redelivery after restart is prevented.
- Existing receipt handles remain usable after restart.
- Delivery attempt progression remains consistent.
- Snapshot + WAL recovery can reconstruct post-snapshot deliveries.
- Every externally visible queue transition becomes durably represented.

### Negative

- `receive()` now requires a WAL append.
- Delivery latency may increase because the delivery cannot be returned before
  the durability boundary is crossed.
- WAL volume increases because every delivery attempt produces a LEASE_STARTED
  record.
- Recovery becomes responsible for reconstructing IN_FLIGHT state.
- Receipt handles become durable tokens rather than process-local identifiers.

## Performance Status

Persisting every delivery lease is expected to add work to the receive path.

The performance impact has not been measured.

No latency or throughput claim should be made until benchmarks exist.

Possible future optimizations include:

- group commit
- batched lease persistence
- asynchronous durability with weaker delivery guarantees

Any such optimization would require an explicit durability-contract change or
equivalent correctness mechanism.

## Security Consideration

Receipt handles now survive process restart and are stored durably.

They should therefore be treated as capability-like ownership tokens rather
than meaningless random runtime identifiers.

Future external APIs may need to consider:

- sufficient entropy
- accidental exposure
- logging practices
- token lifetime

This ADR does not introduce authentication or cryptographic protection for
receipt handles.

## Relationship to Snapshotting

Snapshotting did not create the missing durability guarantee.

It exposed it.

For snapshot + WAL-suffix recovery to be correct, every state transition after
the snapshot that changes externally observable queue behavior must be
represented durably.

LEASE_STARTED closes the READY -> IN_FLIGHT gap.

## Mental Model

A delivery lease is:

    Lease {
        messageId,
        receiptHandle,
        attempt,
        leaseUntil
    }

The lease represents a durable time-bounded ownership decision.

Queue restart:

    != lease expiry
    != ACK
    != NACK
    != ownership revocation

Therefore restart does not terminate the lease.