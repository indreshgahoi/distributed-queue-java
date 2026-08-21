# ADR-0002: READY Queue Ordering

## Status

Accepted

## Context

Messages are initially published in FIFO order.

Once visibility leases and retries were introduced, an expired message could return to READY after newer messages had already become eligible for delivery.

Strict global ordering across retries would require failed messages to block newer work.

## Decision

Guarantee FIFO ordering only among messages currently in READY.

Expired or delayed messages are appended to the tail of READY when they become eligible again.

## Example

Initial publication order:

M1, M2, M3

M1 and M2 are delivered.

M1 later expires while M3 remains READY.

The resulting READY order may be:

M3, M1

## Rationale

This avoids head-of-line blocking caused by failed or poison messages.

The project currently prioritizes reliable work distribution over strict global processing order.

## Alternatives Considered

### Reinsert expired messages at the head

Rejected because repeatedly failing messages could block unrelated work.

### Strict global FIFO

Rejected because it would significantly reduce concurrency and throughput.

### Message-group ordering

Deferred for a later phase because it introduces partition/group semantics.

## Consequences

Positive:

- failed messages do not block unrelated READY work
- queue remains simple
- higher potential concurrency

Negative:

- publication order is not preserved across redelivery
- processing order is not globally FIFO