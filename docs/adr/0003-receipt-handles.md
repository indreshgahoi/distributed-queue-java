# ADR-0003: Use Receipt Handles for Delivery Ownership

## Status

Accepted

## Context

Acknowledgement originally used:

ack(messageId)

After introducing visibility leases, the following race became possible:

1. Consumer C1 receives M1.
2. C1's lease expires.
3. M1 becomes READY again.
4. Consumer C2 receives M1.
5. C1 wakes up and calls ack(M1).

Using only the message ID cannot distinguish the stale delivery from the current delivery.

## Decision

Introduce a unique receipt handle for every delivery attempt.

Acknowledgement and NACK operate on the receipt handle rather than message ID.

## Model

Message identity:

M1

First delivery:

messageId = M1
receiptHandle = R1

Second delivery:

messageId = M1
receiptHandle = R2

R1 and R2 represent different ownership periods.

## Rationale

Message identity answers:

"What logical message is this?"

Receipt handle answers:

"Which delivery currently has the right to act?"

## Alternatives Considered

### Continue acknowledging by message ID

Rejected because stale consumers could acknowledge newer deliveries.

### Maintain consumer ID ownership

Rejected because consumer identity alone is not enough to distinguish multiple deliveries to the same consumer.

## Consequences

Positive:

- stale acknowledgements are rejected
- ownership semantics become explicit
- ACK and NACK apply to a specific delivery attempt

Negative:

- additional delivery metadata must be maintained
- clients must preserve the receipt handle until ACK/NACK