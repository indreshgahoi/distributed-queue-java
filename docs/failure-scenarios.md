# Failure Scenarios

## F1 — Consumer crashes after receive

Initial state:

READY = [M1]

Sequence:

1. Consumer C1 calls `receive()`.
2. M1 is removed from the queue.
3. C1 crashes before processing completes.
4. M1 is no longer available.

Result:

The message is permanently lost.

## Violated Requirement

A consumer failure must not permanently remove unprocessed work.

## Required Behavior

Receiving a message should transfer temporary ownership
to a consumer instead of permanently deleting the message.

A message should become permanently removable only after
successful acknowledgement.

## F2 — Consumer crashes while message is IN_FLIGHT

Initial state:

READY = [M1]

Sequence:

1. Consumer C1 calls `receive()`.
2. M1 moves from READY to IN_FLIGHT.
3. C1 crashes before calling `ack()`.
4. No consumer can receive M1 again.

Result:

The message is not lost, but it is stuck indefinitely.

## Violated Requirement

Unacknowledged work must eventually become available again.

## Required Behavior

Ownership of an IN_FLIGHT message must be temporary.

If acknowledgement does not arrive before the ownership period
expires, the message must return to READY.

## F3 — Stale consumer acknowledgement

Initial state:

M1 is delivered to C1 with receipt handle R1.

Sequence:

1. C1 receives M1 using R1.
2. C1 stops making progress.
3. R1's lease expires.
4. M1 returns to READY.
5. C2 receives M1 with a new receipt handle R2.
6. C1 wakes up and tries to acknowledge R1.

Required behavior:

R1 must no longer be valid.

C1's acknowledgement must not remove C2's active delivery.

Invariant:

Only the current active delivery may be acknowledged.

## F4 — Poison message retries forever

Sequence:

1. M1 is delivered.
2. Processing fails.
3. Lease expires.
4. M1 is redelivered.
5. Processing fails again.
6. The cycle repeats indefinitely.

Result:

The message consumes queue capacity and worker effort forever.

Required behavior:

Retries must be bounded.

After the configured maximum delivery attempts,
the message must leave the normal READY/IN_FLIGHT lifecycle
and move to DEAD_LETTER.