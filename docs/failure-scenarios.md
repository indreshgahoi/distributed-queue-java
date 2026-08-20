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