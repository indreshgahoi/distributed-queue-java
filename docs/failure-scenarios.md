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