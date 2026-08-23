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

## F5 - Know failure has to wait for the lease to expire before redelivery

Sequence:
1. M1 is delivered to C1.
2. C1 fails in M1 processing
3. M1 has to wait for the lease to expire before it can be redelivered to another consumer.

Result:
 C1 nack(M1) as soon as it knows that M1 processing has failed, but M1 is not redelivered until the lease expires.

Required behavior:
 A consumer should be able to explicitly reject its current delivery using nack() and request retry after a delay/backoff without waiting for the lease to expire.

## F5 — Process crashes after publish

T0  Producer publishes M1
T1  publish() returns success
T2  JVM crashes
T3  JVM restarts

Current behavior:
M1 is gone.

Required behavior:
If publish() returned success, M1 must be recoverable
after restart.
> A state transition must not be reported successful until the corresponding durable record has been written according to the queue's durability contract.

## F6 — NACKed Message Loses Its Retry Schedule After Restart

### Initial State

M1 is currently IN_FLIGHT on delivery attempt 1.

```text
M1
state = IN_FLIGHT
attempt = 1
receiptHandle = R1
```
Sequence
1. Consumer processing fails.
2. Consumer calls:
   nack(R1, 30 seconds)
3. The queue moves M1 to:
   DELAYED
   nextAttempt = 2
   retryAt = 10:00:30
4. nack() returns success.
5. The JVM crashes at 10:00:10.
6. The queue restarts at 10:00:15.
   Current Behavior
   If only PUBLISH and ACK are durable, recovery sees:
   PUBLISH M1
   but does not know that M1 was successfully NACKed.
   Recovery therefore reconstructs:
   M1 -> READY
   immediately.
   **Problem**
   The restart changes externally visible queue behavior.
   M1 becomes eligible before its previously accepted retry time.
   This violates the successful NACK contract.
   **Violated Requirement**
   A successful state transition that changes future message eligibility must
   survive restart.
   **Required Behavior**
   The NACK transition must be durably recorded before nack() returns success.
   The durable record must contain enough information to reconstruct:
   message identity
   next delivery attempt
   absolute retry eligibility time
   After restart:
   now < retryAt
   -> remain DELAYED

   now >= retryAt
   -> eligible to move to READY

## F7 — NACK WAL Failure After In-Memory Removal
Sequence
1. M1 is IN_FLIGHT with receipt handle R1.
2. nack(R1, 30s) begins.
3. The implementation removes R1 from IN_FLIGHT.
4. WAL append fails.
5. nack() throws an exception.    
   **Incorrect Result**  
   M1 is no longer IN_FLIGHT, but the NACK was not durable.  
   The queue has lost the active ownership state despite the operation failing.  
   **Required Ordering**  
   validate receipt handle    
   |  
   calculate retryAt  
   |  
   append NACK WAL record    
   |  
   WAL succeeds    
   |  
   remove IN_FLIGHT    
   |  
   add DELAYED    
   If WAL append fails:    
   IN_FLIGHT must remain unchanged    
   **Invariant**  
   > A failed durable transition must not partially mutate the in-memory queue state.  

## F8 — Retry Delay Is Recalculated Incorrectly After Restart
**Sequence**
10:00:00 nack(M1, 30 seconds)  
10:00:10 JVM crashes  
10:00:20 JVM restarts  
**Incorrect Recovery**  
If the WAL stores only:  
retryDelay = 30 seconds 
and recovery calculates: 
retryAt = restartTime + retryDelay  
then M1 becomes eligible at:  
10:00:50  
**Correct Recovery**  
The original accepted retry time was:  
10:00:30  
Therefore the WAL must preserve:  
retryAt = 10:00:30  
**Mental Model**  
Persist decisions, not enough information to accidentally make a different
decision during recovery.

## F9 — Lease Expiry Attempt Is Lost After Restart

### Sequence

1. M1 is published.
2. M1 is delivered as attempt 1 with receipt R1.
3. R1's visibility lease expires.
4. `requeueExpiredMessages()` successfully requeues M1.
5. M1 is now READY for attempt 2.
6. JVM crashes.
7. Queue restarts.

### Current Durable History

PUBLISH M1

### Incorrect Recovery

M1 -> READY(attempt=1)

### Required Recovery

M1 -> READY(attempt=2)

### Root Cause

The lease-expiry transition exists only in volatile memory.

### Required Fix

Persist the lease-expiry transition before changing the in-memory state.

## F10 — WAL Failure During Lease Expiry

### Sequence

M1 = IN_FLIGHT(R1, attempt=1)
|
lease expires
|
LEASE_EXPIRED WAL append fails

### Required Result

M1 must remain IN_FLIGHT under R1.

The queue must not expose a partially completed transition.

### Invariant

A failed durable state transition must leave the previous runtime state intact.

## F11 — Crash During WAL Record Write

### Sequence

1. PUBLISH M1 is durably written.
2. PUBLISH M2 is durably written.
3. PUBLISH M3 begins.
4. The process crashes after only part of M3 reaches the WAL.

The file may contain:

    [M1 complete]
    [M2 complete]
    [M3 partial]

### Risk

Without explicit framing, recovery may be unable to determine whether
the final bytes represent:

- a valid record,
- an incomplete record,
- or corrupted data.

### Required Direction

Each WAL entry must carry an explicit record length.

Recovery can therefore distinguish:

    complete frame
        vs
    incomplete trailing frame

F13 — New code interprets an old WAL using the wrong physical format

```text
Old file:
[length][payload]

New decoder expects:
[length][payload][checksum]
```

Without versioning:
bytes may be misinterpreted as valid framing/checksum data.

Required behavior:
detect unsupported format before record recovery begins.

The key mental model is:
> Data formats are APIs too. Once bytes can survive longer than the process that wrote them, format compatibility becomes an architectural concern.


## F14 — Unbounded WAL Growth

### Sequence

The queue runs for a long period.

Messages are continuously:

- published
- acknowledged
- retried
- dead-lettered

Most historical messages are already DONE, but their WAL records remain.

### Result

The WAL grows monotonically.

Recovery time becomes proportional to total historical operations rather
than current queue state.

Disk consumption also grows without bound.

### Required Direction

Periodically persist a snapshot of the current logical queue state.

Once a snapshot is safely durable, WAL history represented by that snapshot
may eventually be compacted or discarded according to a defined policy.

## F15 — Delivery Lease Is Lost After Queue Restart

### Initial State

M1 is READY.

### Sequence

1. Consumer calls `receive()`.
2. Queue removes M1 from READY.
3. Queue creates:

       receiptHandle = R1
       attempt = 1
       leaseUntil = 10:30

4. M1 becomes IN_FLIGHT.
5. Delivery is returned to the consumer.
6. Queue process crashes at 10:10.
7. Queue restarts at 10:12.

### Current Failure Without Durable Delivery State

If the WAL contains only:

    PUBLISH M1

recovery reconstructs:

    M1 -> READY

The same message can therefore be delivered again at 10:12 even though the
original lease was valid until 10:30.

### Problem

The queue forgot an externally visible ownership decision.

Restart changed delivery semantics.

### Required Behavior

Before exposing the delivery, persist:

    LEASE_STARTED
    messageId = M1
    receiptHandle = R1
    attempt = 1
    leaseUntil = 10:30

Recovery must restore:

    M1 -> IN_FLIGHT(R1)

until the lease terminates normally.


## F16 — Consumer Cannot ACK After Queue Restart

### Sequence

1. Consumer receives M1 with receipt handle R1.
2. Consumer processes M1 successfully.
3. Queue process restarts before ACK reaches the queue.
4. Consumer retries:

       ack(R1)

### Incorrect Policy

If queue restart invalidates all receipt handles:

    ack(R1) -> false

Eventually the lease expires and M1 is redelivered despite successful
processing.

### Required Behavior

Queue restart alone must not invalidate R1.

If the recovered lease is still active:

    ack(R1) -> true

Receipt-handle validity is tied to the delivery lease, not the lifetime of
one JVM process.


## F17 — receive() Exposes Ownership Before It Is Durable

### Incorrect Ordering

    remove M1 from READY
        |
        v
    create R1
        |
        v
    return Delivery
        |
        v
    append LEASE_STARTED

If the process crashes after returning the delivery but before the WAL append,
the consumer believes it owns M1 while recovery believes M1 is READY.

### Required Ordering

    identify READY M1
        |
        v
    create R1 + leaseUntil
        |
        v
    append LEASE_STARTED
        |
        v
    cross durability boundary
        |
        v
    READY -> IN_FLIGHT
        |
        v
    return Delivery

### Invariant

Externally visible ownership must never exist only in volatile memory.


## F18 — LEASE_STARTED WAL Failure Partially Changes Queue State

### Sequence

1. M1 is READY.
2. `receive()` begins.
3. Implementation removes M1 from READY.
4. LEASE_STARTED WAL append fails.

### Incorrect Result

M1 is no longer READY but no durable delivery lease exists.

The message may effectively disappear until another repair path detects it.

### Required Result

If LEASE_STARTED cannot be made durable:

    M1 remains READY
    no active receipt handle exists
    receive() does not return a Delivery

### Principle

A failed durable transition must leave the previous authoritative state intact.