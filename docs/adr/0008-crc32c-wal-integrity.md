# ADR-0007: Use CRC32C for WAL Record Integrity

## Status

Accepted

## Context

The WAL already uses explicit framing:

    [payload-length][payload]

Framing allows recovery to detect incomplete records such as:

    [length = 100][only 30 payload bytes]
                         ^
                         crash

However, framing alone cannot detect silent corruption when the frame is
structurally complete.

For example:

    Original:

    [length = 100][... A B C D ...]

    Corrupted:

    [length = 100][... A X C D ...]
                         ^

Recovery still sees:

    valid length
    complete payload

Without an integrity check, corrupted bytes may be interpreted as a valid
WalRecord.

The WAL therefore needs an integrity mechanism that can detect accidental
byte corruption before deserialization.

## Decision

Add a CRC32C checksum to every WAL frame.

The physical frame becomes:

    +------------------+----------------------+------------------+
    | Payload Length   | Payload              | CRC32C           |
    | 4 bytes          | N bytes              | 4 bytes          |
    +------------------+----------------------+------------------+

The checksum is calculated over the serialized payload bytes only.

Write path:

    WalRecord
        |
        v
    serialize
        |
        v
    payload bytes
        |
        +--> CRC32C(payload)
        |
        v
    [length][payload][checksum]
        |
        v
    FileChannel.write()
        |
        v
    force()

Recovery path:

    read length
        |
        v
    read payload
        |
        v
    read stored checksum
        |
        v
    calculate CRC32C(payload)
        |
        v
    compare
       / \
      /   \
    match  mismatch
      |       |
      v       v
deserialize WalException

## Why CRC32C?

CRC32C is intended for detecting accidental data corruption.

Java provides a built-in implementation:

    java.util.zip.CRC32C

This keeps the WAL implementation dependency-free.

CRC32C is also commonly suited to storage and transport integrity checks.

The project does not currently require cryptographic integrity.

## Why Not CRC32?

CRC32 would also detect many accidental bit errors.

CRC32C was selected because it is a modern CRC variant commonly used for
storage-oriented integrity checking and is directly available in the JDK.

This project has not benchmarked CRC32 versus CRC32C performance.

Therefore this decision is based on integrity semantics and implementation
simplicity, not measured performance.

## Why Not SHA-256?

SHA-256 provides cryptographic integrity properties and much stronger collision
resistance.

That is unnecessary for the current WAL threat model.

The WAL is protecting against:

- accidental byte corruption
- storage faults
- damaged record contents

It is not currently designed to protect against a malicious actor deliberately
modifying WAL contents.

Using SHA-256 would add complexity without solving a current requirement.

## Why Not Rely Only on Framing?

Framing detects structural incompleteness.

For example:

    [length=100][30 bytes]
                   ^
                   incomplete

But framing cannot detect:

    [length=100][100 corrupted bytes]

because the structure remains valid.

Therefore:

    framing
        !=
    integrity verification

Both are required for different failure modes.

## Recovery Semantics

A checksum mismatch is considered corruption.

Recovery MUST fail with WalException.

The WAL MUST NOT silently truncate a structurally complete frame only because
its checksum is invalid.

This is intentionally different from torn-tail recovery.

### Torn Final Frame

Example:

    [valid][valid][partial]
                     ^
                     EOF

Policy:

    recover valid prefix
    truncate incomplete final frame

### Complete Frame with Invalid Checksum

Example:

    [valid][complete but corrupted][valid...]

Policy:

    fail recovery

The system does not have enough evidence to safely assume that the corrupted
frame is merely an interrupted final append.

## Ordering

Checksum verification occurs before deserialization.

Required recovery order:

    read payload
        |
        v
    read checksum
        |
        v
    verify checksum
        |
        v
    deserialize

Corrupted bytes must not first be interpreted as a logical WalRecord.

## Consequences

### Positive

- Detects accidental payload corruption.
- Prevents silently recovering corrupted queue state.
- Integrity verification happens before deserialization.
- Uses only JDK APIs.
- Fits naturally into the existing framed WAL design.

### Negative

- Adds 4 bytes to every WAL frame.
- Requires checksum calculation for every append.
- Requires checksum verification during recovery.
- Adds another failure mode that operators must understand.
- Does not provide cryptographic integrity.

## Performance Status

CRC calculation introduces additional work on the write and recovery paths.

The impact has not been measured in this project.

No throughput or latency claim should be made until benchmarks exist.

## Non-Goals

This decision does not provide:

- encryption
- authentication
- protection from malicious modification
- replication
- end-to-end application-level checksums
- protection from all hardware or filesystem failures

## Invariants

A valid complete WAL frame is:

    [length][payload][CRC32C(payload)]

Recovery accepts the frame only when:

    storedChecksum == CRC32C(payload)

An incomplete final frame may be repaired using torn-tail recovery.

A complete frame with checksum mismatch is corruption and causes recovery to
fail.

## Mental Model

Framing answers:

    "Do I have the complete record?"

Checksum answers:

    "Are these the same bytes that were written?"

Durability answers:

    "Did we cross the persistence boundary before reporting success?"

These are three independent concerns:

    Durability
        |
        +-- was the decision persisted?

    Framing
        |
        +-- where does the record begin/end?

    Checksum
        |
        +-- are the recovered bytes intact?