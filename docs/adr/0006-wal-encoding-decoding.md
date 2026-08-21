# ADR-0006: Use Base64 Encoding for String Fields in the Initial WAL Format

## Status

Accepted

## Context

The initial WAL uses a newline-delimited text representation with `|` as the
field separator.

A WAL record is conceptually:

TYPE | messageId | payload | receiptHandle | attempt | timestamp

Message payloads and identifiers are arbitrary strings.

These strings may contain:

- the field separator `|`
- newline characters
- Unicode characters
- other values that could interfere with parsing

The WAL therefore requires an unambiguous representation of string fields.

## Decision

Encode nullable string fields using URL-safe Base64 without padding before
writing them to the WAL.

Decode those fields during WAL recovery.

The structural WAL fields such as record type, attempt number and timestamp
remain directly represented as text.

## Rationale

Base64 keeps the initial WAL implementation simple while ensuring that arbitrary
UTF-8 string content cannot be confused with WAL delimiters or record
boundaries.

The project currently prioritizes correctness and explicit WAL mechanics over
storage efficiency and human readability.

## Alternatives Considered

### Escape Special Characters

For example:

- `|` -> `\|`
- newline -> `\n`
- backslash -> `\\`

Not selected because escaping introduces additional parsing rules and edge cases.
The escape character itself must also be escaped, and malformed input becomes
harder to reason about.

### JSON Lines

Example:

{"type":"PUBLISH","messageId":"...","payload":"..."}

Advantages:

- readable
- naturally handles escaping
- easier schema evolution

Not selected for the initial implementation because it would introduce either
a JSON library dependency or additional serialization code and would make the
WAL format less focused on the underlying durability mechanics.

### Length-Prefixed Binary Records

Example:

[length][type][payload...]

Advantages:

- robust framing
- efficient parsing
- arbitrary binary payloads
- suitable for checksums and versioning

Not selected yet because it introduces more binary-format complexity than is
needed for the first WAL implementation.

This is a likely future evolution when partial-write detection and corruption
handling are introduced.

### Protobuf / Avro

Advantages:

- explicit schemas
- compact representation
- schema evolution support

Not selected because these formats introduce external dependencies and schema
tooling before the project requires them.

## Consequences

Positive:

- arbitrary UTF-8 string content is safe inside the delimiter-based WAL format
- serialization remains deterministic
- implementation remains dependency-free
- parsing logic is straightforward

Negative:

- Base64 increases encoded size by approximately one third
- WAL files are less directly human-readable
- null and empty-string representation must be defined carefully

## Important Limitation

Base64 provides encoding only.

It does not provide:

- encryption
- authentication
- integrity verification
- corruption detection

Future WAL versions may introduce record framing and checksums independently of
this encoding decision.

## Revisit When

Revisit the WAL representation when introducing:

- checksums
- partial-record detection
- WAL versioning
- binary payloads
- compaction
- significant storage-volume optimization