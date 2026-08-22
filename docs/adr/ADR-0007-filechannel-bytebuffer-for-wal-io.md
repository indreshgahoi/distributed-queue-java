# ADR-003: Use FileChannel and ByteBuffer for WAL I/O

## Status

Accepted

## Context

The queue uses a Write-Ahead Log (WAL) to durably record state-transition
decisions before applying them to the in-memory queue state.

Starting with WAL framing, records are stored using the following physical
layout:

    +-------------------+--------------------------+
    | Length            | Serialized WAL Record    |
    | 4 bytes           | N bytes                  |
    +-------------------+--------------------------+

For example:

    [length=120][record-1]
    [length=85 ][record-2]
    [length=96 ][record-3]

The WAL implementation therefore needs precise control over:

1. binary representation of the length prefix;
2. sequential byte-oriented writes;
3. partial writes;
4. exact reads during recovery;
5. file position;
6. durability through explicit flushing to stable storage.

Higher-level APIs such as `FileWriter`, `BufferedWriter`, or
`Files.writeString()` are convenient for text files but hide some of the
byte-level behavior that is important for a WAL.

## Decision

Use Java NIO `FileChannel` together with `ByteBuffer` for WAL file I/O.

A WAL frame is constructed in memory as:

    ByteBuffer
        |
        +-- 4-byte record length
        |
        +-- serialized record bytes

Conceptually:

    WalRecord
        |
        v
    serialize
        |
        v
    byte[]
        |
        v
    ByteBuffer
        |
        +---- putInt(length)
        |
        +---- put(payload)
        |
        v
    FileChannel.write()
        |
        v
    WAL file

Recovery performs the reverse operation:

    FileChannel
        |
        v
    read 4 bytes
        |
        v
    determine N
        |
        v
    read exactly N bytes
        |
        v
    deserialize WalRecord

## Why ByteBuffer?

`FileChannel` operates on bytes through `ByteBuffer`.

`ByteBuffer` also gives us explicit control over the binary representation
of the WAL frame.

For example:

    ByteBuffer frame =
            ByteBuffer.allocate(
                    Integer.BYTES + payload.length
            );

    frame.putInt(payload.length);
    frame.put(payload);
    frame.flip();

This allows the WAL format to contain a fixed-width binary integer followed
by an arbitrary serialized payload.

The buffer also maintains the state required for incremental I/O:

- position
- limit
- capacity

After writing data into the buffer:

    position = end of written data

Calling:

    flip()

changes the buffer from producer/write mode to consumer/read mode:

    position = 0
    limit = previous position

The buffer can then be consumed by `FileChannel`.

## Why FileChannel?

### 1. Precise byte-level I/O

A WAL is fundamentally a sequence of bytes rather than a text document.

`FileChannel` lets the implementation explicitly read and write framed
binary data.

This is important for:

    [4-byte length][N-byte payload]

rather than relying on textual delimiters such as newline characters.

### 2. Explicit durability control

The WAL must establish the durable record before the queue mutates its
volatile state.

The queue depends on the ordering:

    WAL append
        |
        v
    durable
        |
        v
    mutate queue state

`FileChannel` provides:

    channel.force(true)

which requests that updates be forced to the underlying storage device.

This makes the durability boundary visible in the implementation.

### 3. Partial-write handling

A call to:

    channel.write(buffer)

is not conceptually equivalent to:

    "the entire buffer has been written"

The implementation must account for partial writes:

    while (buffer.hasRemaining()) {
        channel.write(buffer);
    }

Using `ByteBuffer` makes the remaining unwritten region explicit through
the buffer's position and limit.

### 4. Exact recovery reads

Recovery needs to distinguish:

    clean EOF

from:

    partial length prefix

from:

    partial record payload

For example:

    [length=100][only 37 bytes]
                         ^
                         crash

`FileChannel + ByteBuffer` allows recovery to explicitly request and count
the bytes required for each portion of the frame.

### 5. File-position control

`FileChannel` exposes file position directly.

This will be useful as the WAL evolves toward features such as:

- identifying the last valid frame;
- truncating an incomplete tail;
- snapshots;
- WAL segments;
- recovery offsets;
- compaction.

### 6. Fits future binary WAL formats

The current record payload may use a simple textual serialization.

The physical frame is independent:

    [length][payload]

Later the payload encoding can change to:

- custom binary encoding;
- Protobuf;
- another versioned codec;

without changing the basic `FileChannel + ByteBuffer` I/O model.

## Why Not FileWriter / BufferedWriter?

These APIs are optimized for character-oriented output.

For example:

    writer.write(record);
    writer.newLine();

is appropriate for human-readable text files.

Our WAL requires:

    binary length
    +
    arbitrary record bytes

Using a character writer would require additional encoding and would make
the physical framing less explicit.

## Why Not Files.writeString()?

`Files.writeString()` is convenient for simple whole-operation text writes,
but the WAL needs lower-level control over:

- framing;
- incremental reads;
- partial reads/writes;
- file position;
- explicit force semantics.

Therefore the convenience of `Files.writeString()` does not outweigh the
loss of control for this storage component.

## Consequences

### Positive

- Explicit binary framing.
- Precise record boundaries.
- Explicit durability boundary through `force()`.
- Partial reads and writes can be handled correctly.
- Better foundation for corruption and truncation detection.
- Natural evolution toward binary WAL formats.
- Direct control over file offsets and recovery.

### Negative

- More code than higher-level file APIs.
- Buffer state (`position`, `limit`, `flip`) must be understood correctly.
- Partial reads and writes must be handled explicitly.
- Incorrect buffer manipulation can introduce subtle storage bugs.
- This does not by itself guarantee atomic disk writes.

## Important Non-Guarantee

Using `FileChannel` and `ByteBuffer` does NOT make a WAL record atomic.

For example, a crash may still leave:

    [length=100][37 bytes written...]
                              ^
                              crash

Framing allows recovery to detect this condition.

Crash recovery policy is a separate concern.

## Mental Model

`ByteBuffer` answers:

    "Which bytes are ready to be transferred?"

`FileChannel` answers:

    "Where are those bytes transferred to/from?"

Framing answers:

    "Where does one WAL record end and the next begin?"

`force()` answers:

    "Where is our durability boundary?"

Together:

    Logical WalRecord
          |
          | serialize
          v
       byte[]
          |
          | frame
          v
      ByteBuffer
          |
          | channel.write()
          v
      FileChannel
          |
          | force()
          v
        Storage