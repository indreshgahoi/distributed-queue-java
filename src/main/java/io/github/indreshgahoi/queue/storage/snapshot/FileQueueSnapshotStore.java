package io.github.indreshgahoi.queue.storage.snapshot;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

public final class FileQueueSnapshotStore
        implements QueueSnapshotStore {

    /*
     * "DQSN"
     *
     * Distributed Queue Snapshot
     */
    private static final int SNAPSHOT_MAGIC =
            0x4451534E;

    private static final int SNAPSHOT_VERSION =
            1;

    private static final int CHECKSUM_BYTES =
            Integer.BYTES;

    private static final int HEADER_BYTES =
            Integer.BYTES   // magic
                    + Integer.BYTES; // version

    private static final int MAX_SNAPSHOT_SIZE =
            128 * 1024 * 1024;

    private static final String SEPARATOR = "|";
    private static final String READ_SEPARATOR = "\\|";

    private final Path snapshotPath;
    private final Path tempPath;

    public FileQueueSnapshotStore(
            Path snapshotPath
    ) {
        this.snapshotPath =
                Objects.requireNonNull(
                        snapshotPath,
                        "snapshotPath"
                );

        this.tempPath =
                snapshotPath.resolveSibling(
                        snapshotPath.getFileName()
                                + ".tmp"
                );
    }

    @Override
    public synchronized void save(
            QueueSnapshot snapshot
    ) {
        Objects.requireNonNull(
                snapshot,
                "snapshot"
        );

        byte[] payload =
                serialize(snapshot)
                        .getBytes(
                                StandardCharsets.UTF_8
                        );

        if (payload.length > MAX_SNAPSHOT_SIZE) {
            throw new SnapshotException(
                    "Snapshot exceeds maximum size: "
                            + payload.length
            );
        }

        int checksum =
                calculateChecksum(payload);

        /*
         * Physical snapshot format:
         *
         * [magic]
         * [version]
         * [payloadLength]
         * [payload]
         * [checksum]
         */
        ByteBuffer buffer =
                ByteBuffer.allocate(
                        HEADER_BYTES
                                + Integer.BYTES
                                + payload.length
                                + CHECKSUM_BYTES
                );

        buffer.putInt(
                SNAPSHOT_MAGIC
        );

        buffer.putInt(
                SNAPSHOT_VERSION
        );

        buffer.putInt(
                payload.length
        );

        buffer.put(
                payload
        );

        buffer.putInt(
                checksum
        );

        buffer.flip();

        try {
            /*
             * IMPORTANT:
             *
             * Do not overwrite snapshotPath directly.
             *
             * The previous snapshot remains valid while
             * the new snapshot is being created.
             */
            writeTempSnapshot(
                    buffer
            );

            replaceSnapshotAtomically();

        } catch (IOException e) {

            /*
             * Best effort cleanup.
             *
             * Existing snapshotPath must remain untouched.
             */
            try {
                Files.deleteIfExists(
                        tempPath
                );
            } catch (IOException ignored) {
                // Preserve original failure.
            }

            throw new SnapshotException(
                    "Failed to save queue snapshot",
                    e
            );
        }
    }

    @Override
    public synchronized Optional<QueueSnapshot> loadLatest() {
        if (!Files.exists(snapshotPath)) {
            return Optional.empty();
        }

        try (
                FileChannel channel =
                        FileChannel.open(
                                snapshotPath,
                                StandardOpenOption.READ
                        )
        ) {
            /*
             * Read fixed prefix:
             *
             * magic
             * version
             * payloadLength
             */
            ByteBuffer prefix =
                    ByteBuffer.allocate(
                            HEADER_BYTES
                                    + Integer.BYTES
                    );

            readFully(
                    channel,
                    prefix,
                    "Incomplete snapshot header"
            );

            prefix.flip();

            int magic =
                    prefix.getInt();

            int version =
                    prefix.getInt();

            int payloadLength =
                    prefix.getInt();

            validateHeader(
                    magic,
                    version,
                    payloadLength
            );

            ByteBuffer payloadBuffer =
                    ByteBuffer.allocate(
                            payloadLength
                    );

            readFully(
                    channel,
                    payloadBuffer,
                    "Incomplete snapshot payload"
            );

            ByteBuffer checksumBuffer =
                    ByteBuffer.allocate(
                            CHECKSUM_BYTES
                    );

            readFully(
                    channel,
                    checksumBuffer,
                    "Incomplete snapshot checksum"
            );

            /*
             * Snapshot must contain no unexpected trailing bytes.
             */
            ByteBuffer extra =
                    ByteBuffer.allocate(1);

            if (channel.read(extra) != -1) {
                throw new SnapshotException(
                        "Snapshot contains unexpected trailing bytes"
                );
            }

            payloadBuffer.flip();
            checksumBuffer.flip();

            byte[] payload =
                    new byte[payloadLength];

            payloadBuffer.get(
                    payload
            );

            int storedChecksum =
                    checksumBuffer.getInt();

            int calculatedChecksum =
                    calculateChecksum(
                            payload
                    );

            if (storedChecksum
                    != calculatedChecksum) {

                throw new SnapshotException(
                        "Snapshot checksum mismatch"
                );
            }

            String serialized =
                    new String(
                            payload,
                            StandardCharsets.UTF_8
                    );

            return Optional.of(
                    deserialize(serialized)
            );

        } catch (IOException e) {
            throw new SnapshotException(
                    "Failed to load queue snapshot",
                    e
            );
        }
    }

    private void writeTempSnapshot(
            ByteBuffer buffer
    ) throws IOException {

        /*
         * CREATE + TRUNCATE is safe here because this
         * is only the temporary candidate snapshot.
         */
        try (
                FileChannel channel =
                        FileChannel.open(
                                tempPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.TRUNCATE_EXISTING
                        )
        ) {
            while (buffer.hasRemaining()) {
                channel.write(
                        buffer
                );
            }

            /*
             * Durability boundary for the candidate snapshot.
             */
            channel.force(true);
        }
    }

    private void replaceSnapshotAtomically()
            throws IOException {

        try {
            Files.move(
                    tempPath,
                    snapshotPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );

        } catch (AtomicMoveNotSupportedException e) {

            /*
             * Fallback.
             *
             * Note:
             * this loses the strong atomic-replacement guarantee,
             * so this limitation should be documented.
             */
            Files.move(
                    tempPath,
                    snapshotPath,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
    }

    private void validateHeader(
            int magic,
            int version,
            int payloadLength
    ) {
        if (magic != SNAPSHOT_MAGIC) {
            throw new SnapshotException(
                    "Invalid snapshot magic"
            );
        }

        if (version != SNAPSHOT_VERSION) {
            throw new SnapshotException(
                    "Unsupported snapshot version: "
                            + version
            );
        }

        if (payloadLength <= 0) {
            throw new SnapshotException(
                    "Invalid snapshot payload length: "
                            + payloadLength
            );
        }

        if (payloadLength > MAX_SNAPSHOT_SIZE) {
            throw new SnapshotException(
                    "Snapshot payload exceeds maximum size: "
                            + payloadLength
            );
        }
    }

    private static void readFully(
            FileChannel channel,
            ByteBuffer buffer,
            String errorMessage
    ) throws IOException {

        while (buffer.hasRemaining()) {

            int read =
                    channel.read(
                            buffer
                    );

            if (read == -1) {
                throw new SnapshotException(
                        errorMessage
                );
            }
        }
    }

    private static int calculateChecksum(
            byte[] payload
    ) {
        CRC32C crc32c =
                new CRC32C();

        crc32c.update(
                payload,
                0,
                payload.length
        );

        return (int) crc32c.getValue();
    }

    // ---------------------------------------------------------------------
    // Logical snapshot serialization
    // ---------------------------------------------------------------------

    private String serialize(
            QueueSnapshot snapshot
    ) {
        StringBuilder builder =
                new StringBuilder();

        /*
         * Snapshot metadata.
         */
        builder.append("POSITION")
                .append(SEPARATOR)
                .append(snapshot.walPosition().segmentId())
                .append(SEPARATOR)
                .append(snapshot.walPosition().offset())
                .append('\n');

        for (ReadySnapshotEntry entry
                : snapshot.ready()) {

            builder.append("READY")
                    .append(SEPARATOR)
                    .append(encode(entry.messageId()))
                    .append(SEPARATOR)
                    .append(encode(entry.payload()))
                    .append(SEPARATOR)
                    .append(entry.nextAttempt())
                    .append('\n');
        }

        for (InFlightSnapshotEntry entry
                : snapshot.inFlight()) {

            builder.append("IN_FLIGHT")
                    .append(SEPARATOR)
                    .append(encode(entry.messageId()))
                    .append(SEPARATOR)
                    .append(encode(entry.payload()))
                    .append(SEPARATOR)
                    .append(encode(entry.receiptHandle()))
                    .append(SEPARATOR)
                    .append(entry.attempt())
                    .append(SEPARATOR)
                    .append(entry.leaseUntil().toEpochMilli())
                    .append('\n');
        }

        for (DelayedSnapshotEntry entry
                : snapshot.delayed()) {

            builder.append("DELAYED")
                    .append(SEPARATOR)
                    .append(encode(entry.messageId()))
                    .append(SEPARATOR)
                    .append(encode(entry.payload()))
                    .append(SEPARATOR)
                    .append(entry.nextAttempt())
                    .append(SEPARATOR)
                    .append(entry.retryAt().toEpochMilli())
                    .append('\n');
        }

        for (DeadLetterSnapshotEntry entry
                : snapshot.deadLetters()) {

            builder.append("DEAD_LETTER")
                    .append(SEPARATOR)
                    .append(encode(entry.messageId()))
                    .append(SEPARATOR)
                    .append(encode(entry.payload()))
                    .append('\n');
        }

        return builder.toString();
    }

    private QueueSnapshot deserialize(
            String serialized
    ) {
        String[] lines =
                serialized.split(
                        "\\n"
                );

        WalPosition position =
                null;

        List<ReadySnapshotEntry> ready =
                new ArrayList<>();

        List<InFlightSnapshotEntry> inFlight =
                new ArrayList<>();

        List<DelayedSnapshotEntry> delayed =
                new ArrayList<>();

        List<DeadLetterSnapshotEntry> deadLetters =
                new ArrayList<>();

        for (String line : lines) {

            if (line.isBlank()) {
                continue;
            }

            String[] parts =
                    line.split(
                            READ_SEPARATOR,
                            -1
                    );

            switch (parts[0]) {

                case "POSITION" -> {

                    requireFieldCount(
                            parts,
                            3,
                            "POSITION"
                    );

                    position =
                            new WalPosition(
                                    Long.parseLong(
                                            parts[1]
                                    ),
                                    Long.parseLong(
                                            parts[2]
                                    )
                            );
                }

                case "READY" -> {

                    requireFieldCount(
                            parts,
                            4,
                            "READY"
                    );

                    ready.add(
                            new ReadySnapshotEntry(
                                    decode(parts[1]),
                                    decode(parts[2]),
                                    Integer.parseInt(
                                            parts[3]
                                    )
                            )
                    );
                }

                case "IN_FLIGHT" -> {

                    requireFieldCount(
                            parts,
                            6,
                            "IN_FLIGHT"
                    );

                    inFlight.add(
                            new InFlightSnapshotEntry(
                                    decode(parts[1]),
                                    decode(parts[2]),
                                    decode(parts[3]),
                                    Integer.parseInt(
                                            parts[4]
                                    ),
                                    Instant.ofEpochMilli(
                                            Long.parseLong(
                                                    parts[5]
                                            )
                                    )
                            )
                    );
                }

                case "DELAYED" -> {

                    requireFieldCount(
                            parts,
                            5,
                            "DELAYED"
                    );

                    delayed.add(
                            new DelayedSnapshotEntry(
                                    decode(parts[1]),
                                    decode(parts[2]),
                                    Integer.parseInt(
                                            parts[3]
                                    ),
                                    Instant.ofEpochMilli(
                                            Long.parseLong(
                                                    parts[4]
                                            )
                                    )
                            )
                    );
                }

                case "DEAD_LETTER" -> {

                    requireFieldCount(
                            parts,
                            3,
                            "DEAD_LETTER"
                    );

                    deadLetters.add(
                            new DeadLetterSnapshotEntry(
                                    decode(parts[1]),
                                    decode(parts[2])
                            )
                    );
                }

                default ->
                        throw new SnapshotException(
                                "Unknown snapshot entry type: "
                                        + parts[0]
                        );
            }
        }

        if (position == null) {
            throw new SnapshotException(
                    "Snapshot does not contain WAL position"
            );
        }

        return new QueueSnapshot(
                position,
                ready,
                inFlight,
                delayed,
                deadLetters
        );
    }

    private static void requireFieldCount(
            String[] parts,
            int expected,
            String type
    ) {
        if (parts.length != expected) {
            throw new SnapshotException(
                    "Invalid "
                            + type
                            + " snapshot entry. Expected "
                            + expected
                            + " fields but found "
                            + parts.length
            );
        }
    }

    private static String encode(
            String value
    ) {
        Objects.requireNonNull(
                value,
                "snapshot value"
        );

        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        value.getBytes(
                                StandardCharsets.UTF_8
                        )
                );
    }

    private static String decode(
            String encoded
    ) {
        try {
            return new String(
                    Base64.getUrlDecoder()
                            .decode(encoded),
                    StandardCharsets.UTF_8
            );

        } catch (IllegalArgumentException e) {
            throw new SnapshotException(
                    "Invalid Base64 snapshot field",
                    e
            );
        }
    }
}