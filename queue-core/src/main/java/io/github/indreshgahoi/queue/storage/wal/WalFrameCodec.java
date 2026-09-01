package io.github.indreshgahoi.queue.storage.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.zip.CRC32C;

/*
 * Owns the physical WAL frame format and the logical WalRecord encoding.
 *
 * Frame layout:
 *
 * +----------------------+----------------------+----------------+
 * | payload length       | serialized record    | CRC32C         |
 * | 4 bytes              | N bytes              | 4 bytes        |
 * +----------------------+----------------------+----------------+
 */
final class WalFrameCodec {

    private static final int LENGTH_PREFIX_BYTES =
            Integer.BYTES;

    private static final int CHECKSUM_BYTES =
            Integer.BYTES;

    private static final int MAX_RECORD_SIZE =
            16 * 1024 * 1024;

    private static final String WRITE_SEPARATOR = "|";
    private static final String READ_SEPARATOR = "\\|";

    ByteBuffer encode(WalRecord record) {
        Objects.requireNonNull(
                record,
                "record"
        );

        byte[] payload =
                serialize(record)
                        .getBytes(StandardCharsets.UTF_8);

        validateRecordSize(payload.length);

        ByteBuffer frame =
                ByteBuffer.allocate(
                        LENGTH_PREFIX_BYTES
                                + payload.length
                                + CHECKSUM_BYTES
                );

        frame.putInt(payload.length);
        frame.put(payload);
        frame.putInt(calculateChecksum(payload));
        frame.flip();

        return frame;
    }

    DecodedFrame readNext(
            FileChannel channel
    ) throws IOException {
        Objects.requireNonNull(
                channel,
                "channel"
        );

        long frameStart = channel.position();

        ByteBuffer lengthBuffer =
                ByteBuffer.allocate(
                        LENGTH_PREFIX_BYTES
                );

        ReadResult lengthResult =
                readFully(
                        channel,
                        lengthBuffer
                );

        if (lengthResult.bytesRead() == 0) {
            return DecodedFrame.cleanEof(
                    frameStart
            );
        }

        if (!lengthResult.complete()) {
            return DecodedFrame.tornLength(
                    frameStart
            );
        }

        lengthBuffer.flip();

        int payloadLength = lengthBuffer.getInt();
        validateFrameLength(payloadLength);

        ByteBuffer payloadBuffer =
                ByteBuffer.allocate(
                        payloadLength
                );

        ReadResult payloadResult =
                readFully(
                        channel,
                        payloadBuffer
                );

        if (!payloadResult.complete()) {
            return DecodedFrame.tornPayload(
                    frameStart
            );
        }

        ByteBuffer checksumBuffer =
                ByteBuffer.allocate(
                        CHECKSUM_BYTES
                );

        ReadResult checksumResult =
                readFully(
                        channel,
                        checksumBuffer
                );

        if (!checksumResult.complete()) {
            return DecodedFrame.tornChecksum(
                    frameStart
            );
        }

        verifyChecksum(
                payloadBuffer.array(),
                checksumBuffer,
                frameStart
        );

        String serialized =
                new String(
                        payloadBuffer.array(),
                        StandardCharsets.UTF_8
                );

        return DecodedFrame.complete(
                frameStart,
                deserialize(serialized)
        );
    }

    private static String serialize(WalRecord record) {
        return record.type().name()
                + WRITE_SEPARATOR
                + encodeNullable(record.messageId())
                + WRITE_SEPARATOR
                + encodeNullable(record.payload())
                + WRITE_SEPARATOR
                + encodeNullable(record.receiptHandle())
                + WRITE_SEPARATOR
                + record.attempt()
                + WRITE_SEPARATOR
                + record.timestamp().toEpochMilli();
    }

    private static WalRecord deserialize(
            String serialized
    ) {
        String[] parts =
                serialized.split(
                        READ_SEPARATOR,
                        -1
                );

        if (parts.length != 6) {
            throw new WalException(
                    "Invalid WAL record: expected 6 fields but found "
                            + parts.length
            );
        }

        try {
            return new WalRecord(
                    WalRecordType.valueOf(parts[0]),
                    decodeNullable(parts[1]),
                    decodeNullable(parts[2]),
                    decodeNullable(parts[3]),
                    Integer.parseInt(parts[4]),
                    Instant.ofEpochMilli(
                            Long.parseLong(parts[5])
                    )
            );

        } catch (RuntimeException e) {
            throw new WalException(
                    "Unable to deserialize WAL record",
                    e
            );
        }
    }

    /*
     * Base64 protects the delimiter-based logical representation.
     * It is encoding, not encryption.
     */
    private static String encodeNullable(String value) {
        if (value == null) {
            return "";
        }

        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        value.getBytes(
                                StandardCharsets.UTF_8
                        )
                );
    }

    private static String decodeNullable(String encoded) {
        if (encoded.isEmpty()) {
            return null;
        }

        return new String(
                Base64.getUrlDecoder()
                        .decode(encoded),
                StandardCharsets.UTF_8
        );
    }

    private static void verifyChecksum(
            byte[] payload,
            ByteBuffer checksumBuffer,
            long frameStart
    ) {
        checksumBuffer.flip();

        int storedChecksum = checksumBuffer.getInt();
        int calculatedChecksum = calculateChecksum(payload);

        if (calculatedChecksum != storedChecksum) {
            throw new WalException(
                    "WAL checksum mismatch at offset "
                            + frameStart
            );
        }
    }

    private static int calculateChecksum(byte[] payload) {
        CRC32C crc32c = new CRC32C();

        crc32c.update(
                payload,
                0,
                payload.length
        );

        return (int) crc32c.getValue();
    }

    private static void validateRecordSize(int recordSize) {
        if (recordSize <= 0) {
            throw new WalException(
                    "WAL record payload must not be empty"
            );
        }

        if (recordSize > MAX_RECORD_SIZE) {
            throw new WalException(
                    "WAL record exceeds maximum size: "
                            + recordSize
            );
        }
    }

    private static void validateFrameLength(int payloadLength) {
        if (payloadLength <= 0) {
            throw new WalException(
                    "Invalid WAL frame length: "
                            + payloadLength
            );
        }

        if (payloadLength > MAX_RECORD_SIZE) {
            throw new WalException(
                    "WAL frame exceeds maximum size: "
                            + payloadLength
            );
        }
    }

    private static ReadResult readFully(
            FileChannel channel,
            ByteBuffer buffer
    ) throws IOException {
        int totalRead = 0;

        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);

            if (read == -1) {
                break;
            }

            totalRead += read;
        }

        return new ReadResult(
                totalRead,
                !buffer.hasRemaining()
        );
    }

    private record ReadResult(
            int bytesRead,
            boolean complete
    ) {
    }
}
