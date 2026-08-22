package io.github.indreshgahoi.queue.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public final class FileWriteAheadLog
        implements WriteAheadLog {

    /*
     * Physical WAL frame:
     *
     * +----------------------+----------------------+
     * | payload length       | serialized record    |
     * | 4 bytes              | N bytes              |
     * +----------------------+----------------------+
     */
    private static final int LENGTH_PREFIX_BYTES =
            Integer.BYTES;

    /*
     * Defensive upper bound.
     *
     * WAL bytes come from persistent storage and must
     * not be blindly trusted during recovery.
     */
    private static final int MAX_RECORD_SIZE =
            16 * 1024 * 1024;

    private static final String WRITE_SEPARATOR = "|";
    private static final String READ_SEPARATOR = "\\|";

    private final Path path;

    /*
     * Used only for append operations.
     */
    private final FileChannel appendChannel;

    /*
     * Small injection seam used by tests to simulate:
     *
     * partial physical write
     *      +
     * IOException
     */
    private final FrameAppender frameAppender;

    /*
     * If append fails, we cannot know how much of the
     * current frame reached the file.
     *
     * Therefore this WAL instance must reject all
     * subsequent appends.
     */
    private boolean failed;

    private boolean closed;

    public FileWriteAheadLog(Path path) {
        this(
                path,
                FileWriteAheadLog::writeAndForceFrame
        );
    }

    /*
     * Package-private constructor for deterministic
     * failure-injection tests.
     */
    FileWriteAheadLog(
            Path path,
            FrameAppender frameAppender
    ) {
        this.path =
                Objects.requireNonNull(
                        path,
                        "path"
                );

        this.frameAppender =
                Objects.requireNonNull(
                        frameAppender,
                        "frameAppender"
                );

        try {
            this.appendChannel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.APPEND
                    );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to open WAL: " + path,
                    e
            );
        }
    }

    @Override
    public synchronized void append(
            WalRecord record
    ) {
        ensureAppendable();

        Objects.requireNonNull(
                record,
                "record"
        );

        byte[] payload =
                serialize(record)
                        .getBytes(StandardCharsets.UTF_8);

        validateRecordSize(payload.length);

        /*
         * Build:
         *
         * [4-byte payload length][payload bytes]
         */
        ByteBuffer frame =
                ByteBuffer.allocate(
                        LENGTH_PREFIX_BYTES
                                + payload.length
                );

        frame.putInt(payload.length);
        frame.put(payload);

        /*
         * Switch from writing into the ByteBuffer
         * to reading from it into FileChannel.
         */
        frame.flip();

        try {
            frameAppender.append(
                    appendChannel,
                    frame
            );

        } catch (IOException e) {

            /*
             * A partial frame may already exist.
             *
             * Do not allow another valid frame to be
             * appended behind it.
             */
            failed = true;

            throw new WalException(
                    "WAL append failed; WAL requires recovery before further writes",
                    e
            );
        }
    }

    @Override
    public synchronized List<WalRecord> readAll() {
        ensureOpen();

        List<WalRecord> records =
                new ArrayList<>();

        /*
         * Recovery needs WRITE access because it may
         * truncate a torn final frame.
         */
        try (
                FileChannel recoveryChannel =
                        FileChannel.open(
                                path,
                                StandardOpenOption.READ,
                                StandardOpenOption.WRITE
                        )
        ) {

            while (true) {

                /*
                 * Known-good boundary.
                 *
                 * If the current frame is incomplete,
                 * truncate back to this position.
                 */
                long frameStart =
                        recoveryChannel.position();

                ByteBuffer lengthBuffer =
                        ByteBuffer.allocate(
                                LENGTH_PREFIX_BYTES
                        );

                ReadResult lengthResult =
                        readFully(
                                recoveryChannel,
                                lengthBuffer
                        );

                /*
                 * No bytes at all:
                 *
                 * clean EOF immediately after the
                 * previous complete frame.
                 */
                if (lengthResult.bytesRead() == 0) {
                    break;
                }

                /*
                 * Example:
                 *
                 * [valid frame][00 00]
                 *
                 * Crash occurred while writing the
                 * 4-byte length prefix.
                 */
                if (!lengthResult.complete()) {

                    truncateTail(
                            recoveryChannel,
                            frameStart
                    );

                    break;
                }

                lengthBuffer.flip();

                int payloadLength =
                        lengthBuffer.getInt();

                /*
                 * A complete but impossible length is
                 * considered corruption, not torn-tail
                 * recovery.
                 */
                validateFrameLength(
                        payloadLength
                );

                ByteBuffer payloadBuffer =
                        ByteBuffer.allocate(
                                payloadLength
                        );

                ReadResult payloadResult =
                        readFully(
                                recoveryChannel,
                                payloadBuffer
                        );

                /*
                 * Example:
                 *
                 * [length = 100][only 25 bytes...]
                 *
                 * EOF occurred before the full frame
                 * payload was written.
                 */
                if (!payloadResult.complete()) {

                    truncateTail(
                            recoveryChannel,
                            frameStart
                    );

                    break;
                }

                payloadBuffer.flip();

                String serialized =
                        StandardCharsets.UTF_8
                                .decode(payloadBuffer)
                                .toString();

                /*
                 * If framing is complete but logical
                 * deserialization fails, treat it as
                 * corruption.
                 *
                 * Do NOT silently truncate.
                 */
                records.add(
                        deserialize(serialized)
                );
            }

            return List.copyOf(records);

        } catch (IOException e) {
            throw new WalException(
                    "Failed to recover WAL: "
                            + path,
                    e
            );
        }
    }

    /*
     * Production frame append.
     */
    private static void writeAndForceFrame(
            FileChannel channel,
            ByteBuffer frame
    ) throws IOException {

        /*
         * FileChannel.write() may perform a partial
         * write, so continue until the entire frame
         * has been consumed.
         */
        while (frame.hasRemaining()) {
            channel.write(frame);
        }

        /*
         * Current durability policy:
         *
         * every WAL record is forced before append()
         * reports success.
         */
        channel.force(true);
    }

    /*
     * Read until:
     *
     * - the requested buffer is full, or
     * - EOF is encountered.
     */
    private static ReadResult readFully(
            FileChannel channel,
            ByteBuffer buffer
    ) throws IOException {

        int totalRead = 0;

        while (buffer.hasRemaining()) {

            int read =
                    channel.read(buffer);

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

    /*
     * Repair an incomplete final frame.
     *
     * lastValidPosition is the beginning of the
     * incomplete frame.
     */
    private static void truncateTail(
            FileChannel channel,
            long lastValidPosition
    ) throws IOException {

        channel.truncate(
                lastValidPosition
        );

        /*
         * Recovery modified the durable WAL,
         * therefore persist the repair.
         */
        channel.force(true);
    }

    private void validateRecordSize(
            int recordSize
    ) {
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

    private void validateFrameLength(
            int payloadLength
    ) {
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

    private void ensureOpen() {
        if (closed) {
            throw new WalException(
                    "WAL is already closed"
            );
        }
    }

    private void ensureAppendable() {
        ensureOpen();

        if (failed) {
            throw new WalException(
                    "WAL is in failed state; close and reopen it to recover"
            );
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        try {
            appendChannel.close();

        } catch (IOException e) {
            throw new WalException(
                    "Failed to close WAL: "
                            + path,
                    e
            );
        }
    }

    /*
     * Logical WAL-record serialization.
     *
     * This is independent from physical framing.
     *
     * Physical format:
     *
     * [length][serialized record]
     */
    private String serialize(
            WalRecord record
    ) {
        return record.type().name()
                + WRITE_SEPARATOR
                + encode(record.messageId())
                + WRITE_SEPARATOR
                + encode(record.payload())
                + WRITE_SEPARATOR
                + encode(record.receiptHandle())
                + WRITE_SEPARATOR
                + record.attempt()
                + WRITE_SEPARATOR
                + record.timestamp().toEpochMilli();
    }

    private WalRecord deserialize(
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
            WalRecordType type =
                    WalRecordType.valueOf(
                            parts[0]
                    );

            String messageId =
                    decode(parts[1]);

            String payload =
                    decode(parts[2]);

            String receiptHandle =
                    decode(parts[3]);

            int attempt =
                    Integer.parseInt(
                            parts[4]
                    );

            Instant timestamp =
                    Instant.ofEpochMilli(
                            Long.parseLong(
                                    parts[5]
                            )
                    );

            return new WalRecord(
                    type,
                    messageId,
                    payload,
                    receiptHandle,
                    attempt,
                    timestamp
            );

        } catch (RuntimeException e) {
            throw new WalException(
                    "Unable to deserialize WAL record",
                    e
            );
        }
    }

    /*
     * Base64 protects our delimiter-based logical
     * record representation.
     *
     * It is encoding, not encryption.
     */
    private String encode(
            String value
    ) {
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

    private String decode(
            String encoded
    ) {
        if (encoded.isEmpty()) {
            return null;
        }

        return new String(
                Base64.getUrlDecoder()
                        .decode(encoded),
                StandardCharsets.UTF_8
        );
    }

    /*
     * Represents the outcome of attempting to fill
     * a ByteBuffer.
     */
    record ReadResult(
            int bytesRead,
            boolean complete
    ) {
    }

    /*
     * Failure-injection boundary.
     *
     * Production:
     *
     * write entire frame + force
     *
     * Tests:
     *
     * write part of frame + throw IOException
     */
    @FunctionalInterface
    interface FrameAppender {

        void append(
                FileChannel channel,
                ByteBuffer frame
        ) throws IOException;
    }
}