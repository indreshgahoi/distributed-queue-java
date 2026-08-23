package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

class FileWriteAheadLogTest {

    private static final int WAL_HEADER_SIZE =
            Integer.BYTES * 2;

    @TempDir
    Path tempDir;

    @Test
    void appendedRecordCanBeReadBack() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord expected =
                new WalRecord(
                        WalRecordType.PUBLISH,
                        "message-1",
                        "hello",
                        null,
                        1,
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(expected);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            List<WalRecord> records =
                    wal.readAll();

            assertEquals(1, records.size());
            assertEquals(expected, records.getFirst());
        }
    }

    @Test
    void multipleFramedRecordsCanBeReadBackIndependently() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord publish =
                new WalRecord(
                        WalRecordType.PUBLISH,
                        "message-1",
                        "payment-created",
                        null,
                        1,
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        WalRecord nack =
                new WalRecord(
                        WalRecordType.NACK,
                        "message-1",
                        null,
                        "receipt-1",
                        2,
                        Instant.parse("2026-08-22T00:00:10Z")
                );

        WalRecord ack =
                new WalRecord(
                        WalRecordType.ACK,
                        "message-2",
                        null,
                        "receipt-2",
                        1,
                        Instant.parse("2026-08-22T00:00:20Z")
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(publish);
            wal.append(nack);
            wal.append(ack);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(
                            publish,
                            nack,
                            ack
                    ),
                    wal.readAll()
            );
        }
    }

    @Test
    void recordPayloadMayContainDelimiterCharacters() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord expected =
                new WalRecord(
                        WalRecordType.PUBLISH,
                        "message-1",
                        "hello|world|payment",
                        null,
                        1,
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(expected);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    expected,
                    wal.readAll().getFirst()
            );
        }
    }

    @Test
    void recordPayloadMayContainNewLines() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord expected =
                new WalRecord(
                        WalRecordType.PUBLISH,
                        "message-1",
                        """
                                payment
                                created
                                successfully
                                """,
                        null,
                        1,
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(expected);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    expected,
                    wal.readAll().getFirst()
            );
        }
    }

    @Test
    void unicodePayloadRoundTripsCorrectly() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord expected =
                new WalRecord(
                        WalRecordType.PUBLISH,
                        "message-1",
                        "नमस्ते दुनिया 🚀",
                        null,
                        1,
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(expected);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    expected,
                    wal.readAll().getFirst()
            );
        }
    }

    @Test
    void reopeningWalAllowsAdditionalRecordsToBeAppended() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord(
                        "message-1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "message-2",
                        "B"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(second);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(first, second),
                    wal.readAll()
            );
        }
    }

    @Test
    void emptyWalReturnsNoRecords() {
        Path walPath = tempDir.resolve("queue.wal");

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertTrue(
                    wal.readAll().isEmpty()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Framing
    // ---------------------------------------------------------------------

    @Test
    void walRecordLengthPrefixMatchesPayloadLength()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "hello"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        byte[] fileBytes =
                Files.readAllBytes(walPath);

        assertTrue(
                fileBytes.length > Integer.BYTES,
                "WAL must contain a 4-byte frame length and payload"
        );

        ByteBuffer buffer =
                ByteBuffer.wrap(fileBytes);

        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );
        buffer.getInt(); // magic
        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );
        buffer.getInt(); // version

        int declaredLength =
                buffer.getInt();

        int actualPayloadLength =
                fileBytes.length - Integer.BYTES - Integer.BYTES - Integer.BYTES - Integer.BYTES;

        assertEquals(
                actualPayloadLength,
                declaredLength
        );
    }

    @Test
    void multipleRecordsHaveIndependentFramedBoundaries()
            throws IOException {

        Path walPath =
                tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord("message-1", "A");

        WalRecord second =
                publishRecord("message-2", "B");

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
            wal.append(second);
        }

        byte[] bytes =
                Files.readAllBytes(walPath);

        ByteBuffer buffer =
                ByteBuffer.wrap(bytes);



        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );
        buffer.getInt(); // magic
        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );
        buffer.getInt(); // version

        /*
         * Frame 1
         */
        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );

        int firstPayloadLength =
                buffer.getInt();

        assertTrue(firstPayloadLength > 0);

        assertTrue(
                buffer.remaining()
                        >= firstPayloadLength
                        + Integer.BYTES
        );

        /*
         * Skip payload1.
         */
        buffer.position(
                buffer.position()
                        + firstPayloadLength
        );

        /*
         * Skip checksum1.
         */
        buffer.position(
                buffer.position()
                        + Integer.BYTES
        );

        /*
         * Frame 2
         */
        assertTrue(
                buffer.remaining() >= Integer.BYTES
        );

        int secondPayloadLength =
                buffer.getInt();

        assertTrue(secondPayloadLength > 0);

        /*
         * What remains must be exactly:
         *
         * payload2 + checksum2
         */
        assertEquals(
                secondPayloadLength
                        + Integer.BYTES,
                buffer.remaining()
        );
    }

    @Test
    void frameBoundaryDoesNotDependOnNewline()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "line-1\nline-2\nline-3"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        byte[] bytes =
                Files.readAllBytes(walPath);

        ByteBuffer buffer =
                ByteBuffer.wrap(bytes);

        buffer.position(WAL_HEADER_SIZE);

        int frameLength =
                buffer.getInt();

        assertEquals(
                bytes.length
                        - WAL_HEADER_SIZE
                        - Integer.BYTES
                        - Integer.BYTES,
                frameLength
        );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    record,
                    wal.readAll().getFirst()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Torn-tail recovery
    // ---------------------------------------------------------------------

    @Test
    void truncatedLengthPrefixIsRecoveredByTruncatingTail()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord valid =
                publishRecord(
                        "message-1",
                        "A"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(valid);
        }

        long validWalSize =
                Files.size(walPath);

        /*
         * Simulate crash while writing the next
         * 4-byte frame length.
         *
         * Only 2 bytes reached disk.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.APPEND
                     )) {

            ByteBuffer partialLength =
                    ByteBuffer.wrap(
                            new byte[]{
                                    0x00,
                                    0x10
                            }
                    );

            while (partialLength.hasRemaining()) {
                channel.write(partialLength);
            }
        }

        assertTrue(
                Files.size(walPath) > validWalSize
        );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(valid),
                    wal.readAll()
            );
        }

        /*
         * Recovery physically repairs the WAL.
         */
        assertEquals(
                validWalSize,
                Files.size(walPath)
        );
    }

    @Test
    void truncatedFinalFrameIsRecoveredByTruncatingTail()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord valid =
                publishRecord(
                        "message-1",
                        "A"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(valid);
        }

        long validWalSize =
                Files.size(walPath);

        /*
         * Next frame declares 100 payload bytes,
         * but only 5 bytes reach disk.
         */
        appendPartialFrame(
                walPath,
                100,
                new byte[]{
                        1, 2, 3, 4, 5
                }
        );

        assertTrue(
                Files.size(walPath) > validWalSize
        );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(valid),
                    wal.readAll()
            );
        }

        assertEquals(
                validWalSize,
                Files.size(walPath)
        );
    }

    @Test
    void recoveryPreservesValidPrefixAndTruncatesIncompleteTail()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord(
                        "message-1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "message-2",
                        "B"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
            wal.append(second);
        }

        long validWalSize =
                Files.size(walPath);

        appendPartialFrame(
                walPath,
                100,
                new byte[]{
                        1, 2, 3, 4, 5
                }
        );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(
                            first,
                            second
                    ),
                    wal.readAll()
            );
        }

        assertEquals(
                validWalSize,
                Files.size(walPath)
        );
    }

    @Test
    void recoveryTruncatesIncompleteFinalFrameAndAllowsFutureAppend()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord(
                        "message-1",
                        "A"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
        }

        long validWalSize =
                Files.size(walPath);

        appendPartialFrame(
                walPath,
                100,
                new byte[]{
                        1, 2, 3, 4, 5
                }
        );

        assertTrue(
                Files.size(walPath) > validWalSize,
                "Test setup must create a torn WAL tail"
        );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(first),
                    wal.readAll()
            );
        }

        assertEquals(
                validWalSize,
                Files.size(walPath)
        );

        WalRecord third =
                publishRecord(
                        "message-3",
                        "C"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(third);
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(
                            first,
                            third
                    ),
                    wal.readAll()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Corruption
    // ---------------------------------------------------------------------

    @Test
    void invalidNegativeFrameLengthIsRejected()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        try (WriteAheadLog ignored =
                     new FileWriteAheadLog(walPath)) {
        }

        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.APPEND
                     )) {

            ByteBuffer buffer =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            buffer.putInt(-1);
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertThrows(
                    WalException.class,
                    wal::readAll
            );
        }
    }

    // ---------------------------------------------------------------------
    // Failed append / poisoned writer
    // ---------------------------------------------------------------------

    @Test
    void appendFailurePoisonsWalAndPreventsRecordsAfterTornFrame()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord m1 =
                publishRecord(
                        "message-1",
                        "A"
                );

        WalRecord m2 =
                publishRecord(
                        "message-2",
                        "B"
                );

        WalRecord m3 =
                publishRecord(
                        "message-3",
                        "C"
                );

        WalRecord m4 =
                publishRecord(
                        "message-4",
                        "D"
                );

        /*
         * Establish valid prefix:
         *
         * [M1][M2]
         */
        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(m1);
            wal.append(m2);
        }

        FileWriteAheadLog.FrameAppender failingAppender =
                new FileWriteAheadLog.FrameAppender() {

                    private boolean firstCall = true;

                    @Override
                    public void append(
                            FileChannel channel,
                            ByteBuffer frame
                    ) throws IOException {

                        if (!firstCall) {
                            throw new AssertionError(
                                    "Poisoned WAL must not attempt another physical write"
                            );
                        }

                        firstCall = false;

                        /*
                         * Write:
                         *
                         * complete 4-byte frame length
                         * +
                         * only a few payload bytes.
                         */
                        int bytesToWrite =
                                Math.min(
                                        Integer.BYTES + 3,
                                        frame.remaining()
                                );

                        int originalLimit =
                                frame.limit();

                        frame.limit(
                                frame.position()
                                        + bytesToWrite
                        );

                        while (frame.hasRemaining()) {
                            channel.write(frame);
                        }

                        frame.limit(
                                originalLimit
                        );

                        throw new IOException(
                                "Simulated mid-frame write failure"
                        );
                    }
                };

        long sizeAfterM1M2 =
                Files.size(walPath);

        try (
                FileWriteAheadLog wal =
                        new FileWriteAheadLog(
                                walPath,
                                failingAppender
                        )
        ) {

            assertThrows(
                    WalException.class,
                    () -> wal.append(m3)
            );

            long sizeAfterPartialM3 =
                    Files.size(walPath);

            assertTrue(
                    sizeAfterPartialM3 > sizeAfterM1M2,
                    "Test must leave a partial M3 frame on disk"
            );

            /*
             * Once M3 partially fails, this writer is poisoned.
             *
             * M4 must never be physically attempted.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(m4)
            );

            assertEquals(
                    sizeAfterPartialM3,
                    Files.size(walPath),
                    "Rejected M4 must not change the WAL"
            );
        }

        /*
         * A new instance performs torn-tail recovery.
         */
        try (WriteAheadLog recovered =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(
                            m1,
                            m2
                    ),
                    recovered.readAll()
            );

            /*
             * Repaired WAL is now usable.
             */
            recovered.append(m4);
        }

        /*
         * Final history:
         *
         * [M1][M2][M4]
         */
        try (WriteAheadLog reopened =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(
                            m1,
                            m2,
                            m4
                    ),
                    reopened.readAll()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Corruption / Checksum
    // ---------------------------------------------------------------------
    @Test
    void walFrameContainsChecksum()
            throws IOException {

        Path walPath =
                tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "hello"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        byte[] bytes =
                Files.readAllBytes(walPath);

        ByteBuffer buffer =
                ByteBuffer.wrap(bytes);

        buffer.position(WAL_HEADER_SIZE);

        int payloadLength =
                buffer.getInt();

        /*
         * Existing v0.9.3 format:
         *
         * 4-byte length
         * +
         * payload
         *
         * v0.10.0 requires another 4-byte checksum.
         */
        assertEquals(
                WAL_HEADER_SIZE
                        + Integer.BYTES
                        + payloadLength
                        + Integer.BYTES,
                bytes.length
        );
    }

    @Test
    void storedChecksumMatchesPayload()
            throws IOException {

        Path walPath =
                tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "hello"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        byte[] bytes =
                Files.readAllBytes(walPath);

        ByteBuffer buffer =
                ByteBuffer.wrap(bytes);

        buffer.position(WAL_HEADER_SIZE);

        int payloadLength =
                buffer.getInt();

        byte[] payload =
                new byte[payloadLength];

        buffer.get(payload);

        int storedChecksum =
                buffer.getInt();

        CRC32C crc =
                new CRC32C();

        crc.update(
                payload,
                0,
                payload.length
        );

        int expectedChecksum =
                (int) crc.getValue();

        assertEquals(
                expectedChecksum,
                storedChecksum
        );
    }

    @Test
    void corruptedPayloadIsDetectedByChecksum()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "hello"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        /*
         * Physical frame:
         *
         * [4-byte length][payload][4-byte checksum]
         *
         * Corrupt exactly one payload byte while leaving
         * the checksum untouched.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {
            int walOffset = Integer.BYTES * 2;
            channel.position(walOffset);

            ByteBuffer lengthBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            channel.read(lengthBuffer);
            lengthBuffer.flip();

            int payloadLength =
                    lengthBuffer.getInt();

            assertTrue(payloadLength > 0);

            /*
             * Position now points to first payload byte.
             */
            long payloadPosition =
                    Integer.BYTES;

            channel.position(
                    walOffset + payloadPosition
            );

            ByteBuffer oneByte =
                    ByteBuffer.allocate(1);

            assertEquals(
                    1,
                    channel.read(oneByte)
            );

            oneByte.flip();

            byte original =
                    oneByte.get();

            byte corrupted =
                    (byte) (original ^ 0x01);

            channel.position(
                    walOffset + payloadPosition
            );

            ByteBuffer replacement =
                    ByteBuffer.wrap(
                            new byte[]{corrupted}
                    );

            while (replacement.hasRemaining()) {
                channel.write(replacement);
            }
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertThrows(
                    WalException.class,
                    wal::readAll
            );
        }
    }

    @Test
    void corruptedStoredChecksumIsDetected()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord record =
                publishRecord(
                        "message-1",
                        "hello"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(record);
        }

        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {

            channel.position(WAL_HEADER_SIZE);

            ByteBuffer lengthBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            channel.read(lengthBuffer);
            lengthBuffer.flip();

            int payloadLength =
                    lengthBuffer.getInt();

            long checksumPosition =
                    WAL_HEADER_SIZE
                            + Integer.BYTES
                            + payloadLength;

            channel.position(checksumPosition);

            ByteBuffer checksumBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            channel.read(checksumBuffer);
            checksumBuffer.flip();

            int checksum =
                    checksumBuffer.getInt();

            int corruptedChecksum =
                    checksum ^ 0x01;

            channel.position(checksumPosition);

            ByteBuffer replacement =
                    ByteBuffer.allocate(Integer.BYTES);

            replacement.putInt(corruptedChecksum);
            replacement.flip();

            while (replacement.hasRemaining()) {
                channel.write(replacement);
            }
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertThrows(
                    WalException.class,
                    wal::readAll
            );
        }
    }

    @Test
    void checksumMismatchIsNotTreatedAsRecoverableTornTail()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord(
                        "message-1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "message-2",
                        "B"
                );

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
            wal.append(second);
        }

        long sizeBeforeCorruption =
                Files.size(walPath);

        /*
         * Corrupt one byte inside second record's payload.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {

            channel.position(WAL_HEADER_SIZE);

            ByteBuffer firstLengthBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            channel.read(firstLengthBuffer);
            firstLengthBuffer.flip();

            int firstPayloadLength =
                    firstLengthBuffer.getInt();

            long secondFrameStart =
                    WAL_HEADER_SIZE
                            + Integer.BYTES
                            + firstPayloadLength
                            + Integer.BYTES;

            channel.position(secondFrameStart);

            ByteBuffer secondLengthBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            channel.read(secondLengthBuffer);
            secondLengthBuffer.flip();

            int secondPayloadLength =
                    secondLengthBuffer.getInt();

            assertTrue(secondPayloadLength > 0);

            long secondPayloadStart =
                    secondFrameStart
                            + Integer.BYTES;

            channel.position(secondPayloadStart);

            ByteBuffer oneByte =
                    ByteBuffer.allocate(1);

            channel.read(oneByte);
            oneByte.flip();

            byte original =
                    oneByte.get();

            channel.position(secondPayloadStart);

            channel.write(
                    ByteBuffer.wrap(
                            new byte[]{
                                    (byte) (original ^ 0x01)
                            }
                    )
            );
        }

        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            assertThrows(
                    WalException.class,
                    wal::readAll
            );
        }

        /*
         * Very important:
         *
         * checksum corruption must NOT cause automatic
         * WAL truncation.
         */
        assertEquals(
                sizeBeforeCorruption,
                Files.size(walPath)
        );
    }

    // ---------------------------------------------------------------------
    // WAL Header/ Validation
    // ---------------------------------------------------------------------
    @Test
    void newWalStartsWithMagicAndVersion()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        try (WriteAheadLog ignored =
                     new FileWriteAheadLog(walPath)) {
        }

        byte[] bytes =
                Files.readAllBytes(walPath);

        ByteBuffer buffer =
                ByteBuffer.wrap(bytes);

        assertEquals(
                0x4451574C,
                buffer.getInt()
        );

        assertEquals(
                1,
                buffer.getInt()
        );
    }

    @Test
    void existingWalWithSupportedVersionCanBeReopened() {
        Path walPath = tempDir.resolve("queue.wal");

        WalRecord first =
                publishRecord(
                        "message-1",
                        "A"
                );

        /*
         * First open creates:
         *
         * [magic][version]
         *
         * and then appends a record.
         */
        try (WriteAheadLog wal =
                     new FileWriteAheadLog(walPath)) {

            wal.append(first);
        }

        /*
         * Reopening must validate the existing header
         * and continue normally.
         */
        try (WriteAheadLog reopened =
                     new FileWriteAheadLog(walPath)) {

            assertEquals(
                    List.of(first),
                    reopened.readAll()
            );
        }
    }

    @Test
    void invalidWalMagicIsRejected()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            Integer.BYTES * 2
                    );

            /*
             * Wrong magic.
             */
            header.putInt(0x12345678);

            /*
             * Valid-looking version.
             */
            header.putInt(1);

            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }
        }

        assertThrows(
                WalException.class,
                () -> new FileWriteAheadLog(walPath)
        );
    }

    @Test
    void unsupportedWalVersionIsRejected()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer header =
                    ByteBuffer.allocate(
                            Integer.BYTES * 2
                    );

            /*
             * DQWL
             */
            header.putInt(0x4451574C);

            /*
             * Deliberately unsupported version.
             */
            header.putInt(999);

            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }
        }

        assertThrows(
                WalException.class,
                () -> new FileWriteAheadLog(walPath)
        );
    }

    @Test
    void reopeningExistingWalDoesNotWriteHeaderAgain()
            throws IOException {

        Path walPath = tempDir.resolve("queue.wal");

        /*
         * Create the WAL.
         */
        try (WriteAheadLog ignored =
                     new FileWriteAheadLog(walPath)) {
        }

        long sizeAfterFirstOpen =
                Files.size(walPath);

        /*
         * Header should currently be:
         *
         * magic   = 4 bytes
         * version = 4 bytes
         *
         * total   = 8 bytes
         */
        assertEquals(
                Integer.BYTES * 2,
                sizeAfterFirstOpen
        );

        /*
         * Reopen several times.
         */
        try (WriteAheadLog ignored =
                     new FileWriteAheadLog(walPath)) {
        }

        try (WriteAheadLog ignored =
                     new FileWriteAheadLog(walPath)) {
        }

        long sizeAfterReopen =
                Files.size(walPath);

        /*
         * If constructor accidentally writes the header
         * every time, this would become:
         *
         * 8
         * 16
         * 24
         * ...
         */
        assertEquals(
                sizeAfterFirstOpen,
                sizeAfterReopen
        );
    }

    @Test
    void incompleteWalHeaderIsRejected() throws IOException {
        Path walPath = tempDir.resolve("queue.wal");

        /*
         * Valid WAL header is:
         *
         * [magic: 4 bytes][version: 4 bytes]
         *
         * We intentionally write only part of the header.
         */
        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.WRITE
                     )) {

            ByteBuffer partialHeader =
                    ByteBuffer.allocate(Integer.BYTES);

            /*
             * Write only the magic.
             * Version bytes are missing.
             */
            partialHeader.putInt(0x4451574C); // DQWL
            partialHeader.flip();

            while (partialHeader.hasRemaining()) {
                channel.write(partialHeader);
            }
        }

        WalException exception =
                assertThrows(
                        WalException.class,
                        () -> new FileWriteAheadLog(walPath)
                );

        assertTrue(
                exception.getMessage()
                        .contains("Incomplete WAL header")
        );
    }

    private void appendPartialFrame(
            Path walPath,
            int declaredPayloadLength,
            byte[] actualPayload
    ) throws IOException {

        try (FileChannel channel =
                     FileChannel.open(
                             walPath,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.APPEND
                     )) {

            ByteBuffer length =
                    ByteBuffer.allocate(
                            Integer.BYTES
                    );

            length.putInt(
                    declaredPayloadLength
            );

            length.flip();

            while (length.hasRemaining()) {
                channel.write(length);
            }

            ByteBuffer payload =
                    ByteBuffer.wrap(
                            actualPayload
                    );

            while (payload.hasRemaining()) {
                channel.write(payload);
            }
        }
    }

    private WalRecord publishRecord(
            String messageId,
            String payload
    ) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                messageId,
                payload,
                null,
                1,
                Instant.parse(
                        "2026-08-22T00:00:00Z"
                )
        );
    }
}
