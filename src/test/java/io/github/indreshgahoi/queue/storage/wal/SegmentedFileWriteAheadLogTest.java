package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;
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

import static org.junit.jupiter.api.Assertions.*;

class SegmentedFileWriteAheadLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendedRecordCanBeReadBack() {
        WalRecord expected =
                publishRecord(
                        "message-1",
                        "A"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            wal.append(expected);

            assertEquals(
                    List.of(expected),
                    wal.readAll()
            );
        }
    }

    @Test
    void multipleRecordsCanBeReadBackFromSingleSegment() {
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

        WalRecord third =
                publishRecord(
                        "message-3",
                        "C"
                );

        /*
         * Large threshold intentionally keeps all records
         * inside segment 0.
         *
         * Rotation is not under test here.
         */
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024 * 1024
                     )) {

            wal.append(first);
            wal.append(second);
            wal.append(third);

            assertEquals(
                    List.of(
                            first,
                            second,
                            third
                    ),
                    wal.readAll()
            );

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void reopeningWalRecoversRecordsFromExistingActiveSegment() {
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

        /*
         * First process lifetime.
         */
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024 * 1024
                     )) {

            wal.append(first);
            wal.append(second);
        }

        /*
         * New WAL instance simulates restart.
         *
         * Startup should discover segment 0,
         * validate it and reopen it as active.
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024 * 1024
                     )) {

            assertEquals(
                    List.of(
                            first,
                            second
                    ),
                    reopened.readAll()
            );

            assertEquals(
                    0,
                    reopened.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void currentDurablePositionAdvancesAfterAppend() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024 * 1024
                     )) {

            WalPosition initial =
                    wal.currentDurablePosition();

            /*
             * Initially the position should be directly
             * after the segment header.
             */
            assertEquals(
                    0,
                    initial.segmentId()
            );

            assertEquals(
                    WalSegmentInitializer.WAL_HEADER_SIZE,
                    initial.offset()
            );

            wal.append(
                    publishRecord(
                            "message-1",
                            "A"
                    )
            );

            WalPosition afterFirst =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    afterFirst.segmentId()
            );

            assertTrue(
                    afterFirst.offset()
                            > initial.offset()
            );

            wal.append(
                    publishRecord(
                            "message-2",
                            "B"
                    )
            );

            WalPosition afterSecond =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    afterSecond.segmentId()
            );

            assertTrue(
                    afterSecond.offset()
                            > afterFirst.offset()
            );
        }
    }

    @Test
    void appendAfterCloseIsRejected() {
        SegmentedFileWriteAheadLog wal =
                new SegmentedFileWriteAheadLog(
                        tempDir,
                        1_024
                );

        wal.close();

        assertThrows(
                WalException.class,
                () -> wal.append(
                        publishRecord(
                                "message-1",
                                "A"
                        )
                )
        );
    }

    @Test
    void checksumCorruptionInActiveSegmentIsRejected()
            throws IOException {

        long segmentTargetBytes =
                1_024;

        WalRecord record =
                publishRecord(
                        "m1",
                        "payload-A"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(record);
        }

        Path activeSegment =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        corruptFirstFramePayloadByte(
                activeSegment
        );

        /*
         * Structurally complete frame + wrong checksum
         * is corruption even in the active segment.
         */
        assertThrows(
                WalException.class,
                () -> new SegmentedFileWriteAheadLog(
                        tempDir,
                        segmentTargetBytes
                )
        );
    }

    @Test
    void checksumCorruptionInSealedSegmentIsRejected()
            throws IOException {

        long segmentTargetBytes =
                64;

        WalRecord first =
                publishRecord(
                        "m1",
                        "large-record-to-trigger-rotation-aaaaaaaa"
                );

        WalRecord second =
                publishRecord(
                        "m2",
                        "B"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(first);

            /*
             * Causes segment 0 to become sealed.
             */
            wal.append(second);
        }

        Path sealedSegment =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        corruptFirstFramePayloadByte(
                sealedSegment
        );

        assertThrows(
                WalException.class,
                () -> new SegmentedFileWriteAheadLog(
                        tempDir,
                        segmentTargetBytes
                )
        );
    }

    private void corruptFirstFramePayloadByte(
            Path segmentPath
    ) throws IOException {

        /*
         * Segment layout:
         *
         * [magic:4]
         * [version:4]
         * [length:4]
         * [payload:N]
         * [checksum:4]
         *
         * First payload byte starts at:
         *
         * header(8) + length(4) = 12
         */
        long firstPayloadByte =
                WalSegmentInitializer.WAL_HEADER_SIZE
                        + Integer.BYTES;

        try (FileChannel channel =
                     FileChannel.open(
                             segmentPath,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE
                     )) {

            channel.position(
                    firstPayloadByte
            );

            ByteBuffer original =
                    ByteBuffer.allocate(1);

            assertEquals(
                    1,
                    channel.read(original)
            );

            original.flip();

            byte value =
                    original.get();

            channel.position(
                    firstPayloadByte
            );

            ByteBuffer corrupted =
                    ByteBuffer.wrap(
                            new byte[]{
                                    (byte) (value ^ 0x01)
                            }
                    );

            while (corrupted.hasRemaining()) {
                channel.write(corrupted);
            }
        }
    }

    //-----------------------------------------------------------------------
    // WAL rotation Test Case
    //--------------------------------------------------------------------------
    @Test
    void frameThatCrossesThresholdRemainsInCurrentSegment() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first =
                publishRecord(
                        "m1",
                        "A"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            WalPosition before =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    before.segmentId()
            );

            wal.append(first);

            /*
             * The first frame takes segment 0 beyond the
             * tiny target, but the frame must remain entirely
             * in segment 0.
             *
             * Rotation must NOT happen midway through append.
             */
            WalPosition after =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    after.segmentId()
            );

            assertTrue(
                    after.offset()
                            > segmentTargetBytes
            );

            assertFalse(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    )
            );

            assertEquals(
                    List.of(first),
                    wal.readAll()
            );
        }
    }

    @Test
    void nextAppendAfterThresholdCreatesNewSegment() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first =
                publishRecord(
                        "m1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "m2",
                        "B"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            /*
             * First record remains in segment 0,
             * even though it takes the segment over target.
             */
            wal.append(first);

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * BEFORE this append, segment 0 is already
             * over threshold, so rotation must happen.
             */
            wal.append(second);

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000000.wal"
                            )
                    )
            );

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    )
            );

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void oldSegmentIsNeverModifiedAfterRotation()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first =
                publishRecord(
                        "m1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "m2",
                        "B"
                );

        WalRecord third =
                publishRecord(
                        "m3",
                        "C"
                );

        Path segmentZero =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(first);

            long sealedSize =
                    Files.size(
                            segmentZero
                    );

            /*
             * Rotates to segment 1, then appends second.
             */
            wal.append(second);

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    sealedSize,
                    Files.size(segmentZero)
            );

            /*
             * Further writes must never touch segment 0.
             */
            wal.append(third);

            assertEquals(
                    sealedSize,
                    Files.size(segmentZero),
                    "Sealed segment must never be modified after rotation"
            );
        }
    }

    @Test
    void newSegmentStartsWithValidWalHeader()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            /*
             * Forces rotation before this append.
             */
            wal.append(
                    publishRecord(
                            "m2",
                            "B"
                    )
            );
        }

        Path segmentOne =
                tempDir.resolve(
                        "segment-000001.wal"
                );

        assertTrue(
                Files.exists(segmentOne)
        );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        /*
         * Strongest simple assertion:
         * the segment must independently satisfy
         * the WAL header contract.
         */
        assertDoesNotThrow(
                () -> initializer.validate(
                        segmentOne
                )
        );

        assertTrue(
                Files.size(segmentOne)
                        > WalSegmentInitializer.WAL_HEADER_SIZE
        );
    }

    @Test
    void currentDurablePositionMovesToNewSegmentAfterRotation() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            WalPosition initial =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    initial.segmentId()
            );

            assertEquals(
                    WalSegmentInitializer.WAL_HEADER_SIZE,
                    initial.offset()
            );

            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            WalPosition afterFirst =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    afterFirst.segmentId()
            );

            /*
             * This append first rotates to segment 1.
             */
            wal.append(
                    publishRecord(
                            "m2",
                            "B"
                    )
            );

            WalPosition afterRotation =
                    wal.currentDurablePosition();

            assertEquals(
                    1,
                    afterRotation.segmentId()
            );

            /*
             * Offset is relative to segment 1, not globally
             * relative to all WAL files.
             */
            assertTrue(
                    afterRotation.offset()
                            > WalSegmentInitializer.WAL_HEADER_SIZE
            );
        }
    }

    //---------------------------------------------------------------
    // Crash Safe Rotation Test
    //------------------------------------------------------------------
    @Test
    void leftoverTempSegmentDoesNotBecomeActiveAfterRestart() {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        /*
         * Build a valid segment 0.
         */
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );
        }

        /*
         * Simulate a crash during rotation.
         *
         * A candidate exists, but it has never been
         * atomically promoted to .wal.
         */
        Path candidate =
                tempDir.resolve(
                        "segment-000001.tmp"
                );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                candidate
        );

        assertTrue(
                Files.exists(candidate)
        );

        assertFalse(
                Files.exists(
                        tempDir.resolve(
                                "segment-000001.wal"
                        )
                )
        );

        /*
         * Restart.
         *
         * .tmp must be ignored.
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    0,
                    reopened.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    1,
                    reopened.readAll()
                            .size()
            );
        }
    }

    @Test
    void failedSegmentPromotionLeavesPreviousSegmentActive()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        SegmentedFileWriteAheadLog.SegmentPromoter failingPromoter =
                (candidate, destination) -> {
                    throw new IOException(
                            "Simulated segment promotion failure"
                    );
                };

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes,
                             failingPromoter,
                             SegmentedFileWriteAheadLog::writeFrame,
                             SegmentedFileWriteAheadLog::openAppendChannel
                     )) {

            /*
             * First frame stays entirely in segment 0
             * and pushes it over the threshold.
             */
            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            WalPosition beforeRotation =
                    wal.currentDurablePosition();

            assertEquals(
                    0,
                    beforeRotation.segmentId()
            );

            /*
             * Second append attempts rotation first.
             *
             * Promotion fails.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(
                            publishRecord(
                                    "m2",
                                    "B"
                            )
                    )
            );

            /*
             * In-memory active segment must still be 0.
             */
            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * No authoritative segment 1 exists.
             */
            assertFalse(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    )
            );
        }

        /*
         * Restart must also discover segment 0 as active.
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    0,
                    reopened.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    1,
                    reopened.readAll()
                            .size()
            );
        }
    }

    @Test
    void successfulPromotionMakesNewSegmentActive() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * Segment 0 is already over threshold.
             *
             * This append should:
             *
             * 1. create segment-000001.tmp
             * 2. initialize + force
             * 3. atomically promote to segment-000001.wal
             * 4. make segment 1 active
             * 5. append m2 there
             */
            wal.append(
                    publishRecord(
                            "m2",
                            "B"
                    )
            );

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000000.wal"
                            )
                    )
            );

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    )
            );

            assertFalse(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.tmp"
                            )
                    )
            );
        }

        /*
         * Authority must survive restart.
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    1,
                    reopened.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    2,
                    reopened.readAll()
                            .size()
            );
        }
    }

    @Test
    void rotationNeverOverwritesExistingNextSegment()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        /*
         * Establish segment 0 and push it over threshold.
         */
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );
        }

        /*
         * Manually create segment 1.
         *
         * On restart, segment 1 becomes active because it is
         * the highest authoritative .wal segment.
         */
        Path segmentOne =
                tempDir.resolve(
                        "segment-000001.wal"
                );

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                segmentOne
        );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * First real record goes into active segment 1.
             *
             * Because target = header + 1, this makes
             * segment 1 exceed its threshold.
             */
            wal.append(
                    publishRecord(
                            "m2",
                            "B"
                    )
            );

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * Now create the segment that rotation would
             * normally try to create next.
             */
            Path segmentTwo =
                    tempDir.resolve(
                            "segment-000002.wal"
                    );

            initializer.initialize(
                    segmentTwo
            );

            byte[] segmentTwoBefore =
                    Files.readAllBytes(
                            segmentTwo
                    );

            /*
             * Next append wants:
             *
             * segment 1 -> segment 2
             *
             * But segment 2 already exists.
             *
             * Rotation must fail instead of replacing it.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(
                            publishRecord(
                                    "m3",
                                    "C"
                            )
                    )
            );

            /*
             * This is the actual invariant under test:
             *
             * existing authoritative segment 2 must remain
             * byte-for-byte untouched.
             */
            assertArrayEquals(
                    segmentTwoBefore,
                    Files.readAllBytes(
                            segmentTwo
                    ),
                    "Rotation must never overwrite an existing authoritative segment"
            );

            /*
             * Since rotation failed before authority transfer,
             * this process must still consider segment 1 active.
             */
            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void readFromBeginningOfFirstSegmentReturnsAllRecords() {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first = publishRecord(
                "m1",
                "A"
        );
        WalRecord second = publishRecord(
                "m2",
                "B"
        );
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(
                    first
            );
            wal.append(
                    second
            );
            assertTrue(
                    Files.exists(
                            new WalSegmentFiles()
                                    .pathFor(
                                            tempDir,
                                            1)));

            WalPosition walPosition =
                    new WalPosition(0,
                            WalSegmentInitializer.WAL_HEADER_SIZE
                    );

            List<WalRecord> records =
                    wal.readFrom(walPosition);
            assertEquals(
                    List.of(
                            first,
                            second
                    ),
                    records
            );
        }
    }

    @Test
    void readFromFrameBoundaryWithinSegmentReturnsRemainingRecords() {
        WalRecord first = publishRecord("m1", "A");
        WalRecord second = publishRecord("m2", "B");
        WalRecord third = publishRecord("m3", "C");

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024 * 1_024
                     )) {

            wal.append(first);
            WalPosition afterFirst =
                    wal.currentDurablePosition();

            wal.append(second);
            wal.append(third);

            assertEquals(
                    List.of(second, third),
                    wal.readFrom(afterFirst)
            );
        }

    }

    @Test
    void readFromEndOfSegmentContinuesWithNextSegment() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first = publishRecord("m1", "A");
        WalRecord second = publishRecord("m2", "B");
        WalRecord third = publishRecord("m3", "C");

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(first);
            WalPosition endOfFirstSegment =
                    wal.currentDurablePosition();

            wal.append(second);
            wal.append(third);

            assertEquals(
                    List.of(second, third),
                    wal.readFrom(endOfFirstSegment)
            );
        }

    }

    @Test
    void readFromBeginningOfLaterSegmentSkipsEarlierSegments() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first = publishRecord("m1", "A");
        WalRecord second = publishRecord("m2", "B");
        WalRecord third = publishRecord("m3", "C");

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(first);
            wal.append(second);
            wal.append(third);

            WalPosition beginningOfSegmentOne =
                    new WalPosition(
                            1,
                            WalSegmentInitializer.WAL_HEADER_SIZE
                    );

            assertEquals(
                    List.of(second, third),
                    wal.readFrom(beginningOfSegmentOne)
            );
        }

    }

    @Test
    void readFromPositionPreservesGlobalRecordOrder() {
        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        WalRecord first = publishRecord("m1", "A");
        WalRecord second = publishRecord("m2", "B");
        WalRecord third = publishRecord("m3", "C");
        WalRecord fourth = publishRecord("m4", "D");

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            wal.append(first);
            WalPosition afterFirst =
                    wal.currentDurablePosition();

            wal.append(second);
            wal.append(third);
            wal.append(fourth);

            assertEquals(
                    List.of(second, third, fourth),
                    wal.readFrom(afterFirst)
            );
        }

    }

    @Test
    void readFromCurrentDurablePositionReturnsNoRecords() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            wal.append(publishRecord("m1", "A"));

            assertTrue(
                    wal.readFrom(
                            wal.currentDurablePosition()
                    ).isEmpty()
            );
        }

    }

    @Test
    void readFromUnknownSegmentIsRejected() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            WalPosition unknownSegment =
                    new WalPosition(
                            99,
                            WalSegmentInitializer.WAL_HEADER_SIZE
                    );

            assertThrows(
                    WalException.class,
                    () -> wal.readFrom(unknownSegment)
            );
        }

    }

    @Test
    void readFromOffsetBeforeSegmentRecordAreaIsRejected() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            WalPosition insideHeader =
                    new WalPosition(
                            0,
                            Integer.BYTES
                    );

            assertThrows(
                    WalException.class,
                    () -> wal.readFrom(insideHeader)
            );
        }

    }

    @Test
    void readFromOffsetBeyondSegmentEndIsRejected() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            wal.append(publishRecord("m1", "A"));

            WalPosition end =
                    wal.currentDurablePosition();

            WalPosition beyondEnd =
                    new WalPosition(
                            end.segmentId(),
                            end.offset() + 1
                    );

            assertThrows(
                    WalException.class,
                    () -> wal.readFrom(beyondEnd)
            );
        }

    }

    @Test
    void readFromMiddleOfFrameIsRejected() {
        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            wal.append(publishRecord("m1", "A"));

            WalPosition middleOfLengthPrefix =
                    new WalPosition(
                            0,
                            WalSegmentInitializer.WAL_HEADER_SIZE + 1
                    );

            assertThrows(
                    WalException.class,
                    () -> wal.readFrom(middleOfLengthPrefix)
            );
        }

    }

    @Test
    void appendFailurePoisonsWalAndPreventsRecordAfterTornFrame()
            throws IOException {

        long segmentTargetBytes =
                1_024 * 1_024;

        WalRecord first =
                publishRecord(
                        "m1",
                        "A"
                );

        WalRecord second =
                publishRecord(
                        "m2",
                        "B"
                );

        WalRecord third =
                publishRecord(
                        "m3",
                        "C"
                );

        /*
         * Failure-injection writer:
         *
         * first append succeeds normally
         * second append writes part of the frame,
         * then throws IOException.
         */
        SegmentedFileWriteAheadLog.FrameWriter failingWriter =
                new SegmentedFileWriteAheadLog.FrameWriter() {

                    private int appendCount;

                    @Override
                    public void write(
                            FileChannel channel,
                            ByteBuffer frame
                    ) throws IOException {

                        appendCount++;

                        if (appendCount == 1) {
                            while (frame.hasRemaining()) {
                                channel.write(frame);
                            }

                            return;
                        }

                        if (appendCount == 2) {

                            /*
                             * Physically leave a torn frame:
                             *
                             * write:
                             *   4-byte length prefix
                             *   +
                             *   a few payload bytes
                             *
                             * then fail.
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
                                    "Simulated mid-frame append failure"
                            );
                        }

                        throw new AssertionError(
                                "Poisoned WAL must not attempt another physical write"
                        );
                    }
                };

        Path segmentZero =
                tempDir.resolve(
                        "segment-000000.wal"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes,
                             SegmentedFileWriteAheadLog::promoteSegment,
                             failingWriter,
                             SegmentedFileWriteAheadLog::openAppendChannel
                     )) {

            /*
             * M1 is completely durable.
             */
            wal.append(first);

            long sizeAfterFirst =
                    Files.size(
                            segmentZero
                    );

            /*
             * M2 leaves a torn frame and fails.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(second)
            );

            long sizeAfterPartialSecond =
                    Files.size(
                            segmentZero
                    );

            assertTrue(
                    sizeAfterPartialSecond > sizeAfterFirst,
                    "Test setup must leave a partial M2 frame on disk"
            );

            /*
             * Critical invariant:
             *
             * after a potentially torn append, this WAL
             * instance is poisoned.
             *
             * M3 must not be written behind the torn M2.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(third)
            );

            assertEquals(
                    sizeAfterPartialSecond,
                    Files.size(segmentZero),
                    "Rejected append after poisoning must not modify the WAL"
            );
        }

        /*
         * Restart performs active-tail recovery.
         *
         * Partial M2 must be truncated, leaving only M1.
         */
        try (SegmentedFileWriteAheadLog recovered =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    List.of(first),
                    recovered.readAll()
            );

            /*
             * New process lifetime is healthy again.
             */
            recovered.append(third);
        }

        /*
         * Final durable history:
         *
         * M1, M3
         *
         * never:
         *
         * M1, partial-M2, M3
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    List.of(
                            first,
                            third
                    ),
                    reopened.readAll()
            );
        }
    }

    @Test
    void failedRotationCandidateDoesNotBlockFutureSuccessfulRotation()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        /*
         * First promoter invocation fails.
         * Second invocation succeeds normally.
         */
        class FailOncePromoter
                implements SegmentedFileWriteAheadLog.SegmentPromoter {

            private int attempts;

            @Override
            public void promote(
                    Path candidate,
                    Path destination
            ) throws IOException {

                attempts++;

                if (attempts == 1) {
                    throw new IOException(
                            "Simulated first promotion failure"
                    );
                }

                SegmentedFileWriteAheadLog.promoteSegment(
                        candidate,
                        destination
                );
            }
        }

        FailOncePromoter promoter =
                new FailOncePromoter();

        Path candidate =
                tempDir.resolve(
                        "segment-000001.tmp"
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes,
                             promoter,
                             SegmentedFileWriteAheadLog::writeFrame,
                             SegmentedFileWriteAheadLog::openAppendChannel
                     )) {

            /*
             * First frame pushes segment 0 over threshold.
             */
            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            /*
             * First rotation attempt fails after candidate
             * initialization.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(
                            publishRecord(
                                    "m2",
                                    "B"
                            )
                    )
            );

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            /*
             * Depending on our cleanup policy, candidate may
             * already be deleted.
             *
             * Before another rotation attempt it must not block
             * creation of segment 1.
             */
            if (Files.exists(candidate)) {
                Files.delete(candidate);
            }

            /*
             * The WAL instance is still safe because authority
             * never changed.
             *
             * Retry should now successfully rotate.
             */
            wal.append(
                    publishRecord(
                            "m3",
                            "C"
                    )
            );

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    )
            );
        }
    }

    @Test
    void failureAfterSuccessfulPromotionPoisonsCurrentWalAndRestartUsesNewSegment()
            throws IOException {

        long segmentTargetBytes =
                WalSegmentInitializer.WAL_HEADER_SIZE + 1;

        class FailOpeningSecondSegment
                implements SegmentedFileWriteAheadLog.ActiveSegmentOpener {

            private int openCount;

            @Override
            public FileChannel open(
                    Path path
            ) throws IOException {

                openCount++;

                /*
                 * First call opens initial segment 0.
                 *
                 * Second call happens after segment 1 has
                 * already been atomically promoted.
                 */
                if (openCount == 2) {
                    throw new IOException(
                            "Simulated failure opening promoted segment"
                    );
                }

                return SegmentedFileWriteAheadLog
                        .openAppendChannel(path);
            }
        }

        FailOpeningSecondSegment opener =
                new FailOpeningSecondSegment();

        SegmentedFileWriteAheadLog wal =
                new SegmentedFileWriteAheadLog(
                        tempDir,
                        segmentTargetBytes,
                        SegmentedFileWriteAheadLog::promoteSegment,
                        SegmentedFileWriteAheadLog::writeFrame,
                        opener
                );

        try {
            wal.append(
                    publishRecord(
                            "m1",
                            "A"
                    )
            );

            /*
             * Rotation:
             *
             * candidate created
             * candidate promoted successfully
             * segment-1.wal now authoritative
             * opening new channel fails
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(
                            publishRecord(
                                    "m2",
                                    "B"
                            )
                    )
            );

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000001.wal"
                            )
                    ),
                    "Promotion must already have made segment 1 authoritative"
            );

            /*
             * Current process must not continue using its
             * stale segment-0 channel.
             */
            assertThrows(
                    WalException.class,
                    () -> wal.append(
                            publishRecord(
                                    "m3",
                                    "C"
                            )
                    )
            );

        } finally {
            wal.close();
        }

        /*
         * Restart derives authority from filesystem state.
         *
         * Highest authoritative .wal is segment 1.
         */
        try (SegmentedFileWriteAheadLog reopened =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             segmentTargetBytes
                     )) {

            assertEquals(
                    1,
                    reopened.currentDurablePosition()
                            .segmentId()
            );

            /*
             * M1 survived in sealed segment 0.
             *
             * M2 did NOT get appended because opening
             * segment 1 failed before the write.
             */
            assertEquals(
                    List.of(
                            publishRecord(
                                    "m1",
                                    "A"
                            )
                    ),
                    reopened.readAll()
            );

            /*
             * Fresh process can continue on segment 1.
             */
            reopened.append(
                    publishRecord(
                            "m3",
                            "C"
                    )
            );

            assertEquals(
                    1,
                    reopened.currentDurablePosition()
                            .segmentId()
            );
        }
    }


    @Test
    void directoryForceFailureAfterPromotionPoisonsWalAndRestartUsesNewSegment() {
        int[] directoryForceCalls = {0};

        SegmentedFileWriteAheadLog wal =
                new SegmentedFileWriteAheadLog(
                        tempDir,
                        9,
                        SegmentedFileWriteAheadLog::promoteSegment,
                        SegmentedFileWriteAheadLog::writeFrame,
                        SegmentedFileWriteAheadLog::openAppendChannel,
                        publishedPath -> {
                            directoryForceCalls[0]++;
                            if (directoryForceCalls[0] == 2) {
                                throw new IOException(
                                        "simulated directory force failure"
                                );
                            }
                        }
                );

        WalRecord first = publishRecord("m-1", "first");
        WalRecord rejected = publishRecord("m-2", "rejected");

        wal.append(first);

        assertThrows(
                WalException.class,
                () -> wal.append(rejected)
        );
        assertThrows(
                WalException.class,
                () -> wal.append(
                        publishRecord("m-3", "also-rejected")
                )
        );
        wal.close();

        try (SegmentedFileWriteAheadLog recovered =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             9
                     )) {
            assertEquals(
                    List.of(first),
                    recovered.readAll()
            );
            assertEquals(
                    1,
                    recovered.currentDurablePosition().segmentId()
            );
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
                        "2026-08-25T00:00:00Z"
                )
        );
    }
}
