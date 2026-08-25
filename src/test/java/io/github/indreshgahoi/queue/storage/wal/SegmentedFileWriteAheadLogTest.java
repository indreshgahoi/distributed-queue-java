package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
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