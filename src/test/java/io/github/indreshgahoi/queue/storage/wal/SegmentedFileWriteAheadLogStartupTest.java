package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SegmentedFileWriteAheadLogStartupTest {

    @TempDir
    Path tempDir;

    @Test
    void emptyDirectoryCreatesSegmentZero() {

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            assertTrue(
                    Files.exists(
                            tempDir.resolve(
                                    "segment-000000.wal"
                            )
                    )
            );

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    WalSegmentInitializer.WAL_HEADER_SIZE,
                    wal.currentDurablePosition()
                            .offset()
            );
        }
    }

    @Test
    void existingHighestSegmentBecomesActive()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                tempDir.resolve(
                        "segment-000007.wal"
                )
        );

        initializer.initialize(
                tempDir.resolve(
                        "segment-000008.wal"
                )
        );

        initializer.initialize(
                tempDir.resolve(
                        "segment-000009.wal"
                )
        );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            assertEquals(
                    9,
                    wal.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void invalidHeaderInAnyExistingSegmentFailsStartup()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                tempDir.resolve(
                        "segment-000000.wal"
                )
        );

        /*
         * Authoritative filename,
         * invalid contents.
         */
        Files.write(
                tempDir.resolve(
                        "segment-000001.wal"
                ),
                new byte[]{
                        1, 2, 3
                }
        );

        assertThrows(
                WalException.class,
                () -> new SegmentedFileWriteAheadLog(
                        tempDir,
                        1_024
                )
        );
    }

    @Test
    void leftoverTempSegmentDoesNotBecomeActive()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                tempDir.resolve(
                        "segment-000000.wal"
                )
        );

        Files.write(
                tempDir.resolve(
                        "segment-000001.tmp"
                ),
                new byte[]{
                        1, 2, 3, 4
                }
        );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            assertEquals(
                    0,
                    wal.currentDurablePosition()
                            .segmentId()
            );
        }
    }

    @Test
    void headerOnlyHighestSegmentIsValidActiveSegment()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                tempDir.resolve(
                        "segment-000000.wal"
                )
        );

        initializer.initialize(
                tempDir.resolve(
                        "segment-000001.wal"
                )
        );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             tempDir,
                             1_024
                     )) {

            assertEquals(
                    1,
                    wal.currentDurablePosition()
                            .segmentId()
            );

            assertEquals(
                    WalSegmentInitializer.WAL_HEADER_SIZE,
                    wal.currentDurablePosition()
                            .offset()
            );
        }
    }

    @Test
    void invalidSegmentGapFailsBeforeChoosingActiveSegment()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        initializer.initialize(
                tempDir.resolve(
                        "segment-000000.wal"
                )
        );

        initializer.initialize(
                tempDir.resolve(
                        "segment-000002.wal"
                )
        );

        assertThrows(
                WalException.class,
                () -> new SegmentedFileWriteAheadLog(
                        tempDir,
                        1_024
                )
        );
    }
}