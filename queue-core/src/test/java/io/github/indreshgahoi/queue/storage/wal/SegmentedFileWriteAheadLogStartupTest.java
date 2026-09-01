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
       //--------------------------------------------------------------------------------
      //  Segmented - WAL Recovery Policy Test
     //-----------------------------------------------------------------------------------
       @Test
       void tornTailInActiveSegmentIsRecovered() throws IOException {
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

           /*
            * Build WAL with at least two segments so segment 1
            * becomes active.
            */
           try (SegmentedFileWriteAheadLog wal =
                        new SegmentedFileWriteAheadLog(
                                tempDir,
                                segmentTargetBytes
                        )) {

               wal.append(first);
               wal.append(second);
           }

           Path activeSegment =
                   tempDir.resolve(
                           "segment-000001.wal"
                   );

           long validSize =
                   Files.size(
                           activeSegment
                   );

           /*
            * Simulate crash while writing the next frame
            * to the ACTIVE segment.
            *
            * Declare 100 payload bytes but write only 3.
            */
           try (FileChannel channel =
                        FileChannel.open(
                                activeSegment,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.APPEND
                        )) {

               ByteBuffer length =
                       ByteBuffer.allocate(
                               Integer.BYTES
                       );

               length.putInt(100);
               length.flip();

               while (length.hasRemaining()) {
                   channel.write(length);
               }

               channel.write(
                       ByteBuffer.wrap(
                               new byte[]{
                                       1, 2, 3
                               }
                       )
               );
           }

           assertTrue(
                   Files.size(activeSegment)
                           > validSize
           );

           /*
            * Highest segment is active.
            *
            * Its incomplete tail may be repaired.
            */
           try (SegmentedFileWriteAheadLog recovered =
                        new SegmentedFileWriteAheadLog(
                                tempDir,
                                segmentTargetBytes
                        )) {

               assertEquals(
                       List.of(
                               first,
                               second
                       ),
                       recovered.readAll()
               );
           }

           /*
            * Physical repair should truncate back to the
            * last valid frame boundary.
            */
           assertEquals(
                   validSize,
                   Files.size(activeSegment)
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
                        "2026-08-22T00:00:00Z"
                )
        );
    }

}