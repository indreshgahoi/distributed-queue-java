package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalSegmentDiscoveryTest {

    @TempDir
    Path tempDir;

    private final WalSegmentFiles files =
            new WalSegmentFiles();

    private final WalSegmentDiscovery discovery =
            new WalSegmentDiscovery();

    @Test
    void emptyDirectoryHasNoSegments() {
        assertTrue(
                discovery.discover(tempDir)
                        .isEmpty()
        );

        assertTrue(
                discovery.activeSegment(tempDir)
                        .isEmpty()
        );
    }

    @Test
    void singleSegmentIsActive()
            throws IOException {

        createSegment(0);

        List<WalSegment> segments =
                discovery.discover(tempDir);

        assertEquals(
                1,
                segments.size()
        );

        assertEquals(
                0,
                discovery.activeSegment(tempDir)
                        .orElseThrow()
                        .segmentId()
        );
    }

    @Test
    void highestSegmentIdIsActive()
            throws IOException {

        createSegment(0);
        createSegment(1);
        createSegment(2);

        assertEquals(
                2,
                discovery.activeSegment(tempDir)
                        .orElseThrow()
                        .segmentId()
        );
    }

    @Test
    void segmentsAreDiscoveredInAscendingOrder()
            throws IOException {

        /*
         * Deliberately create them out of order.
         */
        createSegment(2);
        createSegment(0);
        createSegment(1);

        List<Long> ids =
                discovery.discover(tempDir)
                        .stream()
                        .map(WalSegment::segmentId)
                        .toList();

        assertEquals(
                List.of(
                        0L,
                        1L,
                        2L
                ),
                ids
        );
    }

    @Test
    void tempSegmentsAreIgnored()
            throws IOException {

        createSegment(0);

        Files.createFile(
                files.tempPathFor(
                        tempDir,
                        1
                )
        );

        List<WalSegment> segments =
                discovery.discover(tempDir);

        assertEquals(
                1,
                segments.size()
        );

        assertEquals(
                0,
                segments.getFirst()
                        .segmentId()
        );
    }

    @Test
    void unrelatedFilesAreIgnored()
            throws IOException {

        createSegment(0);

        Files.createFile(
                tempDir.resolve(
                        "foo.wal"
                )
        );

        Files.createFile(
                tempDir.resolve(
                        "segment-copy.wal"
                )
        );

        Files.createFile(
                tempDir.resolve(
                        "notes.txt"
                )
        );

        assertEquals(
                1,
                discovery.discover(tempDir)
                        .size()
        );
    }

    @Test
    void gapInSegmentSequenceIsRejected()
            throws IOException {

        createSegment(0);
        createSegment(1);
        createSegment(3);

        assertThrows(
                WalException.class,
                () -> discovery.discover(
                        tempDir
                )
        );
    }

    @Test
    void firstRemainingSegmentDoesNotNeedToBeZero()
            throws IOException {

        /*
         * Important for future compaction.
         *
         * Older segments may already have been deleted.
         */
        createSegment(37);
        createSegment(38);
        createSegment(39);

        List<Long> ids =
                discovery.discover(tempDir)
                        .stream()
                        .map(WalSegment::segmentId)
                        .toList();

        assertEquals(
                List.of(
                        37L,
                        38L,
                        39L
                ),
                ids
        );
    }

    private void createSegment(
            long segmentId
    ) throws IOException {

        Files.createFile(
                files.pathFor(
                        tempDir,
                        segmentId
                )
        );
    }
}