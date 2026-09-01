package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class WalSegmentFilesTest {

    @TempDir
    Path tempDir;

    @Test
    void segmentPathUsesStableSortableName() {
        WalSegmentFiles files =
                new WalSegmentFiles();

        Path path =
                files.pathFor(
                        tempDir,
                        7
                );

        assertEquals(
                tempDir.resolve(
                        "segment-000007.wal"
                ),
                path
        );
    }

    @Test
    void tempSegmentUsesDifferentNonAuthoritativeSuffix() {
        WalSegmentFiles files =
                new WalSegmentFiles();

        Path path =
                files.tempPathFor(
                        tempDir,
                        7
                );

        assertEquals(
                tempDir.resolve(
                        "segment-000007.tmp"
                ),
                path
        );
    }

    @Test
    void negativeSegmentIdIsRejected() {
        WalSegmentFiles files =
                new WalSegmentFiles();

        assertThrows(
                IllegalArgumentException.class,
                () -> files.pathFor(
                        tempDir,
                        -1
                )
        );
    }
}
