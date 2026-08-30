package io.github.indreshgahoi.queue.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;


class WalSegmentReclaimerTest {

    @TempDir
    Path tempDir;

    @Test
    void reclaimDeletesOnlySegmentsBeforeBoundary()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        WalSegmentFiles files =
                new WalSegmentFiles();

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        0
                )
        );

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        1
                )
        );

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        2
                )
        );

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        3
                )
        );

        WalSegmentReclaimer reclaimer =
                new WalSegmentReclaimer(
                        tempDir
                );

        reclaimer.reclaimBefore(
                2
        );

        assertFalse(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                0
                        )
                )
        );

        assertFalse(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                1
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                2
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                3
                        )
                )
        );
    }

    @Test
    void boundarySegmentIsNeverDeleted()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        WalSegmentFiles files =
                new WalSegmentFiles();

        for (long id = 0;
             id <= 2;
             id++) {

            initializer.initialize(
                    files.pathFor(
                            tempDir,
                            id
                    )
            );
        }

        WalSegmentReclaimer reclaimer =
                new WalSegmentReclaimer(
                        tempDir
                );

        reclaimer.reclaimBefore(
                1
        );

        assertFalse(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                0
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                1
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                2
                        )
                )
        );
    }

    @Test
    void reclaimWithFirstSegmentBoundaryDeletesNothing()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        WalSegmentFiles files =
                new WalSegmentFiles();

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        0
                )
        );

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        1
                )
        );

        WalSegmentReclaimer reclaimer =
                new WalSegmentReclaimer(
                        tempDir
                );

        reclaimer.reclaimBefore(
                0
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                0
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                1
                        )
                )
        );
    }
    @Test
    void reclaimNeverCrossesActiveSegment()
            throws IOException {

        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        WalSegmentFiles files =
                new WalSegmentFiles();

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        0
                )
        );

        initializer.initialize(
                files.pathFor(
                        tempDir,
                        1
                )
        );

        WalSegmentReclaimer reclaimer =
                new WalSegmentReclaimer(
                        tempDir
                );

        assertThrows(
                WalException.class,
                () -> reclaimer.reclaimBefore(
                        2
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                0
                        )
                )
        );

        assertTrue(
                Files.exists(
                        files.pathFor(
                                tempDir,
                                1
                        )
                )
        );
    }

    @Test
    void partialDeletionFailureIsSafeToRetry()
            throws IOException {
        WalSegmentInitializer initializer =
                new WalSegmentInitializer();
        WalSegmentFiles files = new WalSegmentFiles();

        for (long id = 0; id <= 3; id++) {
            initializer.initialize(
                    files.pathFor(tempDir, id)
            );
        }

        WalSegmentReclaimer failing =
                new WalSegmentReclaimer(
                        tempDir,
                        path -> {
                            if (path.equals(files.pathFor(tempDir, 1))) {
                                throw new IOException("simulated failure");
                            }
                            Files.delete(path);
                        }
                );

        assertThrows(
                WalException.class,
                () -> failing.reclaimBefore(3)
        );

        assertFalse(Files.exists(files.pathFor(tempDir, 0)));
        assertTrue(Files.exists(files.pathFor(tempDir, 1)));
        assertTrue(Files.exists(files.pathFor(tempDir, 2)));
        assertTrue(Files.exists(files.pathFor(tempDir, 3)));

        new WalSegmentReclaimer(tempDir).reclaimBefore(3);

        assertFalse(Files.exists(files.pathFor(tempDir, 1)));
        assertFalse(Files.exists(files.pathFor(tempDir, 2)));
        assertTrue(Files.exists(files.pathFor(tempDir, 3)));
    }

}
