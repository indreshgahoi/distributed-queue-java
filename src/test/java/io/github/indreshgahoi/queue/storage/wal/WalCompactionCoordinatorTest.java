package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionPlanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalCompactionCoordinatorTest {

    @TempDir
    Path tempDir;

    @Test
    void orchestratesPlanAndReclamationWithoutDeletingBoundary()
            throws IOException {
        WalSegmentFiles files = new WalSegmentFiles();
        WalSegmentInitializer initializer =
                new WalSegmentInitializer();

        for (long id = 0; id <= 2; id++) {
            initializer.initialize(files.pathFor(tempDir, id));
        }

        WalCompactionCoordinator coordinator =
                new WalCompactionCoordinator(
                        new WalCompactionPlanner(),
                        new WalSegmentReclaimer(tempDir)
                );

        coordinator.compactThrough(new WalPosition(2, 0));

        assertFalse(Files.exists(files.pathFor(tempDir, 0)));
        assertFalse(Files.exists(files.pathFor(tempDir, 1)));
        assertTrue(Files.exists(files.pathFor(tempDir, 2)));
    }
}
