package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.LocalMessageQueue;
import io.github.indreshgahoi.queue.QueueConfiguration;
import io.github.indreshgahoi.queue.storage.compaction.SnapshotCompactionCoordinator;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionBoundaryTracker;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionPlanner;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalCompactionCoordinator;
import io.github.indreshgahoi.queue.storage.wal.WalSegmentReclaimer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageLifecycleIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void policyCheckpointMakesOldSegmentsReclaimableAndRecoveryRemainsComplete() {
        Path walDirectory = tempDir.resolve("wal");
        Path snapshotPath = tempDir.resolve("queue.snapshot");
        QueueSnapshotStore snapshotStore =
                new FileQueueSnapshotStore(snapshotPath);
        Clock clock =
                Clock.fixed(
                        Instant.parse("2026-09-01T00:00:00Z"),
                        java.time.ZoneOffset.UTC
                );

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             walDirectory,
                             45
                     );
             LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             new QueueConfiguration(),
                             wal,
                             snapshotStore
                     );
             StorageLifecycleManager lifecycle =
                     lifecycleManager(
                             queue,
                             wal,
                             snapshotStore,
                             walDirectory
                     )) {
            queue.publish("A");
            queue.publish("B");
            queue.publish("C");

            assertTrue(lifecycle.runOnce());
            assertFalse(
                    Files.exists(
                            walDirectory.resolve("segment-000000.wal")
                    )
            );
            assertFalse(
                    Files.exists(
                            walDirectory.resolve("segment-000001.wal")
                    )
            );
            assertTrue(
                    Files.exists(
                            walDirectory.resolve("segment-000002.wal")
                    )
            );
        }

        try (SegmentedFileWriteAheadLog wal =
                     new SegmentedFileWriteAheadLog(
                             walDirectory,
                             45
                     );
             LocalMessageQueue recovered =
                     new LocalMessageQueue(
                             clock,
                             new QueueConfiguration(),
                             wal,
                             snapshotStore
                     )) {
            assertTrue(recovered.receive().isPresent());
            assertTrue(recovered.receive().isPresent());
            assertTrue(recovered.receive().isPresent());
        }
    }

    private StorageLifecycleManager lifecycleManager(
            LocalMessageQueue queue,
            SegmentedFileWriteAheadLog wal,
            QueueSnapshotStore snapshotStore,
            Path walDirectory
    ) {
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        snapshotStore,
                        new WalCompactionBoundaryTracker(),
                        wal.storageLineage(),
                        wal::validatePosition,
                        new WalCompactionCoordinator(
                                new WalCompactionPlanner(),
                                new WalSegmentReclaimer(walDirectory)
                        )
                );

        return new StorageLifecycleManager(
                wal::currentDurablePosition,
                queue::captureSnapshot,
                coordinator,
                new SegmentDistanceCheckpointPolicy(2),
                Duration.ofMinutes(1)
        );
    }
}
