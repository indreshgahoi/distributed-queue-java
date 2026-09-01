package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.compaction.SnapshotCompactionCoordinator;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionBoundaryTracker;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactor;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.SnapshotException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageLifecycleManagerTest {

    @Test
    void startedManagerRunsMaintenanceAutomatically()
            throws InterruptedException {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        QueueSnapshot candidate =
                snapshotAt(new WalPosition(1, 100));
        CountDownLatch checkpointCommitted =
                new CountDownLatch(1);
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        new WalCompactionBoundaryTracker(),
                        position -> { },
                        position -> checkpointCommitted.countDown()
                );

        try (StorageLifecycleManager manager =
                     new StorageLifecycleManager(
                             candidate::walPosition,
                             () -> candidate,
                             coordinator,
                             new SegmentDistanceCheckpointPolicy(1),
                             Duration.ofMillis(10)
                     )) {
            manager.start();

            assertTrue(
                    checkpointCommitted.await(
                            2,
                            TimeUnit.SECONDS
                    )
            );
            assertEquals(candidate, store.loadLatest().orElseThrow());
        }
    }

    @Test
    void belowPolicyThresholdDoesNotCaptureSnapshot() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        AtomicInteger captures = new AtomicInteger();

        try (StorageLifecycleManager manager = manager(
                new WalPosition(1, 100),
                () -> {
                    captures.incrementAndGet();
                    return snapshotAt(new WalPosition(1, 100));
                },
                store,
                new RecordingCompactor(),
                2
        )) {
            assertFalse(manager.runOnce());
            assertEquals(0, captures.get());
            assertTrue(store.loadLatest().isEmpty());
        }
    }

    @Test
    void policyThresholdCapturesCommitsAndCompactsSnapshot() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        RecordingCompactor compactor = new RecordingCompactor();
        QueueSnapshot candidate =
                snapshotAt(new WalPosition(2, 300));

        try (StorageLifecycleManager manager = manager(
                candidate.walPosition(),
                () -> candidate,
                store,
                compactor,
                2
        )) {
            assertTrue(manager.runOnce());
            assertEquals(candidate, store.loadLatest().orElseThrow());
            assertEquals(
                    List.of(candidate.walPosition()),
                    compactor.positions
            );
            assertEquals(
                    Optional.of(candidate.walPosition()),
                    manager.latestSnapshotPosition()
            );
        }
    }

    @Test
    void compactionFailureAfterSnapshotCommitIsRetriedWithoutNewWalProgress() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        FailFirstCompaction compactor =
                new FailFirstCompaction();
        QueueSnapshot candidate =
                snapshotAt(new WalPosition(2, 300));
        AtomicInteger captures = new AtomicInteger();

        try (StorageLifecycleManager manager = manager(
                candidate.walPosition(),
                () -> {
                    captures.incrementAndGet();
                    return candidate;
                },
                store,
                compactor,
                2
        )) {
            assertThrows(
                    StorageLifecycleException.class,
                    manager::runOnce
            );
            assertEquals(candidate, store.loadLatest().orElseThrow());
            assertTrue(manager.lastFailure().isPresent());

            assertTrue(manager.runOnce());
            assertEquals(1, captures.get());
            assertEquals(2, compactor.calls);
            assertTrue(manager.lastFailure().isEmpty());
        }
    }

    @Test
    void failedSnapshotSaveIsRecapturedOnNextCycle() {
        FailFirstSnapshotStore store =
                new FailFirstSnapshotStore();
        QueueSnapshot candidate =
                snapshotAt(new WalPosition(2, 300));
        AtomicInteger captures = new AtomicInteger();

        try (StorageLifecycleManager manager = manager(
                candidate.walPosition(),
                () -> {
                    captures.incrementAndGet();
                    return candidate;
                },
                store,
                new RecordingCompactor(),
                2
        )) {
            assertThrows(
                    StorageLifecycleException.class,
                    manager::runOnce
            );
            assertTrue(store.loadLatest().isEmpty());

            assertTrue(manager.runOnce());
            assertEquals(2, captures.get());
            assertEquals(candidate, store.loadLatest().orElseThrow());
        }
    }

    @Test
    void startupRetriesCompactionForExistingAuthoritativeSnapshot() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        QueueSnapshot existing =
                snapshotAt(new WalPosition(4, 200));
        store.save(existing);
        RecordingCompactor compactor = new RecordingCompactor();

        try (StorageLifecycleManager manager = manager(
                existing.walPosition(),
                () -> existing,
                store,
                compactor,
                2
        )) {
            assertTrue(manager.runOnce());
            assertEquals(
                    List.of(existing.walPosition()),
                    compactor.positions
            );
        }
    }

    @Test
    void closedManagerRejectsFurtherMaintenance() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        StorageLifecycleManager manager = manager(
                new WalPosition(0, 8),
                () -> snapshotAt(new WalPosition(0, 8)),
                store,
                new RecordingCompactor(),
                2
        );

        manager.close();

        assertThrows(
                IllegalStateException.class,
                manager::runOnce
        );
    }

    private StorageLifecycleManager manager(
            WalPosition currentPosition,
            java.util.function.Supplier<QueueSnapshot> snapshotSupplier,
            QueueSnapshotStore store,
            WalCompactor compactor,
            long segmentDistance
    ) {
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        new WalCompactionBoundaryTracker(),
                        position -> { },
                        compactor
                );

        return new StorageLifecycleManager(
                () -> currentPosition,
                snapshotSupplier,
                coordinator,
                new SegmentDistanceCheckpointPolicy(segmentDistance),
                Duration.ofMinutes(1)
        );
    }

    private QueueSnapshot snapshotAt(
            WalPosition position
    ) {
        return new QueueSnapshot(
                position,
                List.of(),
                List.of(),
                List.of(),
                List.of()
        );
    }

    private static class InMemorySnapshotStore
            implements QueueSnapshotStore {

        private QueueSnapshot latest;

        @Override
        public void save(QueueSnapshot snapshot) {
            latest = snapshot;
        }

        @Override
        public Optional<QueueSnapshot> loadLatest() {
            return Optional.ofNullable(latest);
        }
    }

    private static final class FailFirstSnapshotStore
            extends InMemorySnapshotStore {

        private boolean failed;

        @Override
        public void save(QueueSnapshot snapshot) {
            if (!failed) {
                failed = true;
                throw new SnapshotException("simulated save failure");
            }
            super.save(snapshot);
        }
    }

    private static class RecordingCompactor
            implements WalCompactor {

        private final java.util.ArrayList<WalPosition> positions =
                new java.util.ArrayList<>();

        @Override
        public void compactThrough(WalPosition snapshotPosition) {
            positions.add(snapshotPosition);
        }
    }

    private static final class FailFirstCompaction
            implements WalCompactor {

        private int calls;

        @Override
        public void compactThrough(WalPosition snapshotPosition) {
            calls++;
            if (calls == 1) {
                throw new IllegalStateException(
                        "simulated compaction failure"
                );
            }
        }
    }
}
