package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.SnapshotException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotCompactionCoordinatorTest {

    private static final StorageLineage LINEAGE =
            StorageLineage.create();

    @Test
    void foreignSnapshotCannotBecomeAuthoritativeOrAuthorizeCompaction() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();
        RecordingWalCompactor compactor =
                new RecordingWalCompactor();
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { },
                        compactor
                );
        QueueSnapshot foreign =
                new QueueSnapshot(
                        StorageLineage.create(),
                        new WalPosition(2, 400),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of()
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.commitSnapshot(foreign)
        );
        assertTrue(store.loadLatest().isEmpty());
        assertTrue(tracker.currentBoundary().isEmpty());
        assertTrue(compactor.positions.isEmpty());
    }

    @Test
    void committedSnapshotReconstructsBoundaryAfterCoordinatorRestart() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        QueueSnapshot committed =
                snapshotAt(new WalPosition(8, 100));
        store.save(committed);

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        assertEquals(
                committed.walPosition(),
                tracker.currentBoundary().orElseThrow()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(new WalPosition(5, 300))
                )
        );
        assertEquals(committed, store.loadLatest().orElseThrow());
    }

    @Test
    void invalidWalPositionIsRejectedBeforeSnapshotPromotion() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        QueueSnapshot committed =
                snapshotAt(new WalPosition(2, 100));
        store.save(committed);

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> {
                            if (position.segmentId() == 3) {
                                throw new IllegalArgumentException(
                                        "invalid WAL position"
                                );
                            }
                        }
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(new WalPosition(3, 100))
                )
        );
        assertEquals(committed, store.loadLatest().orElseThrow());
        assertEquals(
                committed.walPosition(),
                tracker.currentBoundary().orElseThrow()
        );
    }

    @Test
    void successfulSnapshotCommitAuthorizesWalCompaction() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();
        RecordingWalCompactor compactor =
                new RecordingWalCompactor();
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { },
                        compactor
                );

        WalPosition position = new WalPosition(2, 400);
        coordinator.commitSnapshot(snapshotAt(position));

        assertEquals(List.of(position), compactor.positions);
    }

    @Test
    void failedOrStaleSnapshotDoesNotAuthorizeWalCompaction() {
        FailOnSecondSaveSnapshotStore store =
                new FailOnSecondSaveSnapshotStore();
        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();
        RecordingWalCompactor compactor =
                new RecordingWalCompactor();
        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { },
                        compactor
                );

        WalPosition committed = new WalPosition(1, 100);
        coordinator.commitSnapshot(snapshotAt(committed));

        assertThrows(
                SnapshotException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(new WalPosition(2, 100))
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(new WalPosition(0, 100))
                )
        );

        assertEquals(List.of(committed), compactor.positions);
    }

    @Test
    void successfulSnapshotCommitAdvancesBoundary() {
        InMemorySnapshotStore store =
                new InMemorySnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        QueueSnapshot snapshot =
                snapshotAt(
                        new WalPosition(
                                0,
                                1_000
                        )
                );

        coordinator.commitSnapshot(
                snapshot
        );

        assertEquals(
                snapshot,
                store.loadLatest()
                        .orElseThrow()
        );

        assertEquals(
                snapshot.walPosition(),
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void failedSnapshotCommitDoesNotAdvanceBoundary() {
        WalPosition existing =
                new WalPosition(
                        0,
                        1_000
                );

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        tracker.advanceTo(existing);

        QueueSnapshotStore failingStore =
                new QueueSnapshotStore() {

                    @Override
                    public void save(
                            QueueSnapshot snapshot
                    ) {
                        throw new SnapshotException(
                                "Simulated save failure"
                        );
                    }

                    @Override
                    public Optional<QueueSnapshot> loadLatest() {
                        return Optional.empty();
                    }
                };

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        failingStore,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        assertThrows(
                SnapshotException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(
                                new WalPosition(
                                        0,
                                        2_000
                                )
                        )
                )
        );

        assertEquals(
                existing,
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void staleSnapshotDoesNotReplaceNewerAuthoritativeSnapshot() {
        InMemorySnapshotStore store =
                new InMemorySnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        QueueSnapshot newer =
                snapshotAt(
                        new WalPosition(
                                0,
                                5_000
                        )
                );

        QueueSnapshot stale =
                snapshotAt(
                        new WalPosition(
                                0,
                                2_000
                        )
                );

        coordinator.commitSnapshot(
                newer
        );

        /*
         * Recommended coordinator policy:
         *
         * stale snapshots must be rejected BEFORE
         * snapshotStore.save().
         */
        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.commitSnapshot(
                        stale
                )
        );

        assertEquals(
                newer,
                store.loadLatest()
                        .orElseThrow()
        );

        assertEquals(
                newer.walPosition(),
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void equalSnapshotPositionIsIdempotent() {
        InMemorySnapshotStore store =
                new InMemorySnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        WalPosition position =
                new WalPosition(
                        0,
                        1_000
                );

        QueueSnapshot first =
                snapshotAt(position);

        QueueSnapshot second =
                snapshotAt(position);

        coordinator.commitSnapshot(first);
        coordinator.commitSnapshot(second);

        assertEquals(
                position,
                tracker.currentBoundary()
                        .orElseThrow()
        );

        assertEquals(
                position,
                store.loadLatest()
                        .orElseThrow()
                        .walPosition()
        );
    }

    @Test
    void concurrentSnapshotCommitsDoNotLeaveSnapshotBehindBoundary()
            throws Exception {

        BlockingSnapshotStore store =
                new BlockingSnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        QueueSnapshot older =
                snapshotAt(
                        new WalPosition(
                                0,
                                1_000
                        )
                );

        QueueSnapshot newer =
                snapshotAt(
                        new WalPosition(
                                0,
                                2_000
                        )
                );

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        try {
            Future<?> first =
                    executor.submit(
                            () -> coordinator.commitSnapshot(
                                    older
                            )
                    );

            /*
             * Wait until the first commit enters save().
             */
            store.firstSaveEntered.await();

            Future<?> second =
                    executor.submit(
                            () -> coordinator.commitSnapshot(
                                    newer
                            )
                    );

            /*
             * If commitSnapshot() is synchronized,
             * second cannot enter save() yet.
             */
            assertEquals(
                    1,
                    store.saveCallCount
            );

            /*
             * Allow first commit to finish.
             */
            store.allowFirstSaveToFinish.countDown();

            first.get();
            second.get();

            assertEquals(
                    newer,
                    store.loadLatest()
                            .orElseThrow()
            );

            assertEquals(
                    newer.walPosition(),
                    tracker.currentBoundary()
                            .orElseThrow()
            );

        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentCommitsAreSerializedInCoordinator()
            throws Exception {

        BlockingSnapshotStore store =
                new BlockingSnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        QueueSnapshot firstSnapshot =
                snapshotAt(
                        new WalPosition(
                                0,
                                1_000
                        )
                );

        QueueSnapshot secondSnapshot =
                snapshotAt(
                        new WalPosition(
                                0,
                                2_000
                        )
                );

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        try {
            Future<?> first =
                    executor.submit(
                            () -> coordinator.commitSnapshot(
                                    firstSnapshot
                            )
                    );

            store.firstSaveEntered.await();

            Future<?> second =
                    executor.submit(
                            () -> coordinator.commitSnapshot(
                                    secondSnapshot
                            )
                    );

            /*
             * Give second thread a chance to run.
             *
             * It must be blocked on coordinator monitor,
             * not inside snapshotStore.save().
             */
            Thread.sleep(50);

            assertEquals(
                    1,
                    store.saveCallCount,
                    "Second commit entered snapshot store before first commit completed"
            );

            store.allowFirstSaveToFinish.countDown();

            first.get();
            second.get();

            assertEquals(
                    2,
                    store.saveCallCount
            );

        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void laterFailedCommitDoesNotDamageEarlierSuccessfulCommit() {
        WalPosition firstPosition =
                new WalPosition(
                        0,
                        1_000
                );

        QueueSnapshot first =
                snapshotAt(
                        firstPosition
                );

        FailOnSecondSaveSnapshotStore store =
                new FailOnSecondSaveSnapshotStore();

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        SnapshotCompactionCoordinator coordinator =
                new SnapshotCompactionCoordinator(
                        store,
                        tracker,
                        LINEAGE,
                        position -> { }
                );

        coordinator.commitSnapshot(first);

        assertThrows(
                SnapshotException.class,
                () -> coordinator.commitSnapshot(
                        snapshotAt(
                                new WalPosition(
                                        0,
                                        2_000
                                )
                        )
                )
        );

        /*
         * S1 must remain the committed recovery point.
         */
        assertEquals(
                first,
                store.loadLatest()
                        .orElseThrow()
        );

        assertEquals(
                firstPosition,
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    private QueueSnapshot snapshotAt(
            WalPosition position
    ) {
        return new QueueSnapshot(
                LINEAGE,
                position,
                List.of(),
                List.of(),
                List.of(),
                List.of()
        );
    }

    private static final class InMemorySnapshotStore
            implements QueueSnapshotStore {

        private QueueSnapshot latest;

        @Override
        public synchronized void save(
                QueueSnapshot snapshot
        ) {
            latest = snapshot;
        }

        @Override
        public synchronized Optional<QueueSnapshot> loadLatest() {
            return Optional.ofNullable(
                    latest
            );
        }
    }

    private static final class BlockingSnapshotStore
            implements QueueSnapshotStore {

        private final CountDownLatch firstSaveEntered =
                new CountDownLatch(1);

        private final CountDownLatch allowFirstSaveToFinish =
                new CountDownLatch(1);

        private volatile int saveCallCount;

        private QueueSnapshot latest;

        @Override
        public synchronized void save(
                QueueSnapshot snapshot
        ) {
            saveCallCount++;

            if (saveCallCount == 1) {
                firstSaveEntered.countDown();

                try {
                    allowFirstSaveToFinish.await();

                } catch (InterruptedException e) {
                    Thread.currentThread()
                            .interrupt();

                    throw new RuntimeException(e);
                }
            }

            latest = snapshot;
        }

        @Override
        public synchronized Optional<QueueSnapshot> loadLatest() {
            return Optional.ofNullable(
                    latest
            );
        }
    }

    private static final class FailOnSecondSaveSnapshotStore
            implements QueueSnapshotStore {

        private int saveCount;
        private QueueSnapshot latest;

        @Override
        public void save(
                QueueSnapshot snapshot
        ) {
            saveCount++;

            if (saveCount == 2) {
                throw new SnapshotException(
                        "Simulated second snapshot failure"
                );
            }

            latest = snapshot;
        }

        @Override
        public Optional<QueueSnapshot> loadLatest() {
            return Optional.ofNullable(
                    latest
            );
        }
    }

    private static final class RecordingWalCompactor
            implements WalCompactor {

        private final List<WalPosition> positions =
                new ArrayList<>();

        @Override
        public void compactThrough(
                WalPosition snapshotPosition
        ) {
            positions.add(snapshotPosition);
        }
    }
}
