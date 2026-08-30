package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.SnapshotException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class WalCompactionBoundaryTrackerTest {

    @Test
    void compactionBoundaryComesFromAuthoritativeSnapshot() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        QueueSnapshot authoritativeSnapshot =
                snapshotAt(
                        new WalPosition(
                                0,
                                1_000
                        )
                );

        tracker.advanceTo(
                authoritativeSnapshot.walPosition()
        );

        assertEquals(
                new WalPosition(
                        0,
                        1_000
                ),
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void compactionNeverCrossesSnapshotPosition() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        WalPosition snapshotPosition =
                new WalPosition(
                        0,
                        5_000
                );

        tracker.advanceTo(
                snapshotPosition
        );

        WalPosition boundary =
                tracker.currentBoundary()
                        .orElseThrow();

        /*
         * Boundary must be exactly the authoritative
         * snapshot position.
         *
         * The tracker is not allowed to invent:
         *
         * (0, 5001)
         *
         * or anything beyond the snapshot.
         */
        assertEquals(
                snapshotPosition,
                boundary
        );

        assertTrue(
                boundary.offset()
                        <= snapshotPosition.offset()
        );
    }

    @Test
    void newerSnapshotMayAdvanceCompactionBoundary() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        WalPosition first =
                new WalPosition(
                        0,
                        1_000
                );

        WalPosition second =
                new WalPosition(
                        0,
                        2_000
                );

        tracker.advanceTo(first);

        assertEquals(
                first,
                tracker.currentBoundary()
                        .orElseThrow()
        );

        tracker.advanceTo(second);

        assertEquals(
                second,
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void staleSnapshotCannotMoveCompactionBoundaryBackward() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        WalPosition newer =
                new WalPosition(
                        0,
                        5_000
                );

        WalPosition stale =
                new WalPosition(
                        0,
                        2_000
                );

        tracker.advanceTo(
                newer
        );

        tracker.advanceTo(
                stale
        );

        assertEquals(
                newer,
                tracker.currentBoundary()
                        .orElseThrow()
        );
    }

    @Test
    void segmentedWalPositionCanBecomeCompactionBoundary() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        WalPosition segmented =
                new WalPosition(
                        1,
                        100
                );

        tracker.advanceTo(segmented);

        assertEquals(
                segmented,
                tracker.currentBoundary().orElseThrow()
        );
    }

    @Test
    void noSnapshotMeansNoCompaction() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        /*
         * No authoritative snapshot has ever
         * advanced the tracker.
         */
        assertTrue(
                tracker.currentBoundary()
                        .isEmpty()
        );
    }

    @Test
    void failedSnapshotSaveDoesNotAdvanceCompactionBoundary() {

        WalCompactionBoundaryTracker tracker =
                new WalCompactionBoundaryTracker();

        WalPosition firstPosition =
                new WalPosition(
                        0,
                        1_000
                );

        WalPosition secondPosition =
                new WalPosition(
                        0,
                        2_000
                );

        /*
         * S1 was successfully committed previously.
         */
        tracker.advanceTo(
                firstPosition
        );

        QueueSnapshot second =
                snapshotAt(
                        secondPosition
                );

        QueueSnapshotStore failingStore =
                new QueueSnapshotStore() {

                    @Override
                    public void save(
                            QueueSnapshot snapshot
                    ) {
                        throw new SnapshotException(
                                "Simulated snapshot save failure"
                        );
                    }

                    @Override
                    public Optional<QueueSnapshot> loadLatest() {
                        return Optional.of(
                                snapshotAt(
                                        firstPosition
                                )
                        );
                    }
                };

        assertThrows(
                SnapshotException.class,
                () -> {
                    /*
                     * Important orchestration rule:
                     *
                     * save first.
                     *
                     * Only successful save/promotion
                     * permits boundary advancement.
                     */
                    failingStore.save(
                            second
                    );

                    tracker.advanceTo(
                            second.walPosition()
                    );
                }
        );

        /*
         * Since save failed, advanceTo(P2) was
         * never reached.
         */
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
                position,
                List.of(),
                List.of(),
                List.of(),
                List.of()
        );
    }
}
