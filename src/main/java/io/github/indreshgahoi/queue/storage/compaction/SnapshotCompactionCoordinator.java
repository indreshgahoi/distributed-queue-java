package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;

import java.util.Objects;
import java.util.Optional;

public final class SnapshotCompactionCoordinator {

    private final QueueSnapshotStore snapshotStore;
    private final WalCompactionBoundaryTracker boundaryTracker;
    private final WalCompactor walCompactor;

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker
    ) {
        this(
                snapshotStore,
                boundaryTracker,
                new NoOpWalCompactor()
        );
    }

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker,
            WalCompactor walCompactor
    ) {
        this.snapshotStore =
                Objects.requireNonNull(
                        snapshotStore,
                        "snapshotStore"
                );

        this.boundaryTracker =
                Objects.requireNonNull(
                        boundaryTracker,
                        "boundaryTracker"
                );
        this.walCompactor = Objects.requireNonNull(
                walCompactor,
                "walCompactor"
        );
    }

    public synchronized void commitSnapshot(
            QueueSnapshot snapshot
    ) {
        Objects.requireNonNull(
                snapshot,
                "snapshot"
        );

        WalPosition candidate =
                snapshot.walPosition();

        /*
         * Reject stale snapshots BEFORE they can
         * replace the authoritative snapshot.
         */
        Optional<WalPosition> current =
                boundaryTracker.currentBoundary();

        if (current.isPresent()
                && candidate.compareTo(current.get()) < 0
        ) {

            throw new IllegalArgumentException(
                    "Snapshot position "
                            + candidate
                            + " is older than current compaction boundary "
                            + current.get()
            );
        }

        /*
         * Only after validation may the snapshot
         * become authoritative.
         */
        snapshotStore.save(snapshot);

        /*
         * Successful snapshot promotion allows
         * the boundary to advance.
         */
        boundaryTracker.advanceTo(
                candidate
        );

        /*
         * Only the successfully promoted snapshot may authorize deletion.
         * Reclamation failure does not invalidate that recovery point and is
         * safe to retry because every deletion remains before its segment.
         */
        walCompactor.compactThrough(
                candidate
        );
    }
}
