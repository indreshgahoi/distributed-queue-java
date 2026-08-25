package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;

import java.util.Objects;
import java.util.Optional;

public final class SnapshotCompactionCoordinator {

    private final QueueSnapshotStore snapshotStore;
    private final WalCompactionBoundaryTracker boundaryTracker;

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker
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
    }
}