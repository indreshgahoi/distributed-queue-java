package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Objects;
import java.util.Optional;

public final class WalCompactionBoundaryTracker {

    private WalPosition boundary;

    public synchronized void advanceTo(
            WalPosition snapshotPosition
    ) {
        Objects.requireNonNull(
                snapshotPosition,
                "snapshotPosition"
        );

        if (boundary == null) {
            boundary = snapshotPosition;
            return;
        }

        /*
         * Compaction boundary is monotonic.
         *
         * Stale snapshots must never move it backward.
         */
        if (snapshotPosition.compareTo(boundary) <= 0) {
            return;
        }

        boundary = snapshotPosition;
    }

    public synchronized Optional<WalPosition> currentBoundary() {
        return Optional.ofNullable(
                boundary
        );
    }

}
