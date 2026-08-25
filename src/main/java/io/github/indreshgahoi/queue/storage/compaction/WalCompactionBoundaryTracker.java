package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Objects;
import java.util.Optional;

public final class WalCompactionBoundaryTracker {

    private WalPosition boundary;

    /*
     * Current WAL implementation supports only segment 0.
     *
     * This validation will evolve once segmented WAL
     * support is introduced.
     */
    private static final long CURRENT_SEGMENT_ID = 0;

    public synchronized void advanceTo(
            WalPosition snapshotPosition
    ) {
        Objects.requireNonNull(
                snapshotPosition,
                "snapshotPosition"
        );

        validateSupportedPosition(
                snapshotPosition
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

    private void validateSupportedPosition(
            WalPosition position
    ) {
        if (position.segmentId()
                != CURRENT_SEGMENT_ID) {

            throw new IllegalArgumentException(
                    "Unsupported WAL segment for compaction: "
                            + position.segmentId()
            );
        }
    }
}