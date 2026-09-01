package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Objects;
import java.util.Optional;

public final class SegmentDistanceCheckpointPolicy
        implements CheckpointPolicy {

    private final long segmentDistance;

    public SegmentDistanceCheckpointPolicy(
            long segmentDistance
    ) {
        if (segmentDistance <= 0) {
            throw new IllegalArgumentException(
                    "segmentDistance must be greater than zero"
            );
        }

        this.segmentDistance = segmentDistance;
    }

    @Override
    public boolean shouldCheckpoint(
            Optional<WalPosition> latestSnapshotPosition,
            WalPosition currentWalPosition
    ) {
        Objects.requireNonNull(
                latestSnapshotPosition,
                "latestSnapshotPosition"
        );
        Objects.requireNonNull(
                currentWalPosition,
                "currentWalPosition"
        );

        long baselineSegment =
                latestSnapshotPosition
                        .map(WalPosition::segmentId)
                        .orElse(0L);

        return currentWalPosition.segmentId()
                - baselineSegment
                >= segmentDistance;
    }
}
