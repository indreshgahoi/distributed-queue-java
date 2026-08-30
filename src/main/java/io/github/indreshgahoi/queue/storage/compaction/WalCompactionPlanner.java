package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Objects;

public final class WalCompactionPlanner {

    public WalCompactionPlan plan(
            WalPosition snapshotPosition
    ) {
        Objects.requireNonNull(
                snapshotPosition,
                "snapshotPosition"
        );

        if (snapshotPosition.segmentId() == 0) {
            return WalCompactionPlan.nothingToReclaim();
        }

        return WalCompactionPlan.reclaimBefore(
                snapshotPosition.segmentId()
        );
    }
}
