package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionPlan;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactionPlanner;
import io.github.indreshgahoi.queue.storage.compaction.WalCompactor;

import java.util.Objects;

public final class WalCompactionCoordinator
        implements WalCompactor {

    private final WalCompactionPlanner planner;
    private final WalSegmentReclaimer reclaimer;

    public WalCompactionCoordinator(
            WalCompactionPlanner planner,
            WalSegmentReclaimer reclaimer
    ) {
        this.planner =
                Objects.requireNonNull(
                        planner,
                        "planner"
                );

        this.reclaimer =
                Objects.requireNonNull(
                        reclaimer,
                        "reclaimer"
                );
    }

    @Override
    public void compactThrough(
            WalPosition snapshotPosition
    ) {
        Objects.requireNonNull(
                snapshotPosition,
                "snapshotPosition"
        );

        WalCompactionPlan plan =
                planner.plan(
                        snapshotPosition
                );

        if (!plan.hasReclaimableSegments()) {
            return;
        }

        reclaimer.reclaimBefore(
                plan.boundarySegmentId()
        );
    }
}
