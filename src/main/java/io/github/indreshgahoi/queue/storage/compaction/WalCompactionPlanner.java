package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

public interface WalCompactionPlanner {

    CompactionPlan plan(
            WalPosition snapshotPosition
    );
}