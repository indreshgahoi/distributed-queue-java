package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Optional;

@FunctionalInterface
public interface CheckpointPolicy {

    boolean shouldCheckpoint(
            Optional<WalPosition> latestSnapshotPosition,
            WalPosition currentWalPosition
    );
}
