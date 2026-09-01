package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

@FunctionalInterface
public interface SnapshotPositionValidator {

    void validate(WalPosition position);
}
