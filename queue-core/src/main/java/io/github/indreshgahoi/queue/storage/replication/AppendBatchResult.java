package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.Objects;

public record AppendBatchResult(
        long firstIndex,
        long durableThroughIndex,
        int appendedEntries,
        int alreadyPresentEntries,
        WalPosition durablePosition
) {
    public AppendBatchResult {
        if (firstIndex <= 0 || durableThroughIndex < 0) {
            throw new IllegalArgumentException("Invalid logical append boundary");
        }
        if (appendedEntries < 0 || alreadyPresentEntries < 0) {
            throw new IllegalArgumentException("Entry counts must not be negative");
        }
        Objects.requireNonNull(durablePosition, "durablePosition");
    }
}
