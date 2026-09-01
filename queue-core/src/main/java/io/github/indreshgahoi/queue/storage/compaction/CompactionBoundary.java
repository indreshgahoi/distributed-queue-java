package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;

public record CompactionBoundary(
        WalPosition position
) {
    public CompactionBoundary {
        if (position == null) {
            throw new NullPointerException(
                    "position"
            );
        }
    }
}