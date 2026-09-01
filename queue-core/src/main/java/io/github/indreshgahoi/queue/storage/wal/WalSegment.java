package io.github.indreshgahoi.queue.storage.wal;

import java.nio.file.Path;
import java.util.Objects;

public record WalSegment(
        long segmentId,
        Path path
) {
    public WalSegment {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                    "segmentId must not be negative"
            );
        }

        Objects.requireNonNull(
                path,
                "path"
        );
    }
}