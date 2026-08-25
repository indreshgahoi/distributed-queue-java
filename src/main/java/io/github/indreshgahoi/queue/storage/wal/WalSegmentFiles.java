package io.github.indreshgahoi.queue.storage.wal;

import java.nio.file.Path;

public final class WalSegmentFiles {

    private static final String PREFIX = "segment-";
    private static final String SUFFIX = ".wal";

    public Path pathFor(
            Path walDirectory,
            long segmentId
    ) {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                    "segmentId must not be negative"
            );
        }
        return walDirectory.resolve(
                "segment-%06d.wal"
                        .formatted(segmentId)
        );
    }

    public Path tempPathFor(
            Path walDirectory,
            long segmentId
    ) {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                    "segmentId must not be negative"
            );
        }
        return walDirectory.resolve(
                "segment-%06d.tmp"
                        .formatted(segmentId)
        );
    }
}