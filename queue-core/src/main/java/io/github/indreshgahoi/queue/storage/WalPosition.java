package io.github.indreshgahoi.queue.storage;

public record WalPosition(
        long segmentId,
        long offset
) implements Comparable<WalPosition> {
    public WalPosition {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                    "segmentId must not be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "offset must not be negative"
            );
        }
    }

    @Override
    public int compareTo(
            WalPosition other
    ) {
        int segmentComparison =
                Long.compare(
                        segmentId,
                        other.segmentId
                );

        if (segmentComparison != 0) {
            return segmentComparison;
        }

        return Long.compare(
                offset,
                other.offset
        );
    }
}