package io.github.indreshgahoi.queue.storage.compaction;

public record WalCompactionPlan(
        long boundarySegmentId,
        boolean hasReclaimableSegments
) {

    public WalCompactionPlan {
        if (hasReclaimableSegments && boundarySegmentId <= 0) {
            throw new IllegalArgumentException(
                    "reclaimable boundarySegmentId must be > 0"
            );
        }
    }

    public static WalCompactionPlan nothingToReclaim() {

        return new WalCompactionPlan(
                -1,
                false
        );
    }

    public static WalCompactionPlan reclaimBefore(
            long boundarySegmentId
    ) {
        return new WalCompactionPlan(
                boundarySegmentId,
                true
        );
    }
}
