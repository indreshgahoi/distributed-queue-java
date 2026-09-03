package io.github.indreshgahoi.queue.node.domain.model;

public record ReplicaWalBatchResult(
        long acceptedThroughSequence,
        int appendedEntries,
        int alreadyPresentEntries
) {
}
