package io.github.indreshgahoi.queue.storage.replication;

public record ReplicaBatchAppendResult(
        long acceptedThroughSequence,
        int appendedEntries,
        int alreadyPresentEntries
) {
}
