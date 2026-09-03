package io.github.indreshgahoi.queue.node.domain.model;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.ReplicatedWalEntry;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A bounded transport unit. Sequence is represented once as the first index;
 * every following record occupies the next index and cannot encode a gap.
 */
public record ReplicaWalBatch(
        StorageLineage lineage,
        long leaderEpoch,
        long firstSequence,
        List<WalRecord> records
) {
    public ReplicaWalBatch {
        Objects.requireNonNull(lineage, "lineage");
        records = List.copyOf(
                Objects.requireNonNull(records, "records")
        );
        if (leaderEpoch <= 0) {
            throw new IllegalArgumentException(
                    "leaderEpoch must be positive"
            );
        }
        if (firstSequence <= 0) {
            throw new IllegalArgumentException(
                    "firstSequence must be positive"
            );
        }
        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                    "records must not be empty"
            );
        }
    }

    public List<ReplicatedWalEntry> entries() {
        return IntStream.range(0, records.size())
                .mapToObj(index -> new ReplicatedWalEntry(
                        lineage,
                        leaderEpoch,
                        Math.addExact(firstSequence, index),
                        records.get(index)
                ))
                .toList();
    }
}
