package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.util.Objects;

/**
 * One leader-issued WAL entry. Sequence numbers are monotonic within a
 * storage lineage and do not reset when the leader epoch changes.
 */
public record ReplicatedWalEntry(
        StorageLineage lineage,
        long leaderEpoch,
        long sequence,
        WalRecord record
) {
    public ReplicatedWalEntry {
        Objects.requireNonNull(lineage, "lineage");
        Objects.requireNonNull(record, "record");

        if (leaderEpoch <= 0) {
            throw new IllegalArgumentException(
                    "leaderEpoch must be positive"
            );
        }

        if (sequence <= 0) {
            throw new IllegalArgumentException(
                    "sequence must be positive"
            );
        }
    }
}
