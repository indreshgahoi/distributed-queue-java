package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.util.List;

public interface FollowerReplicaLog extends AutoCloseable {

    ReplicaAppendResult append(ReplicatedWalEntry entry);

    ReplicaBatchAppendResult appendBatch(
            List<ReplicatedWalEntry> entries
    );

    StorageLineage lineage();

    long lastSequence();

    long highestLeaderEpoch();

    @Override
    void close();
}
