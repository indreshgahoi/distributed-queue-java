package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Defines the node-local filesystem layout for one partition lineage.
 * Keeping path construction here prevents storage components from silently
 * disagreeing about where the WAL, snapshot, or replica authority is stored.
 */
record LocalPartitionStorageLayout(Path partitionRoot) {

    private static final String WAL_DIRECTORY = "wal";
    private static final String SNAPSHOT_FILE = "snapshot.bin";
    private static final String REPLICA_HARD_STATE_FILE =
            "replica-hard-state.bin";

    LocalPartitionStorageLayout {
        Objects.requireNonNull(partitionRoot, "partitionRoot");
    }

    static LocalPartitionStorageLayout resolve(
            Path storageRoot,
            StorageLineage lineage
    ) {
        Objects.requireNonNull(storageRoot, "storageRoot");
        Objects.requireNonNull(lineage, "lineage");

        Path partitionRoot = storageRoot
                .resolve(lineage.queueId().toString())
                .resolve(lineage.generationId().toString())
                .resolve("partition-" + lineage.partitionId());
        return new LocalPartitionStorageLayout(partitionRoot);
    }

    Path walDirectory() {
        return partitionRoot.resolve(WAL_DIRECTORY);
    }

    Path snapshotFile() {
        return partitionRoot.resolve(SNAPSHOT_FILE);
    }

    Path replicaHardStateFile() {
        return partitionRoot.resolve(REPLICA_HARD_STATE_FILE);
    }
}
