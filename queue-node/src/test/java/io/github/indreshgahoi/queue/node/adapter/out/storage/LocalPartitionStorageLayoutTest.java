package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalPartitionStorageLayoutTest {

    @Test
    void resolvesEveryPartitionArtifactFromTheSameLineageRoot() {
        UUID queueId = UUID.fromString(
                "00000000-0000-0000-0000-000000000001"
        );
        UUID generationId = UUID.fromString(
                "00000000-0000-0000-0000-000000000002"
        );
        LocalPartitionStorageLayout layout =
                LocalPartitionStorageLayout.resolve(
                        Path.of("storage"),
                        new StorageLineage(queueId, generationId, 7)
                );
        Path expectedRoot = Path.of(
                "storage",
                queueId.toString(),
                generationId.toString(),
                "partition-7"
        );

        assertEquals(expectedRoot, layout.partitionRoot());
        assertEquals(expectedRoot.resolve("wal"), layout.walDirectory());
        assertEquals(
                expectedRoot.resolve("snapshot.bin"),
                layout.snapshotFile()
        );
        assertEquals(
                expectedRoot.resolve("replica-hard-state.bin"),
                layout.replicaHardStateFile()
        );
    }
}
