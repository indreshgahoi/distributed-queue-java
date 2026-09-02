package io.github.indreshgahoi.queue.node.domain.model;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.util.Objects;
import java.util.UUID;

public record PartitionPlacement(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        long placementEpoch,
        long metadataVersion
) {
    public PartitionPlacement {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank() || partitionId != 0
                || placementEpoch <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("Invalid partition placement");
        }
    }

    public StorageLineage lineage() {
        return new StorageLineage(queueId, generationId, partitionId);
    }
}
