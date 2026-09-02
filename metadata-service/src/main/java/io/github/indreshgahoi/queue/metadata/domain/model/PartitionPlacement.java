package io.github.indreshgahoi.queue.metadata.domain.model;

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
        QueueDescriptor.requireText(nodeId, "nodeId");
        if (partitionId != 0 || placementEpoch <= 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException(
                    "Invalid partition placement"
            );
        }
    }
}

