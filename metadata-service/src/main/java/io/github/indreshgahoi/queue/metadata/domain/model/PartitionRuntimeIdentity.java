package io.github.indreshgahoi.queue.metadata.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Complete authority presented by a node when publishing runtime state.
 * Placement identifies the intended host; registration identifies the exact
 * process incarnation allowed to act for that stable node ID.
 */
public record PartitionRuntimeIdentity(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        long registrationEpoch,
        long placementEpoch
) {
    public PartitionRuntimeIdentity {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        QueueDescriptor.requireText(nodeId, "nodeId");
        if (partitionId != 0 || registrationEpoch <= 0
                || placementEpoch <= 0) {
            throw new IllegalArgumentException(
                    "Invalid partition runtime identity"
            );
        }
    }
}
