package io.github.indreshgahoi.queue.node.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Immutable proof under which a local queue was recovered. A stable node ID
 * alone is insufficient because two process incarnations may use that ID, and
 * a placement alone is insufficient because it may be superseded.
 */
public record RuntimePartitionIdentity(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        long registrationEpoch,
        long placementEpoch
) {
    public RuntimePartitionIdentity {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank() || partitionId != 0
                || registrationEpoch <= 0 || placementEpoch <= 0) {
            throw new IllegalArgumentException(
                    "Invalid runtime partition identity"
            );
        }
    }

    public static RuntimePartitionIdentity from(
            PartitionPlacement placement,
            NodeRegistration registration
    ) {
        return new RuntimePartitionIdentity(
                placement.queueId(),
                placement.generationId(),
                placement.partitionId(),
                placement.nodeId(),
                registration.registrationEpoch(),
                placement.placementEpoch()
        );
    }
}
