package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;

import java.util.UUID;

record PartitionPlacementResponse(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        long placementEpoch,
        long metadataVersion
) {
    static PartitionPlacementResponse from(PartitionPlacement placement) {
        return new PartitionPlacementResponse(
                placement.queueId(),
                placement.generationId(),
                placement.partitionId(),
                placement.nodeId(),
                placement.placementEpoch(),
                placement.metadataVersion()
        );
    }
}

