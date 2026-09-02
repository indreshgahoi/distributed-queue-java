package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeState;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeStatus;

import java.time.Instant;
import java.util.UUID;

record PartitionRuntimeStatusResponse(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        long registrationEpoch,
        long placementEpoch,
        PartitionRuntimeState state,
        String failureReason,
        Instant updatedAt
) {
    static PartitionRuntimeStatusResponse from(
            PartitionRuntimeStatus status
    ) {
        return new PartitionRuntimeStatusResponse(
                status.identity().queueId(),
                status.identity().generationId(),
                status.identity().partitionId(),
                status.identity().nodeId(),
                status.identity().registrationEpoch(),
                status.identity().placementEpoch(),
                status.state(),
                status.failureReason(),
                status.updatedAt()
        );
    }
}
