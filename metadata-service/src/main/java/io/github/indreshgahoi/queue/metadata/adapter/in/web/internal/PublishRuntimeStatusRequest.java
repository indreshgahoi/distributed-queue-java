package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeState;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

record PublishRuntimeStatusRequest(
        @NotBlank String nodeId,
        @Positive long registrationEpoch,
        @Positive long placementEpoch,
        @NotNull PartitionRuntimeState state,
        String failureReason
) {
}
