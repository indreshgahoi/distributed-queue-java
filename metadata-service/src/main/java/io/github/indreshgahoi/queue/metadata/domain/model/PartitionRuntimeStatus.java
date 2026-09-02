package io.github.indreshgahoi.queue.metadata.domain.model;

import java.time.Instant;
import java.util.Objects;

public record PartitionRuntimeStatus(
        PartitionRuntimeIdentity identity,
        PartitionRuntimeState state,
        String failureReason,
        Instant updatedAt
) {
    public PartitionRuntimeStatus {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (state == PartitionRuntimeState.READY
                && failureReason != null) {
            throw new IllegalArgumentException(
                    "READY runtime cannot have a failure reason"
            );
        }
        if (state == PartitionRuntimeState.FAILED
                && (failureReason == null || failureReason.isBlank())) {
            throw new IllegalArgumentException(
                    "FAILED runtime requires a failure reason"
            );
        }
    }
}
