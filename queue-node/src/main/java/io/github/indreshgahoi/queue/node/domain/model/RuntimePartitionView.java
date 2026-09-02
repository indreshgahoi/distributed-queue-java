package io.github.indreshgahoi.queue.node.domain.model;

public record RuntimePartitionView(
        RuntimePartitionIdentity identity,
        RuntimePartitionState state,
        String failureReason
) {
}
