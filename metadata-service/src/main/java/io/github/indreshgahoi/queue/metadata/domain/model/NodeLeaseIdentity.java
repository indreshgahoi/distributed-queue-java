package io.github.indreshgahoi.queue.metadata.domain.model;

public record NodeLeaseIdentity(
        String nodeId,
        long registrationEpoch
) {
    public NodeLeaseIdentity {
        QueueDescriptor.requireText(nodeId, "nodeId");
        if (registrationEpoch <= 0) {
            throw new IllegalArgumentException(
                    "registrationEpoch must be positive"
            );
        }
    }
}

