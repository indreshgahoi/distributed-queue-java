package io.github.indreshgahoi.queue.node.domain.model;

import java.time.Instant;
import java.util.Objects;

public record NodeRegistration(
        String nodeId,
        long registrationEpoch,
        Instant leaseExpiresAt
) {
    public NodeRegistration {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        if (registrationEpoch <= 0) {
            throw new IllegalArgumentException(
                    "registrationEpoch must be positive"
            );
        }
    }
}

