package io.github.indreshgahoi.queue.metadata.domain.model;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

public record RegisterNodeCommand(
        String nodeId,
        URI endpoint,
        Duration leaseDuration
) {
    public RegisterNodeCommand {
        QueueDescriptor.requireText(nodeId, "nodeId");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "leaseDuration must be positive"
            );
        }
    }
}

