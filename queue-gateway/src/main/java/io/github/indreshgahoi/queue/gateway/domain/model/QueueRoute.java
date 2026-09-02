package io.github.indreshgahoi.queue.gateway.domain.model;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

public record QueueRoute(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        URI nodeEndpoint,
        long placementEpoch,
        long registrationEpoch
) {
    public QueueRoute {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(nodeEndpoint, "nodeEndpoint");
        if (nodeId.isBlank() || partitionId != 0
                || placementEpoch <= 0 || registrationEpoch <= 0) {
            throw new IllegalArgumentException("Invalid queue route");
        }
        String scheme = nodeEndpoint.getScheme();
        if (!nodeEndpoint.isAbsolute()
                || (!("http".equalsIgnoreCase(scheme))
                && !("https".equalsIgnoreCase(scheme)))) {
            throw new IllegalArgumentException(
                    "nodeEndpoint must use HTTP or HTTPS"
            );
        }
    }
}
