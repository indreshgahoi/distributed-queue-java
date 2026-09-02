package io.github.indreshgahoi.queue.metadata.domain.model;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

/**
 * Fenced observation of the node currently serving one queue partition.
 * The route is discovery information, not a transfer of storage authority;
 * the target node must still validate its local runtime before admission.
 */
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
        QueueDescriptor.requireText(nodeId, "nodeId");
        Objects.requireNonNull(nodeEndpoint, "nodeEndpoint");
        if (partitionId != 0
                || placementEpoch <= 0
                || registrationEpoch <= 0) {
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
