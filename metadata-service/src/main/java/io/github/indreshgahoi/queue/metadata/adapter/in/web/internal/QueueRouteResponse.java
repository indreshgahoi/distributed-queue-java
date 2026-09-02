package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.QueueRoute;

import java.net.URI;
import java.util.UUID;

record QueueRouteResponse(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String nodeId,
        URI nodeEndpoint,
        long placementEpoch,
        long registrationEpoch
) {
    static QueueRouteResponse from(QueueRoute route) {
        return new QueueRouteResponse(
                route.queueId(),
                route.generationId(),
                route.partitionId(),
                route.nodeId(),
                route.nodeEndpoint(),
                route.placementEpoch(),
                route.registrationEpoch()
        );
    }
}
