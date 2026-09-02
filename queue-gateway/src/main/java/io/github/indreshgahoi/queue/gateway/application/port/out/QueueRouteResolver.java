package io.github.indreshgahoi.queue.gateway.application.port.out;

import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;

import java.util.UUID;

public interface QueueRouteResolver {
    QueueRoute resolveReadyRoute(UUID queueId);
}
