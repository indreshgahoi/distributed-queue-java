package io.github.indreshgahoi.queue.gateway.application.service;

import io.github.indreshgahoi.queue.gateway.application.port.in.RouteQueueOperationUseCase;
import io.github.indreshgahoi.queue.gateway.application.port.out.QueueNodeClient;
import io.github.indreshgahoi.queue.gateway.application.port.out.QueueRouteResolver;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardRequest;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
final class QueueRoutingService implements RouteQueueOperationUseCase {
    private static final String MESSAGES_PATH = "/v1/queues/%s/messages";

    private final QueueRouteResolver routes;
    private final QueueNodeClient nodes;

    QueueRoutingService(QueueRouteResolver routes, QueueNodeClient nodes) {
        this.routes = Objects.requireNonNull(routes, "routes");
        this.nodes = Objects.requireNonNull(nodes, "nodes");
    }

    @Override
    public ForwardResponse publish(
            UUID queueId,
            String body,
            String contentType
    ) {
        return route(queueId, new ForwardRequest(
                ForwardRequest.Method.POST,
                messagesPath(queueId),
                body,
                contentType
        ));
    }

    @Override
    public ForwardResponse receive(UUID queueId) {
        return route(queueId, post(messagesPath(queueId) + "/receive"));
    }

    @Override
    public ForwardResponse ack(UUID queueId, String receiptHandle) {
        return route(queueId, post(
                receiptPath(queueId, receiptHandle) + "/ack"
        ));
    }

    @Override
    public ForwardResponse nack(
            UUID queueId,
            String receiptHandle,
            String body,
            String contentType
    ) {
        return route(queueId, new ForwardRequest(
                ForwardRequest.Method.POST,
                receiptPath(queueId, receiptHandle) + "/nack",
                body,
                contentType
        ));
    }

    private ForwardResponse route(UUID queueId, ForwardRequest request) {
        Objects.requireNonNull(queueId, "queueId");
        QueueRoute route = routes.resolveReadyRoute(queueId);

        // A mutation gets exactly one downstream attempt. A transport failure
        // after WAL commit is ambiguous and must never trigger an implicit
        // second publication on a newly resolved route.
        return nodes.forward(route, request);
    }

    private ForwardRequest post(String path) {
        return new ForwardRequest(
                ForwardRequest.Method.POST,
                path,
                null,
                null
        );
    }

    private String messagesPath(UUID queueId) {
        return MESSAGES_PATH.formatted(queueId);
    }

    private String receiptPath(UUID queueId, String receiptHandle) {
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        if (receiptHandle.isBlank() || receiptHandle.contains("/")) {
            throw new IllegalArgumentException("Invalid receipt handle");
        }
        return messagesPath(queueId) + "/" + receiptHandle;
    }
}
