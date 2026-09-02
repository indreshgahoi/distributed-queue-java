package io.github.indreshgahoi.queue.gateway.application.service;

import io.github.indreshgahoi.queue.gateway.application.port.out.QueueNodeClient;
import io.github.indreshgahoi.queue.gateway.application.port.out.QueueRouteResolver;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNodeUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardRequest;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueueRoutingServiceTest {
    private final UUID queueId = UUID.randomUUID();
    private final RecordingRouteResolver routes = new RecordingRouteResolver();
    private final RecordingNodeClient nodes = new RecordingNodeClient();
    private final QueueRoutingService routing = new QueueRoutingService(
            routes,
            nodes
    );

    @Test
    void publishResolvesAndForwardsExactlyOnce() {
        ForwardResponse response = routing.publish(
                queueId,
                "{\"payload\":\"order\"}",
                "application/json"
        );

        assertEquals(1, routes.calls);
        assertEquals(1, nodes.calls);
        assertEquals(201, response.statusCode());
        assertEquals(
                "/v1/queues/" + queueId + "/messages",
                nodes.request.path()
        );
        assertEquals("application/json", nodes.request.contentType());
    }

    @Test
    void transportFailureIsNotRetriedOrResolvedAgain() {
        nodes.failure = new QueueNodeUnavailableException(
                "node-a",
                new IllegalStateException("connection lost")
        );

        assertThrows(
                QueueNodeUnavailableException.class,
                () -> routing.publish(queueId, "{}", "application/json")
        );
        assertEquals(1, routes.calls);
        assertEquals(1, nodes.calls);
    }

    @Test
    void receiptOperationsUseTheResolvedNodePath() {
        routing.ack(queueId, "receipt-1");

        assertEquals(
                "/v1/queues/" + queueId + "/messages/receipt-1/ack",
                nodes.request.path()
        );
    }

    private final class RecordingRouteResolver
            implements QueueRouteResolver {
        private int calls;

        @Override
        public QueueRoute resolveReadyRoute(UUID requestedQueueId) {
            calls++;
            return new QueueRoute(
                    requestedQueueId,
                    UUID.randomUUID(),
                    0,
                    "node-a",
                    URI.create("http://node-a:8081"),
                    3,
                    7
            );
        }
    }

    private static final class RecordingNodeClient
            implements QueueNodeClient {
        private int calls;
        private ForwardRequest request;
        private RuntimeException failure;

        @Override
        public ForwardResponse forward(
                QueueRoute route,
                ForwardRequest request
        ) {
            calls++;
            this.request = request;
            if (failure != null) {
                throw failure;
            }
            return new ForwardResponse(
                    201,
                    "application/json",
                    URI.create(request.path() + "/message-1"),
                    "{\"messageId\":\"message-1\"}"
            );
        }
    }
}
