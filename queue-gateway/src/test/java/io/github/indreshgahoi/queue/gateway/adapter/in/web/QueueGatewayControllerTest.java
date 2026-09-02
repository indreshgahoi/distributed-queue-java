package io.github.indreshgahoi.queue.gateway.adapter.in.web;

import io.github.indreshgahoi.queue.gateway.application.port.in.RouteQueueOperationUseCase;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.URI;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class QueueGatewayControllerTest {
    private final UUID queueId = UUID.randomUUID();
    private final FakeRouting routing = new FakeRouting();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                        new QueueGatewayController(routing)
                )
                .setControllerAdvice(new QueueGatewayExceptionHandler())
                .build();
    }

    @Test
    void publishPreservesDownstreamStatusBodyAndPublicLocation()
            throws Exception {
        routing.response = new ForwardResponse(
                201,
                "application/json",
                URI.create("/v1/queues/" + queueId + "/messages/message-1"),
                "{\"messageId\":\"message-1\"}"
        );

        mvc.perform(post("/v1/queues/{queueId}/messages", queueId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"order\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string(
                        "Location",
                        "/v1/queues/" + queueId + "/messages/message-1"
                ))
                .andExpect(content().json(
                        "{\"messageId\":\"message-1\"}"
                ));
    }

    @Test
    void capacityProblemDetailIsPreserved() throws Exception {
        routing.response = new ForwardResponse(
                429,
                "application/problem+json",
                null,
                "{\"type\":\"urn:distributed-queue:"
                        + "queue-capacity-exceeded\"}"
        );

        mvc.perform(post("/v1/queues/{queueId}/messages", queueId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"order\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.type").value(
                        "urn:distributed-queue:queue-capacity-exceeded"
                ));
    }

    @Test
    void missingReadyRouteReturnsServiceUnavailable() throws Exception {
        routing.failure = new QueueRouteUnavailableException(queueId);

        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/receive",
                        queueId
                ))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.type").value(
                        "urn:distributed-queue:queue-route-unavailable"
                ));
    }

    private static final class FakeRouting
            implements RouteQueueOperationUseCase {
        private ForwardResponse response = new ForwardResponse(
                204,
                null,
                null,
                null
        );
        private RuntimeException failure;

        @Override
        public ForwardResponse publish(
                UUID queueId,
                String body,
                String contentType
        ) {
            return result();
        }

        @Override
        public ForwardResponse receive(UUID queueId) {
            return result();
        }

        @Override
        public ForwardResponse ack(UUID queueId, String receiptHandle) {
            return result();
        }

        @Override
        public ForwardResponse nack(
                UUID queueId,
                String receiptHandle,
                String body,
                String contentType
        ) {
            return result();
        }

        private ForwardResponse result() {
            if (failure != null) {
                throw failure;
            }
            return response;
        }
    }
}
