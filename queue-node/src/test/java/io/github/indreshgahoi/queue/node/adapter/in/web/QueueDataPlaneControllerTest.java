package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.application.port.in.QueueDataPlaneUseCase;
import io.github.indreshgahoi.queue.node.domain.exception.RuntimePartitionUnavailableException;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class QueueDataPlaneControllerTest {
    private final UUID queueId = UUID.randomUUID();
    private final FakeDataPlane dataPlane = new FakeDataPlane();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                        new QueueDataPlaneController(dataPlane)
                )
                .setControllerAdvice(new QueueNodeExceptionHandler())
                .build();
    }

    @Test
    void publishReturnsCreatedMessageIdentity() throws Exception {
        mvc.perform(post("/v1/queues/{queueId}/messages", queueId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"process-order-123\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string(
                        "Location",
                        "/v1/queues/" + queueId + "/messages/message-1"
                ))
                .andExpect(jsonPath("$.messageId").value("message-1"));
    }

    @Test
    void receiveDistinguishesDeliveryFromEmptyQueue() throws Exception {
        dataPlane.delivery = Optional.of(new MessageDelivery(
                "message-1",
                "payload",
                "receipt-1",
                2
        ));
        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/receive",
                        queueId
                ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.receiptHandle").value("receipt-1"))
                .andExpect(jsonPath("$.attempt").value(2));

        dataPlane.delivery = Optional.empty();
        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/receive",
                        queueId
                ))
                .andExpect(status().isNoContent());
    }

    @Test
    void ackAndNackExposeReceiptOutcome() throws Exception {
        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/{receipt}/ack",
                        queueId,
                        "receipt-1"
                ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.succeeded").value(true));

        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/{receipt}/nack",
                        queueId,
                        "stale"
                ).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"retryDelay\":\"PT30S\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.succeeded").value(false));
    }

    @Test
    void unavailableRuntimeUsesProblemDetail() throws Exception {
        dataPlane.available = false;

        mvc.perform(post(
                        "/v1/queues/{queueId}/messages/receive",
                        queueId
                ))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.type").value(
                        "urn:distributed-queue:runtime-partition-unavailable"
                ));
    }

    @Test
    void invalidRequestIsRejectedBeforeUseCase() throws Exception {
        mvc.perform(post("/v1/queues/{queueId}/messages", queueId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value(
                        "urn:distributed-queue:invalid-request"
                ));
    }

    private final class FakeDataPlane implements QueueDataPlaneUseCase {
        private Optional<MessageDelivery> delivery = Optional.empty();
        private boolean available = true;

        @Override
        public String publish(UUID ignored, String payload) {
            requireAvailable();
            return "message-1";
        }

        @Override
        public Optional<MessageDelivery> receive(UUID ignored) {
            requireAvailable();
            return delivery;
        }

        @Override
        public boolean ack(UUID ignored, String receiptHandle) {
            requireAvailable();
            return "receipt-1".equals(receiptHandle);
        }

        @Override
        public boolean nack(
                UUID ignored,
                String receiptHandle,
                Duration retryDelay
        ) {
            requireAvailable();
            return "receipt-1".equals(receiptHandle);
        }

        private void requireAvailable() {
            if (!available) {
                throw new RuntimePartitionUnavailableException(queueId);
            }
        }
    }
}
