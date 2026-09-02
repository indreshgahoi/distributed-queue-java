package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.application.port.in.QueueDataPlaneUseCase;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;

@RestController
@RequestMapping("/v1/queues/{queueId}/messages")
final class QueueDataPlaneController {
    private final QueueDataPlaneUseCase dataPlane;

    QueueDataPlaneController(QueueDataPlaneUseCase dataPlane) {
        this.dataPlane = dataPlane;
    }

    @PostMapping
    @Operation(summary = "Publish a message to a local READY queue runtime")
    ResponseEntity<PublishMessageResponse> publish(
            @PathVariable UUID queueId,
            @Valid @RequestBody
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    content = @io.swagger.v3.oas.annotations.media.Content(
                    examples = @ExampleObject(
                            value = "{\"payload\":\"process-order-123\"}"
                    )
            ))
            PublishMessageRequest request
    ) {
        String messageId = dataPlane.publish(queueId, request.payload());
        return ResponseEntity.created(
                URI.create("/v1/queues/" + queueId + "/messages/" + messageId)
        ).body(new PublishMessageResponse(messageId));
    }

    @PostMapping("/receive")
    @Operation(summary = "Receive the next message from a local READY runtime")
    ResponseEntity<MessageDeliveryResponse> receive(
            @PathVariable UUID queueId
    ) {
        return dataPlane.receive(queueId)
                .map(MessageDeliveryResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @PostMapping("/{receiptHandle}/ack")
    @Operation(summary = "Acknowledge the current delivery attempt")
    OperationResultResponse ack(
            @PathVariable UUID queueId,
            @PathVariable String receiptHandle
    ) {
        return new OperationResultResponse(
                dataPlane.ack(queueId, receiptHandle)
        );
    }

    @PostMapping("/{receiptHandle}/nack")
    @Operation(summary = "Reject the current delivery and schedule retry")
    OperationResultResponse nack(
            @PathVariable UUID queueId,
            @PathVariable String receiptHandle,
            @Valid @RequestBody
            NackMessageRequest request
    ) {
        return new OperationResultResponse(
                dataPlane.nack(
                        queueId,
                        receiptHandle,
                        request.retryDelay()
                )
        );
    }

    record PublishMessageRequest(
            @NotNull @Schema(example = "process-order-123") String payload
    ) {
    }

    record PublishMessageResponse(String messageId) {
    }

    record NackMessageRequest(
            @NotNull @Schema(example = "PT30S") Duration retryDelay
    ) {
    }

    record OperationResultResponse(boolean succeeded) {
    }

    record MessageDeliveryResponse(
            String messageId,
            String payload,
            String receiptHandle,
            int attempt
    ) {
        static MessageDeliveryResponse from(MessageDelivery delivery) {
            return new MessageDeliveryResponse(
                    delivery.messageId(),
                    delivery.payload(),
                    delivery.receiptHandle(),
                    delivery.attempt()
            );
        }
    }
}
