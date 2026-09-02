package io.github.indreshgahoi.queue.gateway.adapter.in.web;

import io.github.indreshgahoi.queue.gateway.application.port.in.RouteQueueOperationUseCase;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/queues/{queueId}/messages")
final class QueueGatewayController {
    private final RouteQueueOperationUseCase routing;

    QueueGatewayController(RouteQueueOperationUseCase routing) {
        this.routing = routing;
    }

    @PostMapping
    @Operation(
            summary = "Publish through the stable queue endpoint",
            description = "Resolves one authoritative READY route and makes "
                    + "exactly one forwarding attempt."
    )
    ResponseEntity<String> publish(
            @PathVariable UUID queueId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    content = @io.swagger.v3.oas.annotations.media.Content(
                            examples = @ExampleObject(
                                    value = "{\"payload\":\"process-order-123\"}"
                            )
                    )
            )
            @RequestBody String body,
            HttpServletRequest request
    ) {
        return response(routing.publish(
                queueId,
                body,
                request.getContentType()
        ));
    }

    @PostMapping("/receive")
    @Operation(summary = "Receive through the stable queue endpoint")
    ResponseEntity<String> receive(@PathVariable UUID queueId) {
        return response(routing.receive(queueId));
    }

    @PostMapping("/{receiptHandle}/ack")
    @Operation(summary = "Acknowledge through the stable queue endpoint")
    ResponseEntity<String> ack(
            @PathVariable UUID queueId,
            @PathVariable String receiptHandle
    ) {
        return response(routing.ack(queueId, receiptHandle));
    }

    @PostMapping("/{receiptHandle}/nack")
    @Operation(summary = "Reject through the stable queue endpoint")
    ResponseEntity<String> nack(
            @PathVariable UUID queueId,
            @PathVariable String receiptHandle,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    content = @io.swagger.v3.oas.annotations.media.Content(
                            examples = @ExampleObject(
                                    value = "{\"retryDelay\":\"PT30S\"}"
                            )
                    )
            )
            @RequestBody String body,
            HttpServletRequest request
    ) {
        return response(routing.nack(
                queueId,
                receiptHandle,
                body,
                request.getContentType()
        ));
    }

    private ResponseEntity<String> response(ForwardResponse forwarded) {
        ResponseEntity.BodyBuilder response = ResponseEntity.status(
                HttpStatusCode.valueOf(forwarded.statusCode())
        );
        if (forwarded.contentType() != null) {
            response.contentType(MediaType.parseMediaType(
                    forwarded.contentType()
            ));
        }
        if (forwarded.location() != null) {
            response.header(
                    HttpHeaders.LOCATION,
                    forwarded.location().toString()
            );
        }
        return response.body(forwarded.body());
    }
}
