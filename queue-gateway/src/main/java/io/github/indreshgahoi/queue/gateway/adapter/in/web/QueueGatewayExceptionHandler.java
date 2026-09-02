package io.github.indreshgahoi.queue.gateway.adapter.in.web;

import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNodeUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.exception.RoutingMetadataUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;

@Slf4j
@RestControllerAdvice
final class QueueGatewayExceptionHandler {
    @ExceptionHandler(QueueNotFoundException.class)
    ResponseEntity<ProblemDetail> notFound(QueueNotFoundException exception) {
        return problem(HttpStatus.NOT_FOUND, "queue-not-found", exception);
    }

    @ExceptionHandler(QueueRouteUnavailableException.class)
    ResponseEntity<ProblemDetail> routeUnavailable(
            QueueRouteUnavailableException exception
    ) {
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE,
                "queue-route-unavailable",
                exception
        );
    }

    @ExceptionHandler(RoutingMetadataUnavailableException.class)
    ResponseEntity<ProblemDetail> metadataUnavailable(
            RoutingMetadataUnavailableException exception
    ) {
        log.warn("event=gateway_routing_metadata_unavailable", exception);
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE,
                "routing-metadata-unavailable",
                exception
        );
    }

    @ExceptionHandler(QueueNodeUnavailableException.class)
    ResponseEntity<ProblemDetail> nodeUnavailable(
            QueueNodeUnavailableException exception
    ) {
        log.warn("event=gateway_queue_node_unreachable", exception);
        return problem(
                HttpStatus.BAD_GATEWAY,
                "queue-node-unreachable",
                exception
        );
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> invalid(IllegalArgumentException exception) {
        return problem(HttpStatus.BAD_REQUEST, "invalid-request", exception);
    }

    @ExceptionHandler(RuntimeException.class)
    ResponseEntity<ProblemDetail> internal(RuntimeException exception) {
        log.error("event=gateway_operation_failed", exception);
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "gateway-operation-failed",
                new RuntimeException("Gateway operation failed")
        );
    }

    private ResponseEntity<ProblemDetail> problem(
            HttpStatus status,
            String type,
            RuntimeException exception
    ) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                status,
                exception.getMessage()
        );
        problem.setType(URI.create("urn:distributed-queue:" + type));
        return ResponseEntity.status(status).body(problem);
    }
}
