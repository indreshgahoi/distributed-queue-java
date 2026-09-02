package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.domain.exception.RuntimePartitionUnavailableException;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;

@RestControllerAdvice
@Slf4j
final class QueueNodeExceptionHandler {
    @ExceptionHandler(RuntimePartitionUnavailableException.class)
    ResponseEntity<ProblemDetail> unavailable(
            RuntimePartitionUnavailableException exception
    ) {
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE,
                "runtime-partition-unavailable",
                exception.getMessage()
        );
    }

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            ConstraintViolationException.class,
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class,
            IllegalArgumentException.class
    })
    ResponseEntity<ProblemDetail> badRequest(Exception exception) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "invalid-request",
                exception.getMessage()
        );
    }

    @ExceptionHandler(RuntimeException.class)
    ResponseEntity<ProblemDetail> internalFailure(RuntimeException exception) {
        log.error("event=data_plane_operation_failed", exception);
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "data-plane-operation-failed",
                "the durable queue operation failed"
        );
    }

    private ResponseEntity<ProblemDetail> problem(
            HttpStatus status,
            String type,
            String detail
    ) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                status,
                detail
        );
        problem.setType(URI.create("urn:distributed-queue:" + type));
        return ResponseEntity.status(status).body(problem);
    }
}
