package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.github.indreshgahoi.queue.metadata.domain.exception.IdempotencyConflictException;
import io.github.indreshgahoi.queue.metadata.domain.exception.MetadataUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueAlreadyExistsException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueMetadataException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.metadata.domain.exception.StaleQueueMetadataException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;

@RestControllerAdvice
final class MetadataExceptionHandler {

    @ExceptionHandler(QueueNotFoundException.class)
    ResponseEntity<ProblemDetail> notFound(
            QueueNotFoundException exception
    ) {
        return problem(
                HttpStatus.NOT_FOUND,
                "queue-not-found",
                exception.getMessage()
        );
    }

    @ExceptionHandler({
            QueueAlreadyExistsException.class,
            IdempotencyConflictException.class,
            StaleQueueMetadataException.class,
            QueueMetadataException.class
    })
    ResponseEntity<ProblemDetail> conflict(
            QueueMetadataException exception
    ) {
        return problem(
                HttpStatus.CONFLICT,
                "metadata-conflict",
                exception.getMessage()
        );
    }

    @ExceptionHandler(MetadataUnavailableException.class)
    ResponseEntity<ProblemDetail> unavailable(
            MetadataUnavailableException exception
    ) {
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE,
                "metadata-unavailable",
                exception.getMessage()
        );
    }

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            ConstraintViolationException.class,
            MissingRequestHeaderException.class,
            IllegalArgumentException.class
    })
    ResponseEntity<ProblemDetail> badRequest(Exception exception) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "invalid-request",
                exception.getMessage()
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
        problem.setType(
                URI.create("urn:distributed-queue:" + type)
        );
        return ResponseEntity.status(status).body(problem);
    }
}
