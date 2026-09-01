package io.github.indreshgahoi.queue.metadata.domain.exception;

public class QueueMetadataException extends RuntimeException {

    public QueueMetadataException(String message) {
        super(message);
    }

    public QueueMetadataException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
