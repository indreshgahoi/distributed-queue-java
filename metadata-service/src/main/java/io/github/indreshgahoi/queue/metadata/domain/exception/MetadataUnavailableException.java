package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class MetadataUnavailableException
        extends QueueMetadataException {

    public MetadataUnavailableException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
