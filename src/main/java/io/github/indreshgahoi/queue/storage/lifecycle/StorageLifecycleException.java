package io.github.indreshgahoi.queue.storage.lifecycle;

public final class StorageLifecycleException
        extends RuntimeException {

    public StorageLifecycleException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
