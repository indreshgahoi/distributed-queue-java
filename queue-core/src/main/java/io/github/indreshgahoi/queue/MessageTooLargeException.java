package io.github.indreshgahoi.queue;

public final class MessageTooLargeException extends RuntimeException {
    public MessageTooLargeException(int actualBytes, int maximumBytes) {
        super(
                "message payload is " + actualBytes
                        + " bytes; maximum is " + maximumBytes
        );
    }
}
