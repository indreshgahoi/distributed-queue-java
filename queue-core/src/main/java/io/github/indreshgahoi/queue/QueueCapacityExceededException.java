package io.github.indreshgahoi.queue;

public final class QueueCapacityExceededException extends RuntimeException {
    public QueueCapacityExceededException(String limit) {
        super("queue capacity limit reached: " + limit);
    }
}
