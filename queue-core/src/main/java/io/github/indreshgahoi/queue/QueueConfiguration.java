package io.github.indreshgahoi.queue;

import java.time.Duration;
import java.util.Objects;

public final class QueueConfiguration {
    public static final int DEFAULT_MAX_MESSAGE_BYTES = 256 * 1024;
    public static final int DEFAULT_MAX_RETAINED_MESSAGES = 100_000;
    public static final long DEFAULT_MAX_RETAINED_BYTES = 1024L * 1024 * 1024;

    private final Duration visibilityTimeout;
    private final int maxDeliveryAttempts;
    private final int maxMessageBytes;
    private final int maxRetainedMessages;
    private final long maxRetainedBytes;

    public QueueConfiguration() {
        this(
                Duration.ofSeconds(30),
                3
        );
    }

    public QueueConfiguration(
            Duration visibilityTimeout,
            int maxDeliveryAttempts
    ) {
        this(
                visibilityTimeout,
                maxDeliveryAttempts,
                DEFAULT_MAX_MESSAGE_BYTES,
                DEFAULT_MAX_RETAINED_MESSAGES,
                DEFAULT_MAX_RETAINED_BYTES
        );
    }

    public QueueConfiguration(
            Duration visibilityTimeout,
            int maxDeliveryAttempts,
            int maxMessageBytes,
            int maxRetainedMessages,
            long maxRetainedBytes
    ) {
        this.visibilityTimeout = Objects.requireNonNull(
                visibilityTimeout,
                "visibilityTimeout"
        );
        this.maxDeliveryAttempts = maxDeliveryAttempts;
        if (maxMessageBytes <= 0) {
            throw new IllegalArgumentException("maxMessageBytes must be positive");
        }
        if (maxRetainedMessages <= 0) {
            throw new IllegalArgumentException("maxRetainedMessages must be positive");
        }
        if (maxRetainedBytes <= 0) {
            throw new IllegalArgumentException("maxRetainedBytes must be positive");
        }
        this.maxMessageBytes = maxMessageBytes;
        this.maxRetainedMessages = maxRetainedMessages;
        this.maxRetainedBytes = maxRetainedBytes;
    }

    public Duration visibilityTimeout() {
        return visibilityTimeout;
    }

    public int maxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    public int maxRetainedMessages() {
        return maxRetainedMessages;
    }

    public long maxRetainedBytes() {
        return maxRetainedBytes;
    }
}
