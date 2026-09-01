package io.github.indreshgahoi.queue;

import java.time.Duration;
import java.util.Objects;

public class QueueConfiguration {
    private Duration visibilityTimeout = Duration.ofSeconds(30);
    private int maxDeliveryAttempts = 3;

    public QueueConfiguration() {

    }

    public QueueConfiguration(Duration visibilityTimeout, int maxDeliveryAttempts) {
        this.visibilityTimeout = Objects.requireNonNull(visibilityTimeout, "visibilityTimeout cannot be) null");
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public Duration visibilityTimeout() {return visibilityTimeout;}
    public int maxDeliveryAttempts() {return maxDeliveryAttempts;}
}
