package io.github.indreshgahoi.queue;

import java.time.Duration;

public class QueueConfiguration {
    private Duration visibilityTimeout = Duration.ofSeconds(30);
    private int maxDeliveryAttempts = 3;

    public Duration visibilityTimeout() {return visibilityTimeout;}
    public int maxDeliveryAttempts() {return maxDeliveryAttempts;}
}
