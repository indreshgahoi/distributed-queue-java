package io.github.indreshgahoi.queue;

import java.time.Duration;
import java.util.Optional;

public interface MessageQueue {

    String publish(String payload);

    Optional<Delivery> receive();

    boolean ack(String receiptHandle);

    boolean nack(String receiptHandle, Duration retryDelay);
}
