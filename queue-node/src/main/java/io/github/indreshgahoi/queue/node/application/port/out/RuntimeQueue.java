package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;

import java.time.Duration;
import java.util.Optional;

/**
 * Node-facing boundary for one recovered queue partition. The adapter owns the
 * concrete local engine so the application layer does not depend on its
 * storage implementation.
 */
public interface RuntimeQueue extends AutoCloseable {
    String publish(String payload);

    Optional<MessageDelivery> receive();

    boolean ack(String receiptHandle);

    boolean nack(String receiptHandle, Duration retryDelay);

    @Override
    void close();
}
