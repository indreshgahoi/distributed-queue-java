package io.github.indreshgahoi.queue;

import java.util.Optional;

public interface MessageQueue {

    String publish(String payload);

    Optional<Delivery> receive();

    boolean ack(String receiptHandle);
}
