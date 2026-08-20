package io.github.indreshgahoi.queue;

public record Delivery(Message message,
                       String receiptHandle,
                       int attempt) {
}
