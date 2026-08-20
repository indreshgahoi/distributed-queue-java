package io.github.indreshgahoi.queue;

public record QueueMessage(Message message,
                           int nextAttempt) {
}
