package io.github.indreshgahoi.queue.internal;

import io.github.indreshgahoi.queue.Message;

public record QueueMessage(Message message,
                           int nextAttempt) {
}
