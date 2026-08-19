package io.github.indreshgahoi.queue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.UUID;

public class InMemoryMessageQueue implements MessageQueue {
    private final Deque<Message> messages = new ArrayDeque<>();

    @Override
    public String publish(String payload) {
        String id = UUID.randomUUID().toString();

        messages.addLast(new Message(id, payload));

        return id;
    }

    @Override
    public Optional<Message> receive() {
        return Optional.ofNullable(messages.pollFirst());
    }
}
