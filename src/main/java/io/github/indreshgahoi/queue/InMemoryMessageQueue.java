package io.github.indreshgahoi.queue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class InMemoryMessageQueue implements MessageQueue {
    private final Deque<Message> messages = new ArrayDeque<>();
    private final Map<String, Message> inFlight = new HashMap<>();

    @Override
    public String publish(String payload) {
        String id = UUID.randomUUID().toString();

        messages.addLast(new Message(id, payload));

        return id;
    }

    @Override
    public Optional<Message> receive() {
        Message message = messages.pollFirst();
        if(message == null) {
            return Optional.empty();
        }
        inFlight.put(message.id(), message);
        return Optional.of(message);
    }

    @Override
    public boolean ack(String messageId) {
        return inFlight.remove(messageId) != null;
    }
}
