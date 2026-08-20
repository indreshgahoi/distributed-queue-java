package io.github.indreshgahoi.queue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class InMemoryMessageQueue implements MessageQueue {
    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(30);



    private final Deque<Message> ready = new ArrayDeque<>();
    private final Map<String, InFlightMessage> inFlight = new HashMap<>();
    private final Clock clock ;

    public InMemoryMessageQueue(Clock clock) {
        this.clock = clock;
    }
    public InMemoryMessageQueue() {
        this.clock = Clock.systemUTC();
    }

    @Override
    public String publish(String payload) {
        String id = UUID.randomUUID().toString();

        ready.addLast(new Message(id, payload));

        return id;
    }

    @Override
    public Optional<Message> receive() {
        Message message = ready.pollFirst();
        if(message == null) {
            return Optional.empty();
        }

        Instant leaseUntil = clock.instant()
                .plus(VISIBILITY_TIMEOUT);

        inFlight.put(message.id(),
                new InFlightMessage(message, leaseUntil));
        return Optional.of(message);
    }

    @Override
    public boolean ack(String messageId) {
        return inFlight.remove(messageId) != null;
    }

    public int requeueExpireMessages() {
        Iterator<Map.Entry<String, InFlightMessage>> it = inFlight.entrySet().iterator();
        Instant now = clock.instant();
        int count = 0;

        while(it.hasNext()) {
            var entry = it.next();
            if(!entry.getValue().leaseUntil().isAfter(now)) {
                ready.addLast(entry.getValue().message());
                it.remove();
                count++;
            }
        }
        return count;
    }
}
