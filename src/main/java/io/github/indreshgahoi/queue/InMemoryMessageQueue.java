package io.github.indreshgahoi.queue;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class InMemoryMessageQueue implements MessageQueue {

    private final Deque<QueueMessage> ready = new ArrayDeque<>();
    private final Deque<Message> deadLetters = new ArrayDeque<>();
    private final Map<String, InFlightMessage> inFlightByReceiptHandle = new HashMap<>();
    private final Clock clock;
    private final QueueConfiguration config;

    public InMemoryMessageQueue(Clock clock) {
        this.clock = clock;
        this.config = new QueueConfiguration();
    }

    public InMemoryMessageQueue() {
        this.clock = Clock.systemUTC();
        this.config = new QueueConfiguration();
    }

    public InMemoryMessageQueue(Clock clock, QueueConfiguration config) {
        this.clock = clock;
        this.config = config;
    }

    @Override
    public String publish(String payload) {
        String id = UUID.randomUUID().toString();

        ready.addLast(new QueueMessage(new Message(id, payload), 1));

        return id;
    }

    @Override
    public Optional<Delivery> receive() {
        QueueMessage qMessage = ready.pollFirst();
        if (qMessage == null) {
            return Optional.empty();
        }

        Instant leaseUntil = clock.instant()
                .plus(config.visibilityTimeout());

        String receiptHandle = UUID.randomUUID().toString();

        inFlightByReceiptHandle.put(receiptHandle,
                new InFlightMessage(qMessage.message(),
                        receiptHandle,
                        leaseUntil,
                        qMessage.nextAttempt()));
        return Optional.of(new Delivery(qMessage.message(), receiptHandle, qMessage.nextAttempt()));
    }

    @Override
    public boolean ack(String receiptHandle) {
        return inFlightByReceiptHandle.remove(receiptHandle) != null;
    }

    public int requeueExpiredMessages() {
        Iterator<Map.Entry<String, InFlightMessage>> it = inFlightByReceiptHandle.entrySet().iterator();
        Instant now = clock.instant();
        int count = 0;

        while (it.hasNext()) {
            var entry = it.next();

            InFlightMessage inFlight = entry.getValue();

            if (inFlight.leaseUntil().isAfter(now)) {
                continue;
            }
            it.remove(); // invalidate the receipt handle

            if (config.maxDeliveryAttempts() > inFlight.attempt()) {
                ready.addLast(new QueueMessage(inFlight.message(),
                                    inFlight.attempt() + 1));
                count++;
            }else {
                deadLetters.addLast(inFlight.message());
            }
        }
        return count;
    }

    public int deadLetterCount() {
        return deadLetters.size();
    }
}
