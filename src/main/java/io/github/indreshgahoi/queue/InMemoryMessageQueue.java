package io.github.indreshgahoi.queue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class InMemoryMessageQueue implements MessageQueue {

    private final Deque<QueueMessage> ready = new ArrayDeque<>();
    private final Deque<Message> deadLetters = new ArrayDeque<>();
    private final Deque<DelayedMessage> delayed = new ArrayDeque<>();
    private final Map<String, InFlightMessage> inFlightByReceiptHandle = new HashMap<>();
    private final Clock clock;
    private final QueueConfiguration config;

    public InMemoryMessageQueue(Clock clock) {
      this(clock, new QueueConfiguration());
    }

    public InMemoryMessageQueue() {
        this(Clock.systemUTC(), new QueueConfiguration());
    }

    public InMemoryMessageQueue(Clock clock, QueueConfiguration config) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
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

    @Override
    public boolean nack(String receiptHandle, Duration retryDelay) {
        validateRetryDelay(retryDelay);

        var inFlight = inFlightByReceiptHandle.remove(receiptHandle);
        if (inFlight == null) {
            return false;
        }
        if (inFlight.attempt() >= config.maxDeliveryAttempts()) {
            deadLetters.addLast(inFlight.message());
            return true;
        }
        Instant retryAt = clock.instant().plus(retryDelay);

        delayed.addLast(new DelayedMessage(inFlight.message(),
                inFlight.attempt() + 1,
                retryAt));

        return true;
    }

    private static void validateRetryDelay(Duration retryDelay) {
        Objects.requireNonNull(retryDelay, "retryDelay");
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "retryDelay must not be negative"
            );
        }
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

            if (inFlight.attempt() >= config.maxDeliveryAttempts()) {
                deadLetters.addLast(inFlight.message());
                continue;
            }

            ready.addLast(
                    new QueueMessage(inFlight.message(),
                            inFlight.attempt() + 1
                    )
            );

            count++;
        }
        return count;
    }

    public int makeDelayedMessagesReady() {
        Instant now = clock.instant();
        int moveToReady = 0;
        for (Iterator<DelayedMessage> it = delayed.iterator(); it.hasNext(); ) {
            DelayedMessage delayedMessage = it.next();
            if (delayedMessage.retryAt().isAfter(now)) {
                continue;
            }
            it.remove();
            ready.addLast(new QueueMessage(delayedMessage.message(), delayedMessage.nextAttempt()));
            moveToReady++;
        }
        return moveToReady;
    }

    public int deadLetterCount() {
        return deadLetters.size();
    }
}
