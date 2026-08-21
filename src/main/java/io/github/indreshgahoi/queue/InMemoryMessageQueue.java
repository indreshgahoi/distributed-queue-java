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
import java.util.concurrent.locks.ReentrantLock;

public final class InMemoryMessageQueue implements MessageQueue {

    private final Deque<QueueMessage> ready = new ArrayDeque<>();
    private final Deque<Message> deadLetters = new ArrayDeque<>();
    private final Deque<DelayedMessage> delayed = new ArrayDeque<>();

    private final Map<String, InFlightMessage> inFlightByReceiptHandle =
            new HashMap<>();

    /**
     * Protects the queue state machine, not individual collections.
     *
     * Every transition between READY, IN_FLIGHT, DELAYED,
     * DEAD_LETTER and DONE is performed while holding this lock.
     */
    private final ReentrantLock lock = new ReentrantLock();

    private final Clock clock;
    private final QueueConfiguration config;

    public InMemoryMessageQueue() {
        this(
                Clock.systemUTC(),
                new QueueConfiguration()
        );
    }

    public InMemoryMessageQueue(Clock clock) {
        this(
                clock,
                new QueueConfiguration()
        );
    }

    public InMemoryMessageQueue(
            Clock clock,
            QueueConfiguration config
    ) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public String publish(String payload) {
        lock.lock();

        try {
            String messageId =
                    UUID.randomUUID().toString();

            Message message =
                    new Message(messageId, payload);

            ready.addLast(
                    new QueueMessage(
                            message,
                            1
                    )
            );

            return messageId;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Delivery> receive() {
        lock.lock();

        try {
            QueueMessage queuedMessage =
                    ready.pollFirst();

            if (queuedMessage == null) {
                return Optional.empty();
            }

            String receiptHandle =
                    UUID.randomUUID().toString();

            Instant leaseUntil =
                    clock.instant()
                            .plus(config.visibilityTimeout());

            InFlightMessage inFlight =
                    new InFlightMessage(
                            queuedMessage.message(),
                            receiptHandle,
                            leaseUntil,
                            queuedMessage.nextAttempt()
                    );

            inFlightByReceiptHandle.put(
                    receiptHandle,
                    inFlight
            );

            return Optional.of(
                    new Delivery(
                            queuedMessage.message(),
                            receiptHandle,
                            queuedMessage.nextAttempt()
                    )
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean ack(String receiptHandle) {
        Objects.requireNonNull(
                receiptHandle,
                "receiptHandle"
        );

        lock.lock();

        try {
            return inFlightByReceiptHandle
                    .remove(receiptHandle) != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean nack(
            String receiptHandle,
            Duration retryDelay
    ) {
        Objects.requireNonNull(
                receiptHandle,
                "receiptHandle"
        );

        Objects.requireNonNull(
                retryDelay,
                "retryDelay"
        );

        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "retryDelay must not be negative"
            );
        }

        lock.lock();

        try {
            InFlightMessage inFlight =
                    inFlightByReceiptHandle
                            .remove(receiptHandle);

            if (inFlight == null) {
                return false;
            }

            if (inFlight.attempt()
                    >= config.maxDeliveryAttempts()) {

                deadLetters.addLast(
                        inFlight.message()
                );

                return true;
            }

            Instant retryAt =
                    clock.instant()
                            .plus(retryDelay);

            delayed.addLast(
                    new DelayedMessage(
                            inFlight.message(),
                            inFlight.attempt() + 1,
                            retryAt
                    )
            );

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int requeueExpiredMessages() {
        lock.lock();

        try {
            Instant now = clock.instant();
            int requeuedCount = 0;

            Iterator<Map.Entry<String, InFlightMessage>>
                    iterator =
                    inFlightByReceiptHandle
                            .entrySet()
                            .iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, InFlightMessage> entry =
                        iterator.next();

                InFlightMessage inFlight =
                        entry.getValue();

                if (inFlight.leaseUntil().isAfter(now)) {
                    continue;
                }

                /*
                 * The old delivery no longer owns the message.
                 * Removing it also invalidates its receipt handle.
                 */
                iterator.remove();

                if (inFlight.attempt()
                        >= config.maxDeliveryAttempts()) {

                    deadLetters.addLast(
                            inFlight.message()
                    );

                    continue;
                }

                ready.addLast(
                        new QueueMessage(
                                inFlight.message(),
                                inFlight.attempt() + 1
                        )
                );

                requeuedCount++;
            }

            return requeuedCount;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int makeDelayedMessagesReady() {
        lock.lock();

        try {
            Instant now = clock.instant();
            int movedCount = 0;

            Iterator<DelayedMessage> iterator =
                    delayed.iterator();

            while (iterator.hasNext()) {
                DelayedMessage delayedMessage =
                        iterator.next();

                if (delayedMessage.retryAt().isAfter(now)) {
                    continue;
                }

                iterator.remove();

                ready.addLast(
                        new QueueMessage(
                                delayedMessage.message(),
                                delayedMessage.nextAttempt()
                        )
                );

                movedCount++;
            }

            return movedCount;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int deadLetterCount() {
        lock.lock();

        try {
            return deadLetters.size();
        } finally {
            lock.unlock();
        }
    }
}