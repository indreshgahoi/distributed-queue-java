package io.github.indreshgahoi.queue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public final class InMemoryMessageQueue
        implements MessageQueue, AutoCloseable {

    private final Deque<QueueMessage> ready =
            new ArrayDeque<>();

    private final Deque<Message> deadLetters =
            new ArrayDeque<>();

    private final Deque<DelayedMessage> delayed =
            new ArrayDeque<>();

    private final Map<String, InFlightMessage>
            inFlightByReceiptHandle =
            new HashMap<>();

    private final ReentrantLock lock =
            new ReentrantLock();

    private final Clock clock;
    private final QueueConfiguration config;
    private final WriteAheadLog wal;

    /*
     * Convenience constructor.
     *
     * Uses an in-memory WAL, so this version does NOT
     * survive JVM restart.
     */
    public InMemoryMessageQueue() {
        this(
                Clock.systemUTC(),
                new QueueConfiguration(),
                new InMemoryWriteAheadLog()
        );
    }

    /*
     * Useful mainly for deterministic tests where
     * the caller supplies a Clock.
     *
     * Still non-durable across JVM restart.
     */
    public InMemoryMessageQueue(Clock clock) {
        this(
                clock,
                new QueueConfiguration(),
                new InMemoryWriteAheadLog()
        );
    }

    /*
     * Allows custom queue configuration while still
     * using an in-memory WAL.
     */
    public InMemoryMessageQueue(
            Clock clock,
            QueueConfiguration config
    ) {
        this(
                clock,
                config,
                new InMemoryWriteAheadLog()
        );
    }

    /*
     * Canonical constructor.
     *
     * Important architectural choices are explicit here:
     *
     * Clock
     * Queue configuration
     * WAL implementation / durability strategy
     */
    public InMemoryMessageQueue(
            Clock clock,
            QueueConfiguration config,
            WriteAheadLog wal
    ) {
        this.clock =
                Objects.requireNonNull(
                        clock,
                        "clock"
                );

        this.config =
                Objects.requireNonNull(
                        config,
                        "config"
                );

        this.wal =
                Objects.requireNonNull(
                        wal,
                        "wal"
                );

        recover();
    }

    @Override
    public String publish(String payload) {
        Objects.requireNonNull(
                payload,
                "payload"
        );

        lock.lock();

        try {
            String messageId =
                    UUID.randomUUID().toString();

            Message message =
                    new Message(
                            messageId,
                            payload
                    );

            WalRecord record =
                    new WalRecord(
                            WalRecordType.PUBLISH,
                            message.id(),
                            message.payload(),
                            null,
                            1,
                            clock.instant()
                    );

            /*
             * WAL first.
             *
             * If this fails, the in-memory state
             * remains unchanged.
             */
            wal.append(record);

            /*
             * Volatile projection second.
             */
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
                            .plus(
                                    config.visibilityTimeout()
                            );

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
            /*
             * ACK is still memory-only in v0.8.1.
             *
             * We deliberately make durable ACK
             * a later step.
             */
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
            Instant now =
                    clock.instant();

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

                if (inFlight.leaseUntil()
                        .isAfter(now)) {

                    continue;
                }

                /*
                 * Lease expired.
                 *
                 * Removing the entry also invalidates
                 * the old receipt handle.
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
            Instant now =
                    clock.instant();

            int movedCount = 0;

            Iterator<DelayedMessage> iterator =
                    delayed.iterator();

            while (iterator.hasNext()) {
                DelayedMessage delayedMessage =
                        iterator.next();

                if (delayedMessage.retryAt()
                        .isAfter(now)) {

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

    private void recover() {
        List<WalRecord> records =
                wal.readAll();

        for (WalRecord record : records) {
            applyRecoveredRecord(record);
        }
    }

    private void applyRecoveredRecord(
            WalRecord record
    ) {
        switch (record.type()) {

            case PUBLISH ->
                    recoverPublish(record);

            default ->
                    throw new WalException(
                            "Unsupported WAL record type during recovery: "
                                    + record.type()
                    );
        }
    }

    private void recoverPublish(
            WalRecord record
    ) {
        Message message =
                new Message(
                        record.messageId(),
                        record.payload()
                );

        ready.addLast(
                new QueueMessage(
                        message,
                        record.attempt()
                )
        );
    }

    @Override
    public void close() {
        lock.lock();

        try {
            wal.close();

        } finally {
            lock.unlock();
        }
    }
}