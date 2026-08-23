package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.internal.DelayedMessage;
import io.github.indreshgahoi.queue.internal.InFlightMessage;
import io.github.indreshgahoi.queue.internal.QueueMessage;
import io.github.indreshgahoi.queue.internal.RecoveryState;
import io.github.indreshgahoi.queue.internal.RecoveryStatus;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalException;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public final class LocalMessageQueue
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
    public LocalMessageQueue() {
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
    public LocalMessageQueue(Clock clock) {
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
    public LocalMessageQueue(
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
    public LocalMessageQueue(
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
            InFlightMessage inFlight =
                    inFlightByReceiptHandle.get(receiptHandle);

            if (inFlight == null) {
                return false;
            }

            wal.append(
                    new WalRecord(
                            WalRecordType.ACK,
                            inFlight.message().id(),
                            null,
                            receiptHandle,
                            inFlight.attempt(),
                            clock.instant()
                    )
            );

            inFlightByReceiptHandle.remove(receiptHandle);

            return true;

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
            InFlightMessage inFlight = inFlightByReceiptHandle.get(receiptHandle);
            if (inFlight == null) {
                return false;
            }
            Instant retryAt =
                    clock.instant()
                            .plus(retryDelay);
            boolean isMoveToDeadLetter = inFlight.attempt() >= config.maxDeliveryAttempts();
            wal.append(
                    new WalRecord(
                            isMoveToDeadLetter ? WalRecordType.DEAD_LETTER: WalRecordType.NACK,
                            inFlight.message().id(),
                            null, // NACK doest need to persist payload
                            receiptHandle,
                            inFlight.attempt() + 1,
                            retryAt
                    )
            );

            inFlightByReceiptHandle.remove(receiptHandle);

            if (isMoveToDeadLetter) {

                deadLetters.addLast(
                        inFlight.message()
                );

                return true;
            }


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
                boolean isMoveToDeadLetter = inFlight.attempt() >= config.maxDeliveryAttempts();
                int nextAttempt =
                        inFlight.attempt() + 1;

                /*
                 * WAL FIRST.
                 *
                 * If this fails, iterator.remove() is never reached.
                 * Therefore the current delivery remains IN_FLIGHT
                 * and its receipt handle remains valid.
                 */

                wal.append(new WalRecord(
                        isMoveToDeadLetter ? WalRecordType.DEAD_LETTER: WalRecordType.LEASE_EXPIRED,
                        inFlight.message().id(),
                        null, // NACK doest need to persist payload
                        inFlight.receiptHandle(),
                        inFlight.attempt() + 1,
                        now
                ));
                /*
                 * Lease expired.
                 *
                 * Removing the entry also invalidates
                 * the old receipt handle.
                 */
                iterator.remove();

                if (isMoveToDeadLetter) {

                    deadLetters.addLast(
                            inFlight.message()
                    );

                    continue;
                }

                ready.addLast(
                        new QueueMessage(
                                inFlight.message(),
                                nextAttempt
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
        Map<String, RecoveryState> states =
                new LinkedHashMap<>();

        for (WalRecord record : wal.readAll()) {

            switch (record.type()) {

                case PUBLISH -> {
                    Message message =
                            new Message(
                                    record.messageId(),
                                    record.payload()
                            );

                    states.put(
                            record.messageId(),
                            new RecoveryState(
                                    message,
                                    RecoveryStatus.READY,
                                    record.attempt(),
                                    null
                            )
                    );
                }

                case ACK -> {
                    RecoveryState current =
                            requireExistingState(
                                    states,
                                    record.messageId()
                            );

                    states.put(
                            record.messageId(),
                            new RecoveryState(
                                    current.message(),
                                    RecoveryStatus.DONE,
                                    current.attempt(),
                                    null
                            )
                    );
                }

                case NACK -> {
                    RecoveryState current =
                            requireExistingState(
                                    states,
                                    record.messageId()
                            );

                        states.put(
                                record.messageId(),
                                new RecoveryState(
                                        current.message(),
                                        RecoveryStatus.DELAYED,
                                        record.attempt(),
                                        record.timestamp()
                                )
                        );

                }
                case DEAD_LETTER -> {
                    RecoveryState current =
                            requireExistingState(
                                    states,
                                    record.messageId()
                            );

                    states.put(
                            record.messageId(),
                            new RecoveryState(
                                    current.message(),
                                    RecoveryStatus.DEAD_LETTER,
                                    record.attempt(),
                                    null
                            )
                    );
                }
                case LEASE_EXPIRED -> {
                    RecoveryState current =
                            requireExistingState(
                                    states,
                                    record.messageId()
                            );

                    states.put(
                            record.messageId(),
                            new RecoveryState(
                                    current.message(),
                                    RecoveryStatus.READY,
                                    record.attempt(),
                                    null
                            )
                    );
                }
            }
        }

        materializeRecoveredState(states);
    }

    private void materializeRecoveredState(
            Map<String, RecoveryState> states
    ) {
        for (RecoveryState state : states.values()) {

            switch (state.status()) {

                case READY -> ready.addLast(
                        new QueueMessage(
                                state.message(),
                                state.attempt()
                        )
                );

                case DELAYED -> delayed.addLast(
                        new DelayedMessage(
                                state.message(),
                                state.attempt(),
                                state.retryAt()
                        )
                );

                case DEAD_LETTER -> deadLetters.addLast(
                        state.message()
                );

                case DONE -> {
                    // Nothing to restore.
                }
            }
        }
    }

    private RecoveryState requireExistingState(
            Map<String, RecoveryState> states,
            String messageId
    ) {
        RecoveryState state = states.get(messageId);

        if (state == null) {
            throw new WalException(
                    "WAL references unknown message: "
                            + messageId
            );
        }

        return state;
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