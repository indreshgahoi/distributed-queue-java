package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.internal.DelayedMessage;
import io.github.indreshgahoi.queue.internal.InFlightMessage;
import io.github.indreshgahoi.queue.internal.QueueMessage;
import io.github.indreshgahoi.queue.internal.RecoveryState;
import io.github.indreshgahoi.queue.internal.RecoveryStatus;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.DeadLetterSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.DelayedSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.InFlightSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.ReadySnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.SnapshotException;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalException;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final Optional<QueueSnapshotStore> snapshotStore;
    private int retainedMessages;
    private long retainedPayloadBytes;

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
                new InMemoryWriteAheadLog(),
                Optional.empty()
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
                new InMemoryWriteAheadLog(),
                Optional.empty()
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
                new InMemoryWriteAheadLog(),
                Optional.empty()
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
        this(
                clock,
                config,
                wal,
                Optional.empty()
        );
    }

    public LocalMessageQueue(
            Clock clock,
            QueueConfiguration config,
            WriteAheadLog wal,
            QueueSnapshotStore snapshotStore
    ) {
        this(
                clock,
                config,
                wal,
                Optional.of(
                        Objects.requireNonNull(
                                snapshotStore,
                                "snapshotStore"
                        )
                )
        );
    }

    private LocalMessageQueue(
            Clock clock,
            QueueConfiguration config,
            WriteAheadLog wal,
            Optional<QueueSnapshotStore> snapshotStore
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

        this.snapshotStore =
                Objects.requireNonNull(
                        snapshotStore,
                        "snapshotStore"
                );

        recover();
    }

    @Override
    public String publish(String payload) {
        Objects.requireNonNull(
                payload,
                "payload"
        );

        int payloadBytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > config.maxMessageBytes()) {
            throw new MessageTooLargeException(
                    payloadBytes,
                    config.maxMessageBytes()
            );
        }

        lock.lock();

        try {
            requirePublishCapacity(payloadBytes);

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
            retainedMessages++;
            retainedPayloadBytes += payloadBytes;

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
                    ready.peekFirst();

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
            /*
             * Persist ownership before exposing or
             * mutating the runtime queue state.
             */
            wal.append(
                    new WalRecord(
                            WalRecordType.LEASE_STARTED,
                            queuedMessage.message().id(),
                            null,
                            receiptHandle,
                            queuedMessage.nextAttempt(),
                            leaseUntil
                    )
            );
            /*
             * WAL succeeded.
             *
             * We can now commit READY -> IN_FLIGHT.
             */
            ready.removeFirst();

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
            retainedMessages--;
            retainedPayloadBytes -= payloadBytes(inFlight.message());

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
                            isMoveToDeadLetter ? WalRecordType.DEAD_LETTER : WalRecordType.NACK,
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
                        isMoveToDeadLetter ? WalRecordType.DEAD_LETTER : WalRecordType.LEASE_EXPIRED,
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

    public QueueSnapshot captureSnapshot() {
        lock.lock();

        try {
            WalPosition position =
                    wal.currentDurablePosition();

            List<ReadySnapshotEntry> readySnapshot =
                    ready.stream()
                            .map(q -> new ReadySnapshotEntry(
                                    q.message().id(),
                                    q.message().payload(),
                                    q.nextAttempt()
                            ))
                            .toList();

            List<InFlightSnapshotEntry> inFlightSnapshot =
                    inFlightByReceiptHandle.values()
                            .stream()
                            .map(inFlight -> new InFlightSnapshotEntry(
                                    inFlight.message().id(),
                                    inFlight.message().payload(),
                                    inFlight.receiptHandle(),
                                    inFlight.attempt(),
                                    inFlight.leaseUntil()
                            ))
                            .toList();

            List<DelayedSnapshotEntry> delayedSnapshot =
                    delayed.stream()
                            .map(d -> new DelayedSnapshotEntry(
                                    d.message().id(),
                                    d.message().payload(),
                                    d.nextAttempt(),
                                    d.retryAt()
                            ))
                            .toList();

            List<DeadLetterSnapshotEntry> deadLetterSnapshot =
                    deadLetters.stream()
                            .map(m -> new DeadLetterSnapshotEntry(
                                    m.id(),
                                    m.payload()
                            ))
                            .toList();

            return new QueueSnapshot(
                    wal.storageLineage(),
                    position,
                    wal instanceof io.github.indreshgahoi.queue.storage.replication.ReplicatedLog replicatedLog
                            ? replicatedLog.lastLogPoint().logIndex()
                            : 0,
                    wal instanceof io.github.indreshgahoi.queue.storage.replication.ReplicatedLog replicatedLog
                            ? replicatedLog.lastLogPoint().logTerm()
                            : 0,
                    readySnapshot,
                    inFlightSnapshot,
                    delayedSnapshot,
                    deadLetterSnapshot
            );

        } finally {
            lock.unlock();
        }
    }


    private void recover() {
        Map<String, RecoveryState> states =
                new LinkedHashMap<>();
        List<WalRecord> recordsToReplay;
        if (snapshotStore.isPresent()) {
            Optional<QueueSnapshot> snapshot =
                    loadSnapshotSafely();
            if (snapshot.isPresent()) {
                QueueSnapshot queueSnapshot =
                        snapshot.get();

                validateSnapshotLineage(
                        queueSnapshot
                );
                if (wal instanceof io.github.indreshgahoi.queue.storage.replication.ReplicatedLog replicatedLog) {
                    replicatedLog.restoreSnapshotBoundary(
                            new io.github.indreshgahoi.queue.storage.replication.LogPoint(
                                    queueSnapshot.lastIncludedIndex(),
                                    queueSnapshot.lastIncludedTerm()
                            )
                    );
                }
                validateSnapshotLogicalBoundary(queueSnapshot);

                /*
                 * Establish snapshot as the recovery baseline.
                 */
                restoreSnapshotIntoRecoveryState(
                        queueSnapshot,
                        states
                );

                /*
                 * Replay only history NOT already represented
                 * by the snapshot.
                 *
                 * readFrom() validates that WalPosition points
                 * to a real WAL frame boundary.
                 */
                recordsToReplay =
                        wal.readFrom(
                                queueSnapshot.walPosition()
                        );

            } else {
                /*
                 * WAL-only recovery is valid only while the WAL still
                 * contains its initial history. Once segment 0 has been
                 * reclaimed, the snapshot is part of the durable authority
                 * chain and cannot be treated as optional.
                 */
                requireCompleteWalHistoryForFallback();
                recordsToReplay =
                        wal.readAll();
            }

        } else {
            /*
             * Snapshot support was not configured.
             */
            recordsToReplay =
                    wal.readAll();
        }

        applyWalRecords(
                states,
                recordsToReplay
        );

        materializeRecoveredState(states);
        restoreCapacityAccounting(states);
    }

    private void validateSnapshotLogicalBoundary(
            QueueSnapshot snapshot
    ) {
        if (!(wal instanceof io.github.indreshgahoi.queue.storage.replication.ReplicatedLog replicatedLog)) {
            return;
        }

        long includedIndex = snapshot.lastIncludedIndex();
        long includedTerm = snapshot.lastIncludedTerm();
        if (includedIndex > replicatedLog.localDurableIndex()) {
            throw new IllegalStateException(
                    "Snapshot logical boundary exceeds durable WAL index"
            );
        }

        replicatedLog.entry(includedIndex).ifPresent(entry -> {
            if (entry.logTerm() != includedTerm) {
                throw new IllegalStateException(
                        "Snapshot term does not match WAL entry at index "
                                + includedIndex
                );
            }
        });
    }

    private void requirePublishCapacity(int payloadBytes) {
        if (retainedMessages >= config.maxRetainedMessages()) {
            throw new QueueCapacityExceededException("retained message count");
        }
        if (retainedPayloadBytes
                > config.maxRetainedBytes() - payloadBytes) {
            throw new QueueCapacityExceededException("retained payload bytes");
        }
    }

    private void restoreCapacityAccounting(
            Map<String, RecoveryState> states
    ) {
        for (RecoveryState state : states.values()) {
            if (state.status() == RecoveryStatus.DONE) {
                continue;
            }
            retainedMessages++;
            retainedPayloadBytes += payloadBytes(state.message());
        }
    }

    private int payloadBytes(Message message) {
        return message.payload().getBytes(StandardCharsets.UTF_8).length;
    }

    private void applyWalRecords(
            Map<String, RecoveryState> states,
            List<WalRecord> records
    ) {
        for (WalRecord record : records) {
            applyWalRecord(
                    states,
                    record
            );
        }
    }

    private void applyWalRecord(
            Map<String, RecoveryState> states,
            WalRecord record
    ) {

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
                                null,
                                record.attempt(),
                                null,
                                null
                        )
                );
            }

            case LEASE_STARTED -> {
                RecoveryState current =
                        requireExistingState(
                                states,
                                record.messageId()
                        );

                states.put(
                        record.messageId(),
                        new RecoveryState(
                                current.message(),
                                RecoveryStatus.IN_FLIGHT,
                                record.receiptHandle(),
                                record.attempt(),
                                record.timestamp(),
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
                                null,
                                current.attempt(),
                                null,
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
                                null,
                                record.attempt(),
                                null,
                                record.timestamp()
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
                                null,
                                record.attempt(),
                                null,
                                null
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
                                null,
                                record.attempt(),
                                null,
                                null
                        )
                );
            }
        }
    }

    private void restoreSnapshotIntoRecoveryState(
            QueueSnapshot snapshot,
            Map<String, RecoveryState> states
    ) {

        for (ReadySnapshotEntry entry :
                snapshot.ready()) {

            Message message =
                    new Message(
                            entry.messageId(),
                            entry.payload()
                    );

            putSnapshotState(
                    states,
                    entry.messageId(),
                    new RecoveryState(
                            message,
                            RecoveryStatus.READY,
                            null,
                            entry.nextAttempt(),
                            null,
                            null
                    )
            );
        }

        for (InFlightSnapshotEntry entry :
                snapshot.inFlight()) {

            Message message =
                    new Message(
                            entry.messageId(),
                            entry.payload()
                    );

            putSnapshotState(
                    states,
                    entry.messageId(),
                    new RecoveryState(
                            message,
                            RecoveryStatus.IN_FLIGHT,
                            entry.receiptHandle(),
                            entry.attempt(),
                            entry.leaseUntil(),
                            null
                    )
            );
        }

        for (DelayedSnapshotEntry entry :
                snapshot.delayed()) {

            Message message =
                    new Message(
                            entry.messageId(),
                            entry.payload()
                    );

            putSnapshotState(
                    states,
                    entry.messageId(),
                    new RecoveryState(
                            message,
                            RecoveryStatus.DELAYED,
                            null,
                            entry.nextAttempt(),
                            null,
                            entry.retryAt()
                    )
            );
        }

        for (DeadLetterSnapshotEntry entry :
                snapshot.deadLetters()) {

            Message message =
                    new Message(
                            entry.messageId(),
                            entry.payload()
                    );

            putSnapshotState(
                    states,
                    entry.messageId(),
                    new RecoveryState(
                            message,
                            RecoveryStatus.DEAD_LETTER,
                            null,
                            0,
                            null,
                            null
                    )
            );
        }
    }

    private void validateSnapshotLineage(
            QueueSnapshot snapshot
    ) {
        if (!wal.storageLineage()
                .equals(snapshot.storageLineage())) {
            throw new SnapshotException(
                    "Snapshot lineage does not match WAL lineage"
            );
        }
    }

    private void putSnapshotState(
            Map<String, RecoveryState> states,
            String messageId,
            RecoveryState state
    ) {
        RecoveryState existing =
                states.putIfAbsent(
                        messageId,
                        state
                );

        if (existing != null) {
            throw new SnapshotException(
                    "Snapshot contains duplicate logical message state: "
                            + messageId
            );
        }
    }

    private Optional<QueueSnapshot> loadSnapshotSafely() {
        try {
            return snapshotStore
                    .orElseThrow()
                    .loadLatest();

        } catch (SnapshotException e) {
            requireCompleteWalHistoryForFallback(e);
            return Optional.empty();
        }
    }

    private void requireCompleteWalHistoryForFallback() {
        requireCompleteWalHistoryForFallback(null);
    }

    private void requireCompleteWalHistoryForFallback(
            SnapshotException snapshotFailure
    ) {
        if (wal.hasCompleteHistory()) {
            return;
        }

        String message =
                "Authoritative snapshot is required because the WAL prefix has been reclaimed";

        if (snapshotFailure == null) {
            throw new SnapshotException(message);
        }

        throw new SnapshotException(
                message,
                snapshotFailure
        );
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

                case IN_FLIGHT -> {
                    Instant now =
                            clock.instant();

                    if (state.leaseUntil()
                            .isAfter(now)) {

                        inFlightByReceiptHandle.put(
                                state.receiptHandle(),
                                new InFlightMessage(
                                        state.message(),
                                        state.receiptHandle(),
                                        state.leaseUntil(),
                                        state.attempt()
                                )
                        );

                    } else {

                        /*
                         * Lease expired while queue was offline.
                         *
                         * Recovery derives this state from the
                         * durable leaseUntil.
                         *
                         * Recovery does NOT create new WAL history.
                         */
                        if (state.attempt()
                                >= config.maxDeliveryAttempts()) {

                            deadLetters.addLast(
                                    state.message()
                            );

                        } else {

                            ready.addLast(
                                    new QueueMessage(
                                            state.message(),
                                            state.attempt() + 1
                                    )
                            );
                        }
                    }
                }

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
                    // Terminal message. Nothing to materialize.
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
