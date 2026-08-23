package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.DeadLetterSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.DelayedSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.InFlightSnapshotEntry;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.ReadySnapshotEntry;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LocalMessageQueueSnapshotTest {

    @Test
    void captureSnapshotContainsReadyMessages() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            String firstId =
                    queue.publish("A");

            String secondId =
                    queue.publish("B");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    2,
                    snapshot.ready().size()
            );

            ReadySnapshotEntry first =
                    snapshot.ready().get(0);

            ReadySnapshotEntry second =
                    snapshot.ready().get(1);

            assertEquals(
                    firstId,
                    first.messageId()
            );

            assertEquals(
                    "A",
                    first.payload()
            );

            assertEquals(
                    1,
                    first.nextAttempt()
            );

            assertEquals(
                    secondId,
                    second.messageId()
            );

            assertEquals(
                    "B",
                    second.payload()
            );

            assertEquals(
                    1,
                    second.nextAttempt()
            );

            assertTrue(
                    snapshot.inFlight().isEmpty()
            );

            assertTrue(
                    snapshot.delayed().isEmpty()
            );

            assertTrue(
                    snapshot.deadLetters().isEmpty()
            );
        }
    }

    @Test
    void captureSnapshotContainsActiveLease() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            String messageId =
                    queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertTrue(
                    snapshot.ready().isEmpty()
            );

            assertEquals(
                    1,
                    snapshot.inFlight().size()
            );

            InFlightSnapshotEntry entry =
                    snapshot.inFlight().getFirst();

            assertEquals(
                    messageId,
                    entry.messageId()
            );

            assertEquals(
                    "A",
                    entry.payload()
            );

            assertEquals(
                    delivery.receiptHandle(),
                    entry.receiptHandle()
            );

            assertEquals(
                    delivery.attempt(),
                    entry.attempt()
            );

            assertEquals(
                    clock.instant()
                            .plus(
                                    config.visibilityTimeout()
                            ),
                    entry.leaseUntil()
            );
        }
    }

    @Test
    void captureSnapshotContainsDelayedMessage() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        Duration retryDelay =
                Duration.ofSeconds(30);

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            String messageId =
                    queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            assertTrue(
                    queue.nack(
                            delivery.receiptHandle(),
                            retryDelay
                    )
            );

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertTrue(
                    snapshot.ready().isEmpty()
            );

            assertTrue(
                    snapshot.inFlight().isEmpty()
            );

            assertEquals(
                    1,
                    snapshot.delayed().size()
            );

            DelayedSnapshotEntry entry =
                    snapshot.delayed().getFirst();

            assertEquals(
                    messageId,
                    entry.messageId()
            );

            assertEquals(
                    "A",
                    entry.payload()
            );

            assertEquals(
                    2,
                    entry.nextAttempt()
            );

            assertEquals(
                    clock.instant()
                            .plus(retryDelay),
                    entry.retryAt()
            );
        }
    }

    @Test
    void captureSnapshotContainsDeadLetterMessage() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            String messageId =
                    queue.publish("poison");

            /*
             * Attempt 1
             */
            Delivery first =
                    queue.receive()
                            .orElseThrow();

            queue.nack(
                    first.receiptHandle(),
                    Duration.ZERO
            );

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            /*
             * Attempt 2
             */
            Delivery second =
                    queue.receive()
                            .orElseThrow();

            queue.nack(
                    second.receiptHandle(),
                    Duration.ZERO
            );

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            /*
             * Final attempt.
             */
            Delivery third =
                    queue.receive()
                            .orElseThrow();

            assertEquals(
                    config.maxDeliveryAttempts(),
                    third.attempt()
            );

            queue.nack(
                    third.receiptHandle(),
                    Duration.ZERO
            );

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    1,
                    snapshot.deadLetters().size()
            );

            DeadLetterSnapshotEntry entry =
                    snapshot.deadLetters()
                            .getFirst();

            assertEquals(
                    messageId,
                    entry.messageId()
            );

            assertEquals(
                    "poison",
                    entry.payload()
            );

            assertTrue(
                    snapshot.ready().isEmpty()
            );

            assertTrue(
                    snapshot.inFlight().isEmpty()
            );

            assertTrue(
                    snapshot.delayed().isEmpty()
            );
        }
    }

    @Test
    void captureSnapshotContainsMultipleQueueStatesAtSameTime() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            /*
             * READY
             */
            String readyId =
                    queue.publish("ready");

            /*
             * IN_FLIGHT
             */
            String inFlightId =
                    queue.publish("in-flight");

            Delivery inFlightDelivery =
                    queue.receive()
                            .orElseThrow();

            /*
             * Because queue is FIFO, first receive()
             * consumed the first published message.
             *
             * To make the states explicit, publish another
             * READY message after establishing IN_FLIGHT.
             */
            String additionalReadyId =
                    queue.publish("ready-2");

            /*
             * DELAYED:
             *
             * consume the next currently READY message
             * and NACK it.
             */
            Delivery delayedDelivery =
                    queue.receive()
                            .orElseThrow();

            queue.nack(
                    delayedDelivery.receiptHandle(),
                    Duration.ofSeconds(30)
            );

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    1,
                    snapshot.inFlight().size()
            );

            assertEquals(
                    inFlightDelivery.message().id(),
                    snapshot.inFlight()
                            .getFirst()
                            .messageId()
            );

            assertEquals(
                    1,
                    snapshot.delayed().size()
            );

            assertEquals(
                    delayedDelivery.message().id(),
                    snapshot.delayed()
                            .getFirst()
                            .messageId()
            );

            assertEquals(
                    1,
                    snapshot.ready().size()
            );

            assertEquals(
                    additionalReadyId,
                    snapshot.ready()
                            .getFirst()
                            .messageId()
            );
        }
    }

    @Test
    void capturingSnapshotDoesNotMutateQueueState() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            String firstId =
                    queue.publish("A");

            String secondId =
                    queue.publish("B");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    2,
                    snapshot.ready().size()
            );

            /*
             * Snapshot capture must be observational.
             *
             * It must not dequeue or otherwise mutate
             * runtime queue state.
             */
            Delivery first =
                    queue.receive()
                            .orElseThrow();

            Delivery second =
                    queue.receive()
                            .orElseThrow();

            assertEquals(
                    firstId,
                    first.message().id()
            );

            assertEquals(
                    secondId,
                    second.message().id()
            );

            assertTrue(
                    queue.receive().isEmpty()
            );
        }
    }

    @Test
    void snapshotEntriesAreIndependentFromSubsequentQueueChanges() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             new InMemoryWriteAheadLog()
                     )) {

            queue.publish("A");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    1,
                    snapshot.ready().size()
            );

            /*
             * Mutate queue after snapshot capture.
             */
            queue.publish("B");

            /*
             * Previously captured snapshot must remain
             * an immutable point-in-time image.
             */
            assertEquals(
                    1,
                    snapshot.ready().size()
            );

            assertEquals(
                    "A",
                    snapshot.ready()
                            .getFirst()
                            .payload()
            );
        }
    }

    @Test
    void captureSnapshotUsesCurrentWalPosition() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        /*
         * This fake WAL gives us deterministic control
         * over the reported durable position.
         */
        PositionAwareWal wal =
                new PositionAwareWal(
                        new WalPosition(
                                0,
                                12_480
                        )
                );

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             wal
                     )) {

            queue.publish("A");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            assertEquals(
                    new WalPosition(
                            0,
                            12_480
                    ),
                    snapshot.walPosition()
            );
        }
    }

    @Test
    void snapshotWalPositionAndStateAreCapturedTogether() {
        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-23T10:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        PositionAwareWal wal =
                new PositionAwareWal(
                        new WalPosition(
                                0,
                                500
                        )
                );

        try (LocalMessageQueue queue =
                     new LocalMessageQueue(
                             clock,
                             config,
                             wal
                     )) {

            String messageId =
                    queue.publish("A");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            /*
             * Snapshot contains the queue state...
             */
            assertEquals(
                    messageId,
                    snapshot.ready()
                            .getFirst()
                            .messageId()
            );

            /*
             * ...and the WAL boundary associated with
             * that captured state.
             */
            assertEquals(
                    new WalPosition(
                            0,
                            500
                    ),
                    snapshot.walPosition()
            );
        }
    }

    private static final class PositionAwareWal
            implements WriteAheadLog {

        private final WalPosition position;

        PositionAwareWal(
                WalPosition position
        ) {
            this.position = position;
        }

        @Override
        public void append(WalRecord record) {
            /*
             * No-op for snapshot-capture unit tests.
             */
        }

        @Override
        public java.util.List<WalRecord> readAll() {
            return java.util.List.of();
        }

        @Override
        public WalPosition currentDurablePosition() {
            return position;
        }

        @Override
        public List<WalRecord> readFrom(WalPosition position) {
            return List.of();
        }

        @Override
        public void close() {
        }
    }
}