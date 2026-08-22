package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageQueuePersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void publishedMessageSurvivesRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        String messageId;

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            messageId = queue.publish("A");
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            Delivery delivery = recovered.receive().orElseThrow();

            assertEquals(messageId, delivery.message().id());
            assertEquals("A", delivery.message().payload());
            assertEquals(1, delivery.attempt());
        }
    }

    @Test
    void multiplePublishedMessagesRecoverInOriginalOrder() {
        Path walPath = tempDir.resolve("queue.wal");

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            queue.publish("A");
            queue.publish("B");
            queue.publish("C");
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            assertEquals(
                    "A",
                    recovered.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertEquals(
                    "B",
                    recovered.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertEquals(
                    "C",
                    recovered.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertTrue(recovered.receive().isEmpty());
        }
    }

    @Test
    void emptyWalRecoversEmptyQueue() {
        Path walPath = tempDir.resolve("queue.wal");

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            assertTrue(queue.receive().isEmpty());
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            assertTrue(recovered.receive().isEmpty());
        }
    }

    @Test
    void recoveryPreservesMessageIdentity() {
        Path walPath = tempDir.resolve("queue.wal");

        String originalMessageId;

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            originalMessageId = queue.publish("payment-created");
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            Delivery delivery = recovered.receive().orElseThrow();

            assertEquals(
                    originalMessageId,
                    delivery.message().id()
            );
        }
    }

    @Test
    void recoveryDoesNotWriteDuplicatePublishRecords() {
        Path walPath = tempDir.resolve("queue.wal");

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            queue.publish("A");
        }

        /*
         * First recovery.
         */
        try (InMemoryMessageQueue ignored = createQueue(walPath)) {
            // Constructor performs recovery.
        }

        /*
         * Second recovery must still contain exactly one logical message.
         *
         * If recovery accidentally calls publish(), the WAL would gain
         * another PUBLISH record every time the queue starts.
         */
        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            assertEquals(
                    "A",
                    recovered.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertTrue(recovered.receive().isEmpty());
        }
    }

    @Test
    void publishAppendsToExistingWalAfterRecovery() {
        Path walPath = tempDir.resolve("queue.wal");

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            queue.publish("A");
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            recovered.publish("B");
        }

        try (InMemoryMessageQueue recoveredAgain = createQueue(walPath)) {
            assertEquals(
                    "A",
                    recoveredAgain.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertEquals(
                    "B",
                    recoveredAgain.receive()
                            .orElseThrow()
                            .message()
                            .payload()
            );

            assertTrue(recoveredAgain.receive().isEmpty());
        }
    }

    @Test
    void closingAndReopeningWalDoesNotLosePublishedMessages() {
        Path walPath = tempDir.resolve("queue.wal");

        String firstId;
        String secondId;

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            firstId = queue.publish("A");
            secondId = queue.publish("B");
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            Delivery first = recovered.receive().orElseThrow();
            Delivery second = recovered.receive().orElseThrow();

            assertEquals(firstId, first.message().id());
            assertEquals(secondId, second.message().id());
        }
    }

    @Test
    void publishFailureDoesNotMutateReadyState() {
        WriteAheadLog failingWal = new WriteAheadLog() {

            @Override
            public void append(WalRecord record) {
                throw new WalException("simulated WAL failure");
            }

            @Override
            public java.util.List<WalRecord> readAll() {
                return java.util.List.of();
            }

            @Override
            public void close() {
            }
        };

        try (
                InMemoryMessageQueue queue =
                        new InMemoryMessageQueue(
                                Clock.systemUTC(),
                                new QueueConfiguration(),
                                failingWal
                        )
        ) {
            assertThrows(
                    WalException.class,
                    () -> queue.publish("A")
            );

            /*
             * WAL failed before the volatile state mutation.
             *
             * Therefore A must never appear in READY.
             */
            assertTrue(queue.receive().isEmpty());
        }
    }

    @Test
    void acknowledgedMessageDoesNotReappearAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        try (InMemoryMessageQueue queue = createQueue(walPath)) {
            queue.publish("A");

            Delivery delivery = queue.receive().orElseThrow();

            assertTrue(
                    queue.ack(delivery.receiptHandle())
            );
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath)) {
            assertTrue(recovered.receive().isEmpty());
        }
    }

    @Test
    void ackWalFailureDoesNotRemoveInFlightDelivery() {
        FailOnAckWal wal = new FailOnAckWal();

        try (
                InMemoryMessageQueue queue =
                        new InMemoryMessageQueue(
                                Clock.systemUTC(),
                                new QueueConfiguration(),
                                wal
                        )
        ) {
            queue.publish("A");

            Delivery delivery =
                    queue.receive().orElseThrow();

            assertThrows(
                    WalException.class,
                    () -> queue.ack(
                            delivery.receiptHandle()
                    )
            );

            /*
             * Now allow WAL writes again.
             *
             * If the failed ACK incorrectly removed the
             * IN_FLIGHT entry, this second ack() would
             * return false.
             */
            wal.allowAck();

            assertTrue(
                    queue.ack(
                            delivery.receiptHandle()
                    )
            );
        }
    }
    @Test
    void nackedMessageDoesNotBecomeReadyBeforeRetryTimeAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(Instant.parse("2026-08-21T10:00:00Z"));

        try (InMemoryMessageQueue queue = createQueue(walPath, clock)) {
            queue.publish("A");

            Delivery delivery = queue.receive().orElseThrow();

            assertTrue(
                    queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(30)
                    )
            );
        }

        try (InMemoryMessageQueue recovered = createQueue(walPath, clock)) {
            assertTrue(recovered.receive().isEmpty());

            assertEquals(
                    0,
                    recovered.makeDelayedMessagesReady()
            );
        }
    }
    @Test
    void nackedMessageBecomesReadyAtRetryTimeAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(Instant.parse("2026-08-21T10:00:00Z"));

        String messageId;

        try (InMemoryMessageQueue queue = createQueue(walPath, clock)) {
            messageId = queue.publish("A");

            Delivery delivery = queue.receive().orElseThrow();

            queue.nack(
                    delivery.receiptHandle(),
                    Duration.ofSeconds(30)
            );
        }

        clock.advance(Duration.ofSeconds(30));

        try (InMemoryMessageQueue recovered = createQueue(walPath, clock)) {
            assertEquals(
                    1,
                    recovered.makeDelayedMessagesReady()
            );

            Delivery redelivery =
                    recovered.receive().orElseThrow();

            assertEquals(
                    messageId,
                    redelivery.message().id()
            );

            assertEquals(2, redelivery.attempt());
        }
    }

    @Test
    void nackWalFailureDoesNotRemoveInFlightDelivery() {
        FailOnNackWal wal = new FailOnNackWal();

        try (
                InMemoryMessageQueue queue =
                        new InMemoryMessageQueue(
                                Clock.systemUTC(),
                                new QueueConfiguration(),
                                wal
                        )
        ) {
            queue.publish("A");

            Delivery delivery =
                    queue.receive().orElseThrow();

            assertThrows(
                    WalException.class,
                    () -> queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(10)
                    )
            );

            wal.allowNack();

            // Old ownership must still exist because failed
            // NACK must not mutate memory.
            assertTrue(
                    queue.ack(delivery.receiptHandle())
            );
        }
    }

    @Test
    void multipleNacksDoNotCreateDuplicateDelayedMessagesAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        String messageId;

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)) {

            messageId = queue.publish("A");

            // Attempt 1
            Delivery first =
                    queue.receive().orElseThrow();

            assertEquals(1, first.attempt());

            assertTrue(
                    queue.nack(
                            first.receiptHandle(),
                            Duration.ofSeconds(10)
                    )
            );

            // Make attempt 2 eligible.
            clock.advance(Duration.ofSeconds(10));

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            // Attempt 2
            Delivery second =
                    queue.receive().orElseThrow();

            assertEquals(2, second.attempt());

            assertTrue(
                    queue.nack(
                            second.receiptHandle(),
                            Duration.ofSeconds(20)
                    )
            );
        }

        /*
         * Restart while M1 should have exactly one logical
         * delayed state:
         *
         * M1
         * nextAttempt = 3
         * retryAt = currentTime + 20s
         */
        try (InMemoryMessageQueue recovered =
                     createQueue(walPath, clock)) {

            // It must not be READY yet.
            assertTrue(recovered.receive().isEmpty());

            // Old attempt-2 delayed state must not also exist.
            assertEquals(
                    0,
                    recovered.makeDelayedMessagesReady()
            );

            clock.advance(Duration.ofSeconds(20));

            /*
             * Exactly ONE delayed representation of M1
             * should become READY.
             */
            assertEquals(
                    1,
                    recovered.makeDelayedMessagesReady()
            );

            Delivery redelivery =
                    recovered.receive().orElseThrow();

            assertEquals(
                    messageId,
                    redelivery.message().id()
            );
            assertEquals(
                    "A",
                    redelivery.message().payload()
            );

            assertEquals(
                    3,
                    redelivery.attempt()
            );

            /*
             * If recovery accumulated both NACK records as
             * separate delayed entries, another copy of M1
             * would now be available.
             */
            assertTrue(recovered.receive().isEmpty());

            /*
             * Running promotion again must also not reveal
             * another stale delayed copy.
             */
            assertEquals(
                    0,
                    recovered.makeDelayedMessagesReady()
            );
        }
    }

    @Test
    void acknowledgedMessageAfterNackDoesNotReappearAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        String messageId;

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)){
              messageId = queue.publish("A");

            // Attempt 1
            Delivery first =
                    queue.receive().orElseThrow();

            assertEquals(1, first.attempt());

            assertTrue(
                    queue.nack(
                            first.receiptHandle(),
                            Duration.ofSeconds(10)
                    )
            );

            // Make attempt 2 eligible.
            clock.advance(Duration.ofSeconds(10));

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            // Attempt 2
            Delivery second =
                    queue.receive().orElseThrow();

            assertEquals(2, second.attempt());

            assertTrue(
                    queue.nack(
                            second.receiptHandle(),
                            Duration.ofSeconds(20)
                    )
            );

            clock.advance(Duration.ofSeconds(20));

            assertEquals(1,
                    queue.makeDelayedMessagesReady());

        }

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)){
            assertTrue(queue.receive().isEmpty());
        }

    }

    private InMemoryMessageQueue createQueue(Path walPath) {
        return new InMemoryMessageQueue(
                Clock.systemUTC(),
                new QueueConfiguration(),
                new FileWriteAheadLog(walPath)
        );
    }
    private InMemoryMessageQueue createQueue(Path walPath, Clock clock) {
        return new InMemoryMessageQueue(
                clock,
                new QueueConfiguration(),
                new FileWriteAheadLog(walPath)
        );
    }
    private static final class FailOnAckWal
            implements WriteAheadLog {

        private final List<WalRecord> records =
                new ArrayList<>();

        private boolean failAck = true;

        @Override
        public void append(WalRecord record) {

            if (record.type() == WalRecordType.ACK && failAck) {
                throw new WalException(
                        "Simulated ACK WAL failure"
                );
            }

            records.add(record);
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        void allowAck() {
            failAck = false;
        }

        @Override
        public void close() {
        }
    }
    private static final class FailOnNackWal
            implements WriteAheadLog {

        private final List<WalRecord> records =
                new ArrayList<>();

        private boolean failNack = true;

        @Override
        public void append(WalRecord record) {
            if (record.type() == WalRecordType.NACK && failNack) {
                throw new WalException(
                        "Simulated NACK WAL failure"
                );
            }

            records.add(record);
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        void allowNack() {
            failNack = false;
        }

        @Override
        public void close() {
        }
    }


}