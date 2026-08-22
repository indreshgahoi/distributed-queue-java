package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.wal.FileWriteAheadLog;
import io.github.indreshgahoi.queue.wal.WalException;
import io.github.indreshgahoi.queue.wal.WalRecord;
import io.github.indreshgahoi.queue.wal.WalRecordType;
import io.github.indreshgahoi.queue.wal.WriteAheadLog;
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

            clock.advance(Duration.ofSeconds(20));

            assertEquals(1,
                    queue.makeDelayedMessagesReady());

        }

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)) {
            assertTrue(queue.receive().isEmpty());
        }

    }

    @Test
    void expiredDeliveryPreservesNextAttemptAfterRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );
        String messageId;
        try (InMemoryMessageQueue queue = createQueue(walPath, clock)) {
            messageId = queue.publish("A");

            Delivery first = queue.receive().orElseThrow();
            assertEquals(1, first.attempt());
            clock.advance(
                    new QueueConfiguration()
                            .visibilityTimeout()
            );

            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );

        }
        try (InMemoryMessageQueue recovered = createQueue(walPath, clock)) {
            Delivery redelivery = recovered.receive().orElseThrow();
            assertEquals(
                    messageId,
                    redelivery.message().id()
            );
            assertEquals(
                    2,
                    redelivery.attempt()
            );
        }

    }

    @Test
    void leaseExpiryWalFailureDoesNotRemoveInFlightDelivery() {

        FailOnLeaseExpiryWal wal =
                new FailOnLeaseExpiryWal();

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        try (InMemoryMessageQueue queue =
                     new InMemoryMessageQueue(
                             clock,
                             new QueueConfiguration(),
                             wal
                     )) {

            queue.publish("A");

            Delivery delivery =
                    queue.receive().orElseThrow();

            clock.advance(
                    new QueueConfiguration()
                            .visibilityTimeout()
            );

            assertThrows(
                    WalException.class,
                    queue::requeueExpiredMessages
            );

            /*
             * If the failed lease-expiry transition did not
             * mutate memory, R1 should still own the delivery.
             */

            wal.allowLeaseExpiry();

            assertTrue(
                    queue.ack(
                            delivery.receiptHandle()
                    )
            );
        }
    }

    @Test
    void latestLeaseExpiredDecisionWinsDuringRecovery() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        String messageId;

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)) {

            messageId = queue.publish("A");

            // Attempt 1
            Delivery first =
                    queue.receive().orElseThrow();

            assertEquals(1, first.attempt());

            clock.advance(config.visibilityTimeout());

            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );

            // Attempt 2
            Delivery second =
                    queue.receive().orElseThrow();

            assertEquals(2, second.attempt());

            clock.advance(config.visibilityTimeout());

            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );
        }

        /*
         * WAL:
         *
         * PUBLISH       M1 attempt=1
         * LEASE_EXPIRED M1 attempt=2
         * LEASE_EXPIRED M1 attempt=3
         *
         * Final decision must be:
         *
         * READY attempt=3
         */
        try (InMemoryMessageQueue recovered =
                     createQueue(walPath, clock)) {

            Delivery third =
                    recovered.receive().orElseThrow();

            assertEquals(
                    messageId,
                    third.message().id()
            );

            assertEquals(
                    3,
                    third.attempt()
            );

            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }
    @Test
    void finalAttemptNackDeadLetterDecisionSurvivesRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)) {

            queue.publish("poison");

            Delivery first =
                    queue.receive().orElseThrow();

            assertTrue(
                    queue.nack(
                            first.receiptHandle(),
                            Duration.ZERO
                    )
            );

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            Delivery second =
                    queue.receive().orElseThrow();

            assertTrue(
                    queue.nack(
                            second.receiptHandle(),
                            Duration.ZERO
                    )
            );

            assertEquals(
                    1,
                    queue.makeDelayedMessagesReady()
            );

            Delivery third =
                    queue.receive().orElseThrow();

            assertEquals(
                    config.maxDeliveryAttempts(),
                    third.attempt()
            );

            /*
             * Your current code writes DEAD_LETTER directly here.
             */
            assertTrue(
                    queue.nack(
                            third.receiptHandle(),
                            Duration.ZERO
                    )
            );

            assertEquals(
                    1,
                    queue.deadLetterCount()
            );
        }

        try (InMemoryMessageQueue recovered =
                     createQueue(walPath, clock)) {

            assertEquals(
                    1,
                    recovered.deadLetterCount()
            );

            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }

    @Test
    void leaseExpiredWalFailureKeepsCurrentDeliveryInFlight() {
        FailOnLeaseExpiredWal wal =
                new FailOnLeaseExpiredWal();

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     new InMemoryMessageQueue(
                             clock,
                             config,
                             wal
                     )) {

            queue.publish("A");

            Delivery delivery =
                    queue.receive().orElseThrow();

            clock.advance(config.visibilityTimeout());

            assertThrows(
                    WalException.class,
                    queue::requeueExpiredMessages
            );

            /*
             * The durable transition failed.
             *
             * Therefore the previous state must remain authoritative:
             * the old receipt still owns M1.
             */
            wal.allowLeaseExpired();

            assertTrue(
                    queue.ack(
                            delivery.receiptHandle()
                    )
            );
        }
    }

    @Test
    void deadLetterDecisionSurvivesRestart() {
        Path walPath = tempDir.resolve("queue.wal");

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     createQueue(walPath, clock)) {

            queue.publish("poison");

            // Attempt 1
            queue.receive().orElseThrow();

            clock.advance(config.visibilityTimeout());
            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );

            // Attempt 2
            queue.receive().orElseThrow();

            clock.advance(config.visibilityTimeout());
            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );

            // Attempt 3 - final attempt
            Delivery finalDelivery =
                    queue.receive().orElseThrow();

            assertEquals(
                    config.maxDeliveryAttempts(),
                    finalDelivery.attempt()
            );

            clock.advance(config.visibilityTimeout());

            /*
             * Final expiry writes DEAD_LETTER.
             *
             * It does not requeue the message.
             */
            assertEquals(
                    0,
                    queue.requeueExpiredMessages()
            );

            assertEquals(
                    1,
                    queue.deadLetterCount()
            );
        }

        /*
         * Recovery must consume the durable DEAD_LETTER decision,
         * not derive a retry from the previous state.
         */
        try (InMemoryMessageQueue recovered =
                     createQueue(walPath, clock)) {

            assertEquals(
                    1,
                    recovered.deadLetterCount()
            );

            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }

    @Test
    void failureDuringMultipleLeaseExpiriesPreservesPerMessageAtomicity() {
        FailOnSecondLeaseExpiredWal wal =
                new FailOnSecondLeaseExpiredWal();

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     new InMemoryMessageQueue(
                             clock,
                             config,
                             wal
                     )) {

            queue.publish("M1");
            queue.publish("M2");

            Delivery first =
                    queue.receive().orElseThrow();

            Delivery second =
                    queue.receive().orElseThrow();

            assertEquals(1, first.attempt());
            assertEquals(1, second.attempt());

            /*
             * Both deliveries are now expired.
             */
            clock.advance(config.visibilityTimeout());

            /*
             * First LEASE_EXPIRED WAL append succeeds.
             * Second LEASE_EXPIRED WAL append fails.
             */
            assertThrows(
                    WalException.class,
                    queue::requeueExpiredMessages
            );

            /*
             * Exactly one transition should already have
             * committed successfully and therefore become READY.
             *
             * We deliberately do NOT assert whether this is M1
             * or M2 because inFlightByReceiptHandle is a HashMap,
             * so iteration order is unspecified.
             */
            Delivery requeued =
                    queue.receive().orElseThrow();

            assertEquals(2, requeued.attempt());

            assertTrue(queue.receive().isEmpty());

            /*
             * Allow WAL writes again.
             */
            wal.allowLeaseExpired();

            /*
             * Of the two original receipt handles:
             *
             * - one belongs to the message already requeued,
             *   so that receipt must now be invalid;
             *
             * - the other belongs to the transition whose WAL
             *   append failed, so that delivery must still be
             *   IN_FLIGHT and its receipt must remain valid.
             */
            boolean firstAck =
                    queue.ack(first.receiptHandle());

            boolean secondAck =
                    queue.ack(second.receiptHandle());

            assertNotEquals(
                    firstAck,
                    secondAck,
                    "Exactly one original delivery must remain IN_FLIGHT"
            );
        }
    }

    @Test
    void retryAfterPartialLeaseExpiryFailureProcessesOnlyRemainingInFlightMessage() {
        FailOnSecondLeaseExpiredWal wal =
                new FailOnSecondLeaseExpiredWal();

        MutableClock clock =
                new MutableClock(
                        Instant.parse("2026-08-22T00:00:00Z")
                );

        QueueConfiguration config =
                new QueueConfiguration();

        try (InMemoryMessageQueue queue =
                     new InMemoryMessageQueue(
                             clock,
                             config,
                             wal
                     )) {

            queue.publish("M1");
            queue.publish("M2");

            queue.receive().orElseThrow();
            queue.receive().orElseThrow();

            clock.advance(config.visibilityTimeout());

            assertThrows(
                    WalException.class,
                    queue::requeueExpiredMessages
            );

            /*
             * One message was successfully transitioned
             * before the failure.
             */
            Delivery firstRequeued =
                    queue.receive().orElseThrow();

            assertEquals(2, firstRequeued.attempt());

            assertTrue(queue.receive().isEmpty());

            wal.allowLeaseExpired();

            /*
             * Retry the expiry scan.
             *
             * Only the delivery whose previous WAL append failed
             * should remain IN_FLIGHT.
             */
            assertEquals(
                    1,
                    queue.requeueExpiredMessages()
            );

            Delivery secondRequeued =
                    queue.receive().orElseThrow();

            assertEquals(2, secondRequeued.attempt());

            /*
             * There must not be a duplicate third message.
             */
            assertTrue(queue.receive().isEmpty());

            assertNotEquals(
                    firstRequeued.message().id(),
                    secondRequeued.message().id()
            );
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

    private static final class FailOnSecondLeaseExpiredWal
            implements WriteAheadLog {

        private final List<WalRecord> records =
                new ArrayList<>();

        private int leaseExpiredAppendCount;
        private boolean failureEnabled = true;

        @Override
        public void append(WalRecord record) {

            if (record.type() == WalRecordType.LEASE_EXPIRED) {

                leaseExpiredAppendCount++;

                if (failureEnabled
                        && leaseExpiredAppendCount == 2) {

                    throw new WalException(
                            "Simulated failure on second LEASE_EXPIRED"
                    );
                }
            }

            records.add(record);
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        void allowLeaseExpired() {
            failureEnabled = false;
        }

        @Override
        public void close() {
        }
    }

    private static final class FailOnLeaseExpiredWal
            implements WriteAheadLog {

        private final List<WalRecord> records =
                new ArrayList<>();

        private boolean failLeaseExpired = true;

        @Override
        public void append(WalRecord record) {
            if (record.type() == WalRecordType.LEASE_EXPIRED
                    && failLeaseExpired) {

                throw new WalException(
                        "Simulated LEASE_EXPIRED WAL failure"
                );
            }

            records.add(record);
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        void allowLeaseExpired() {
            failLeaseExpired = false;
        }

        @Override
        public void close() {
        }
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

    private static final class FailOnLeaseExpiryWal
            implements WriteAheadLog {

        private final List<WalRecord> records =
                new ArrayList<>();

        private boolean failLease = true;

        @Override
        public void append(WalRecord record) {
            if (record.type() == WalRecordType.LEASE_EXPIRED && failLease) {
                throw new WalException(
                        "Simulated LEASE EXPIRED WAL failure"
                );
            }

            records.add(record);
        }

        @Override
        public List<WalRecord> readAll() {
            return List.copyOf(records);
        }

        void allowLeaseExpiry() {
            failLease = false;
        }

        @Override
        public void close() {
        }
    }


}