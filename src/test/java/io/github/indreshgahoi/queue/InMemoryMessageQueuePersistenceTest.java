package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;

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

    private InMemoryMessageQueue createQueue(Path walPath) {
        return new InMemoryMessageQueue(
                Clock.systemUTC(),
                new QueueConfiguration(),
                new FileWriteAheadLog(walPath)
        );
    }
}