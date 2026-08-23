package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.FileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class LocalMessageQueueSnapshotRecoveryTest {

    @TempDir
    Path tempDir;

    // ---------------------------------------------------------------------
    // Snapshot baseline recovery
    // ---------------------------------------------------------------------

    @Test
    void snapshotReadyMessageIsRecovered() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        String messageId;

        /*
         * Process lifetime 1.
         */
        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            messageId =
                    queue.publish("A");

            QueueSnapshot snapshot =
                    queue.captureSnapshot();

            snapshotStore(snapshotPath)
                    .save(snapshot);
        }

        /*
         * Process lifetime 2.
         *
         * Recovery should use the snapshot.
         */
        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            Delivery delivery =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    messageId,
                    delivery.message().id()
            );

            assertEquals(
                    "A",
                    delivery.message().payload()
            );

            assertEquals(
                    1,
                    delivery.attempt()
            );
        }
    }

    @Test
    void snapshotInFlightMessageWithActiveLeaseIsRecovered() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        String receiptHandle;

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            receiptHandle =
                    delivery.receiptHandle();

            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );
        }

        /*
         * Restart before lease expiry.
         */
        clock.advance(
                config.visibilityTimeout()
                        .dividedBy(2)
        );

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            /*
             * Message must remain IN_FLIGHT.
             */
            assertTrue(
                    recovered.receive().isEmpty()
            );

            /*
             * Original receipt handle must remain valid.
             */
            assertTrue(
                    recovered.ack(receiptHandle)
            );
        }
    }

    @Test
    void expiredSnapshotLeaseBecomesReadyDuringRecovery() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        String messageId;
        String oldReceiptHandle;

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            messageId =
                    queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            oldReceiptHandle =
                    delivery.receiptHandle();

            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );
        }

        /*
         * Lease expires while queue is offline.
         */
        clock.advance(
                config.visibilityTimeout()
                        .plusSeconds(1)
        );

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            Delivery redelivery =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    messageId,
                    redelivery.message().id()
            );

            assertEquals(
                    2,
                    redelivery.attempt()
            );

            assertFalse(
                    recovered.ack(
                            oldReceiptHandle
                    )
            );
        }
    }

    @Test
    void snapshotDelayedMessageIsRecovered() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        String messageId;

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            messageId =
                    queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            assertTrue(
                    queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(30)
                    )
            );

            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );
        }

        /*
         * Restart before retryAt.
         */
        clock.advance(
                Duration.ofSeconds(10)
        );

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            assertTrue(
                    recovered.receive().isEmpty()
            );

            assertEquals(
                    0,
                    recovered.makeDelayedMessagesReady()
            );

            clock.advance(
                    Duration.ofSeconds(20)
            );

            assertEquals(
                    1,
                    recovered.makeDelayedMessagesReady()
            );

            Delivery retry =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    messageId,
                    retry.message().id()
            );

            assertEquals(
                    2,
                    retry.attempt()
            );
        }
    }

    @Test
    void snapshotDeadLetterMessageIsRecovered() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            moveMessageToDeadLetter(queue, config);

            assertEquals(
                    1,
                    queue.deadLetterCount()
            );

            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );
        }

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            assertEquals(
                    1,
                    recovered.deadLetterCount()
            );

            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }

    // ---------------------------------------------------------------------
    // WAL suffix overrides snapshot baseline
    // ---------------------------------------------------------------------

    @Test
    void walPublishAfterSnapshotIsRecovered() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        String beforeSnapshot;
        String afterSnapshot;

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            beforeSnapshot =
                    queue.publish("A");

            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );

            /*
             * This record belongs only to the WAL suffix.
             */
            afterSnapshot =
                    queue.publish("B");
        }

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            Delivery first =
                    recovered.receive()
                            .orElseThrow();

            Delivery second =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    beforeSnapshot,
                    first.message().id()
            );

            assertEquals(
                    afterSnapshot,
                    second.message().id()
            );

            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }

    @Test
    void walAckAfterSnapshotOverridesInFlightSnapshotState() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            /*
             * Snapshot says M1 is IN_FLIGHT.
             */
            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );

            /*
             * Newer WAL suffix says DONE.
             */
            assertTrue(
                    queue.ack(
                            delivery.receiptHandle()
                    )
            );
        }

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            assertTrue(
                    recovered.receive().isEmpty()
            );
        }
    }

    @Test
    void walNackAfterSnapshotOverridesInFlightSnapshotState() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            queue.publish("A");

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            /*
             * Snapshot baseline:
             *
             * M1 = IN_FLIGHT
             */
            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );

            /*
             * WAL suffix:
             *
             * M1 = DELAYED
             */
            assertTrue(
                    queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(30)
                    )
            );
        }

        clock.advance(
                Duration.ofSeconds(10)
        );

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            assertTrue(
                    recovered.receive().isEmpty()
            );

            assertEquals(
                    0,
                    recovered.makeDelayedMessagesReady()
            );

            clock.advance(
                    Duration.ofSeconds(20)
            );

            assertEquals(
                    1,
                    recovered.makeDelayedMessagesReady()
            );

            Delivery retry =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    2,
                    retry.attempt()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Fallback
    // ---------------------------------------------------------------------

    @Test
    void missingSnapshotFallsBackToFullWalRecovery() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("does-not-exist.snapshot");

        String messageId;

        /*
         * Build WAL without ever creating a snapshot.
         */
        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            messageId =
                    queue.publish("A");
        }

        try (
                LocalMessageQueue recovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            Delivery delivery =
                    recovered.receive()
                            .orElseThrow();

            assertEquals(
                    messageId,
                    delivery.message().id()
            );

            assertEquals(
                    "A",
                    delivery.message().payload()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Architectural equivalence
    // ---------------------------------------------------------------------

    @Test
    void snapshotPlusWalSuffixMatchesFullWalRecovery() {
        MutableClock clock =
                clockAt("2026-08-23T10:00:00Z");

        QueueConfiguration config =
                new QueueConfiguration();

        Path walPath =
                tempDir.resolve("queue.wal");

        Path snapshotPath =
                tempDir.resolve("queue.snapshot");

        /*
         * Build a non-trivial lifecycle:
         *
         * M1 -> READY
         * M2 -> IN_FLIGHT -> ACK
         * M3 -> IN_FLIGHT -> DELAYED
         *
         * Snapshot is taken in the middle.
         */
        String m1;
        String m3;

        try (
                LocalMessageQueue queue =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            m1 =
                    queue.publish("M1");

            queue.publish("M2");

            Delivery m1Delivery =
                    queue.receive()
                            .orElseThrow();

            /*
             * M1 is IN_FLIGHT at snapshot time.
             */
            snapshotStore(snapshotPath)
                    .save(
                            queue.captureSnapshot()
                    );

            /*
             * WAL suffix:
             *
             * ACK M1
             * then work on M2
             */
            assertTrue(
                    queue.ack(
                            m1Delivery.receiptHandle()
                    )
            );

            m3 =
                    queue.publish("M3");

            Delivery m2Delivery =
                    queue.receive()
                            .orElseThrow();

            assertTrue(
                    queue.nack(
                            m2Delivery.receiptHandle(),
                            Duration.ofSeconds(30)
                    )
            );
        }

        /*
         * Snapshot-based recovery.
         */
        try (
                LocalMessageQueue snapshotRecovered =
                        createQueue(
                                walPath,
                                snapshotPath,
                                clock,
                                config
                        )
        ) {
            /*
             * M1 was ACKed after the snapshot, so must not return.
             *
             * M3 was published after the snapshot, so must exist.
             */
            Delivery ready =
                    snapshotRecovered.receive()
                            .orElseThrow();

            assertEquals(
                    m3,
                    ready.message().id()
            );

            assertTrue(
                    snapshotRecovered.receive()
                            .isEmpty()
            );

            /*
             * M2 is delayed.
             */
            assertEquals(
                    0,
                    snapshotRecovered.makeDelayedMessagesReady()
            );

            clock.advance(
                    Duration.ofSeconds(30)
            );

            assertEquals(
                    1,
                    snapshotRecovered.makeDelayedMessagesReady()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private LocalMessageQueue createQueue(
            Path walPath,
            Path snapshotPath,
            MutableClock clock,
            QueueConfiguration config
    ) {
        WriteAheadLog wal =
                new FileWriteAheadLog(
                        walPath
                );

        QueueSnapshotStore snapshotStore =
                new FileQueueSnapshotStore(
                        snapshotPath
                );


        return new LocalMessageQueue(
                clock,
                config,
                wal,
                snapshotStore
        );
    }

    private QueueSnapshotStore snapshotStore(
            Path snapshotPath
    ) {
        return new FileQueueSnapshotStore(
                snapshotPath
        );
    }

    private MutableClock clockAt(
            String instant
    ) {
        return new MutableClock(
                Instant.parse(instant)
        );
    }

    private void moveMessageToDeadLetter(
            LocalMessageQueue queue,
            QueueConfiguration config
    ) {
        queue.publish("poison");

        for (int attempt = 1;
             attempt <= config.maxDeliveryAttempts();
             attempt++) {

            Delivery delivery =
                    queue.receive()
                            .orElseThrow();

            assertEquals(
                    attempt,
                    delivery.attempt()
            );

            assertTrue(
                    queue.nack(
                            delivery.receiptHandle(),
                            Duration.ZERO
                    )
            );

            if (attempt < config.maxDeliveryAttempts()) {
                assertEquals(
                        1,
                        queue.makeDelayedMessagesReady()
                );
            }
        }
    }
}