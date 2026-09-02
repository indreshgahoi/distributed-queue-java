package io.github.indreshgahoi.queue;

import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalException;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalMessageQueueAdmissionTest {
    @TempDir
    private Path tempDir;

    @Test
    void rejectsPayloadLargerThanUtf8ByteLimitBeforeWalAppend() {
        InMemoryWriteAheadLog wal = new InMemoryWriteAheadLog();
        LocalMessageQueue queue = queue(wal, 4, 10, 100);

        assertThrows(
                MessageTooLargeException.class,
                () -> queue.publish("€€")
        );
        assertEquals(0, wal.readAll().size());
    }

    @Test
    void rejectsPublishAtRetainedMessageLimitBeforeWalAppend() {
        InMemoryWriteAheadLog wal = new InMemoryWriteAheadLog();
        LocalMessageQueue queue = queue(wal, 100, 1, 100);
        queue.publish("first");

        assertThrows(
                QueueCapacityExceededException.class,
                () -> queue.publish("second")
        );
        assertEquals(1, wal.readAll().size());
    }

    @Test
    void rejectsPublishThatWouldExceedRetainedByteLimit() {
        LocalMessageQueue queue = queue(
                new InMemoryWriteAheadLog(),
                100,
                10,
                5
        );
        queue.publish("1234");

        assertThrows(
                QueueCapacityExceededException.class,
                () -> queue.publish("56")
        );
    }

    @Test
    void durableAckReleasesMessageAndByteCapacity() {
        LocalMessageQueue queue = queue(
                new InMemoryWriteAheadLog(),
                100,
                1,
                5
        );
        queue.publish("12345");
        Delivery delivery = queue.receive().orElseThrow();
        queue.ack(delivery.receiptHandle());

        assertDoesNotThrow(() -> queue.publish("abcde"));
    }

    @Test
    void recoveryRestoresCapacityAccounting() {
        InMemoryWriteAheadLog wal = new InMemoryWriteAheadLog();
        QueueConfiguration permissive = configuration(100, 10, 100);
        new LocalMessageQueue(Clock.systemUTC(), permissive, wal)
                .publish("retained");

        LocalMessageQueue recovered = new LocalMessageQueue(
                Clock.systemUTC(),
                configuration(100, 1, 100),
                wal
        );

        assertThrows(
                QueueCapacityExceededException.class,
                () -> recovered.publish("another")
        );
    }

    @Test
    void snapshotRecoveryRestoresCapacityAccounting() {
        InMemoryWriteAheadLog wal = new InMemoryWriteAheadLog();
        FileQueueSnapshotStore snapshots = new FileQueueSnapshotStore(
                tempDir.resolve("snapshot.bin")
        );
        LocalMessageQueue original = new LocalMessageQueue(
                Clock.systemUTC(),
                configuration(100, 10, 100),
                wal,
                snapshots
        );
        original.publish("retained");
        snapshots.save(original.captureSnapshot());
        original.close();

        LocalMessageQueue recovered = new LocalMessageQueue(
                Clock.systemUTC(),
                configuration(100, 1, 100),
                wal,
                snapshots
        );

        assertThrows(
                QueueCapacityExceededException.class,
                () -> recovered.publish("another")
        );
    }

    @Test
    void failedAckDoesNotReleaseCapacity() {
        InMemoryWriteAheadLog wal = new InMemoryWriteAheadLog() {
            @Override
            public void append(WalRecord record) {
                if (record.type() == WalRecordType.ACK) {
                    throw new WalException("simulated ACK failure");
                }
                super.append(record);
            }
        };
        LocalMessageQueue queue = queue(wal, 100, 1, 100);
        queue.publish("retained");
        Delivery delivery = queue.receive().orElseThrow();

        assertThrows(
                WalException.class,
                () -> queue.ack(delivery.receiptHandle())
        );
        assertThrows(
                QueueCapacityExceededException.class,
                () -> queue.publish("another")
        );
    }

    @Test
    void concurrentPublishersCannotOversubscribeCountLimit()
            throws InterruptedException {
        LocalMessageQueue queue = queue(
                new InMemoryWriteAheadLog(),
                100,
                1,
                100
        );
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();

        try (var executor = Executors.newFixedThreadPool(8)) {
            for (int attempt = 0; attempt < 8; attempt++) {
                executor.submit(() -> {
                    start.await();
                    try {
                        queue.publish("message");
                        accepted.incrementAndGet();
                    } catch (QueueCapacityExceededException expected) {
                        // The single retained slot was consumed first.
                    }
                    return null;
                });
            }
            start.countDown();
        }

        assertEquals(1, accepted.get());
    }

    private LocalMessageQueue queue(
            InMemoryWriteAheadLog wal,
            int maxMessageBytes,
            int maxRetainedMessages,
            long maxRetainedBytes
    ) {
        return new LocalMessageQueue(
                Clock.systemUTC(),
                configuration(
                        maxMessageBytes,
                        maxRetainedMessages,
                        maxRetainedBytes
                ),
                wal
        );
    }

    private QueueConfiguration configuration(
            int maxMessageBytes,
            int maxRetainedMessages,
            long maxRetainedBytes
    ) {
        return new QueueConfiguration(
                Duration.ofSeconds(30),
                3,
                maxMessageBytes,
                maxRetainedMessages,
                maxRetainedBytes
        );
    }
}
