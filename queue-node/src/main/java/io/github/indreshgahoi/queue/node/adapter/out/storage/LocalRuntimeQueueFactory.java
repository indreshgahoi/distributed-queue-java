package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.LocalMessageQueue;
import io.github.indreshgahoi.queue.QueueConfiguration;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

@Component
final class LocalRuntimeQueueFactory implements RuntimeQueueFactory {
    private final Path storageRoot;
    private final long walSegmentBytes;
    private final QueueConfiguration queueConfiguration;

    LocalRuntimeQueueFactory(QueueNodeProperties properties) {
        storageRoot = properties.storageRoot();
        walSegmentBytes = properties.walSegmentBytes();
        queueConfiguration = new QueueConfiguration(
                Duration.ofSeconds(30),
                3,
                properties.maxMessageBytes(),
                properties.maxRetainedMessages(),
                properties.maxRetainedBytes()
        );
    }

    @Override
    public RuntimeQueue open(PartitionPlacement placement) {
        Path partitionRoot = storageRoot
                .resolve(placement.queueId().toString())
                .resolve(placement.generationId().toString())
                .resolve("partition-" + placement.partitionId());
        SegmentedFileWriteAheadLog wal = new SegmentedFileWriteAheadLog(
                partitionRoot.resolve("wal"),
                walSegmentBytes,
                placement.lineage()
        );
        try {
            FileQueueSnapshotStore snapshots = new FileQueueSnapshotStore(
                    partitionRoot.resolve("snapshot.bin")
            );
            LocalMessageQueue queue = new LocalMessageQueue(
                    Clock.systemUTC(),
                    queueConfiguration,
                    wal,
                    snapshots
            );
            return new LocalRuntimeQueue(queue);
        } catch (RuntimeException failure) {
            // LocalMessageQueue cannot own the WAL if construction/recovery
            // fails, so the factory must close it to avoid a leaked file lock.
            wal.close();
            throw failure;
        }
    }

    private record LocalRuntimeQueue(LocalMessageQueue delegate)
            implements RuntimeQueue {
        @Override
        public String publish(String payload) {
            return delegate.publish(payload);
        }

        @Override
        public Optional<MessageDelivery> receive() {
            maintainTimeBasedTransitions();
            return delegate.receive().map(delivery -> new MessageDelivery(
                    delivery.message().id(),
                    delivery.message().payload(),
                    delivery.receiptHandle(),
                    delivery.attempt()
            ));
        }

        @Override
        public boolean ack(String receiptHandle) {
            maintainTimeBasedTransitions();
            return delegate.ack(receiptHandle);
        }

        @Override
        public boolean nack(String receiptHandle, Duration retryDelay) {
            maintainTimeBasedTransitions();
            return delegate.nack(receiptHandle, retryDelay);
        }

        @Override
        public void close() {
            delegate.close();
        }

        private void maintainTimeBasedTransitions() {
            // The core engine deliberately exposes clock-driven transitions
            // explicitly. Run them at the request boundary so an expired
            // receipt cannot ACK/NACK merely because no background sweep ran,
            // and an eligible delayed message is visible to receive.
            delegate.requeueExpiredMessages();
            delegate.makeDelayedMessagesReady();
        }
    }
}
