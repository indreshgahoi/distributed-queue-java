package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.LocalMessageQueue;
import io.github.indreshgahoi.queue.QueueConfiguration;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.Clock;

@Component
final class LocalRuntimeQueueFactory implements RuntimeQueueFactory {
    private final Path storageRoot;
    private final long walSegmentBytes;

    LocalRuntimeQueueFactory(QueueNodeProperties properties) {
        storageRoot = properties.storageRoot();
        walSegmentBytes = properties.walSegmentBytes();
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
                    new QueueConfiguration(),
                    wal,
                    snapshots
            );
            return queue::close;
        } catch (RuntimeException failure) {
            // LocalMessageQueue cannot own the WAL if construction/recovery
            // fails, so the factory must close it to avoid a leaked file lock.
            wal.close();
            throw failure;
        }
    }
}
