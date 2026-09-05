package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.node.application.port.out.FollowerReplicaLogProvider;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.FollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.replication.OrderedFollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.replication.LogPoint;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Owns follower storage handles independently of READY primary runtimes. The
 * segmented WAL lock prevents the same local lineage being opened twice.
 */
@Component
final class LocalFollowerReplicaLogProvider
        implements FollowerReplicaLogProvider, AutoCloseable {

    private final Path storageRoot;
    private final long walSegmentBytes;
    private final ConcurrentMap<StorageLineage, FollowerReplicaLog> logs =
            new ConcurrentHashMap<>();

    LocalFollowerReplicaLogProvider(
            QueueNodeProperties properties
    ) {
        storageRoot = properties.storageRoot();
        walSegmentBytes = properties.walSegmentBytes();
    }

    @Override
    public FollowerReplicaLog open(StorageLineage lineage) {
        return logs.computeIfAbsent(lineage, this::openNew);
    }

    @Override
    @PreDestroy
    public void close() {
        logs.values().forEach(FollowerReplicaLog::close);
        logs.clear();
    }

    private FollowerReplicaLog openNew(StorageLineage lineage) {
        LocalPartitionStorageLayout storage =
                LocalPartitionStorageLayout.resolve(storageRoot, lineage);
        SegmentedFileWriteAheadLog wal =
                new SegmentedFileWriteAheadLog(
                        storage.walDirectory(),
                        walSegmentBytes,
                        lineage
                );
        try {
            new FileQueueSnapshotStore(
                    storage.snapshotFile()
            ).loadLatest().ifPresent(snapshot -> {
                if (!lineage.equals(snapshot.storageLineage())) {
                    throw new IllegalStateException(
                            "Snapshot lineage does not match follower WAL"
                    );
                }
                    wal.restoreSnapshotBoundary(
                            new LogPoint(
                                    snapshot.lastIncludedIndex(),
                                    snapshot.lastIncludedTerm()
                            )
                    );
            });
            return new OrderedFollowerReplicaLog(
                    wal,
                    storage.replicaHardStateFile()
            );
        } catch (RuntimeException failure) {
            wal.close();
            throw failure;
        }
    }
}
