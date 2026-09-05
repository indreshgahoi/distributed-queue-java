package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.util.List;
import java.util.Optional;

public interface ReplicatedLog extends AutoCloseable {
    AppendBatchResult appendLocal(long term, List<WalRecord> records);

    AppendBatchResult appendReplicated(LogPoint previous, List<LogEntry> entries);

    List<LogEntry> readFrom(long firstIndex, int maximumEntries);

    Optional<LogEntry> entry(long index);

    LogPoint lastLogPoint();

    long localDurableIndex();

    void restoreSnapshotBoundary(LogPoint boundary);

    LogPoint snapshotBoundary();

    WalPosition currentDurablePosition();

    StorageLineage lineage();

    @Override
    void close();
}
