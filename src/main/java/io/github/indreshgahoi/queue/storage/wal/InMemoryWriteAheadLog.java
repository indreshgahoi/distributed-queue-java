package io.github.indreshgahoi.queue.storage.wal;


import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.util.ArrayList;
import java.util.List;

public class InMemoryWriteAheadLog implements WriteAheadLog {
    List<WalRecord> walRecords = new ArrayList<>();
    private final StorageLineage storageLineage;

    public InMemoryWriteAheadLog() {
        this(StorageLineage.create());
    }

    public InMemoryWriteAheadLog(StorageLineage storageLineage) {
        this.storageLineage = storageLineage;
    }
    @Override
    public void append(WalRecord record) {
        walRecords.add(record);
    }

    @Override
    public List<WalRecord> readAll() {
        return List.copyOf(walRecords);
    }

    @Override
    public WalPosition currentDurablePosition() {
        return new WalPosition(0, readAll().size());
    }

    @Override
    public List<WalRecord> readFrom(WalPosition position) {
        return List.of();
    }

    @Override
    public StorageLineage storageLineage() {
        return storageLineage;
    }

    @Override
    public void close() {

    }
}
