package io.github.indreshgahoi.queue.storage.wal;


import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.ArrayList;
import java.util.List;

public class InMemoryWriteAheadLog implements WriteAheadLog {
    List<WalRecord> walRecords = new ArrayList<>();
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
    public void close() {

    }
}
