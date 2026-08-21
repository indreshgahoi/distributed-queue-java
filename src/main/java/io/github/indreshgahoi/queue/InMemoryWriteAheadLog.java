package io.github.indreshgahoi.queue;

import java.util.List;

public class InMemoryWriteAheadLog implements WriteAheadLog {
    @Override
    public void append(WalRecord record) {

    }

    @Override
    public List<WalRecord> readAll() {
        return List.of();
    }

    @Override
    public void close() {

    }
}
