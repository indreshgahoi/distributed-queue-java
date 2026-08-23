package io.github.indreshgahoi.queue.storage.wal;


import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.List;

public interface WriteAheadLog extends AutoCloseable {

    void append(WalRecord record);

    List<WalRecord> readAll();

    WalPosition currentDurablePosition();

    List<WalRecord> readFrom(WalPosition position);

    @Override
    void close();
}