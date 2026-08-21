package io.github.indreshgahoi.queue;

import java.util.List;

public interface WriteAheadLog extends AutoCloseable {

    void append(WalRecord record);

    List<WalRecord> readAll();

    @Override
    void close();
}