package io.github.indreshgahoi.queue.storage.wal;


import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.List;

public interface WriteAheadLog extends AutoCloseable {

    void append(WalRecord record);

    List<WalRecord> readAll();

    WalPosition currentDurablePosition();

    List<WalRecord> readFrom(WalPosition position);

    /**
     * Returns whether this WAL still contains history from the queue's
     * initial segment. A retained suffix is not sufficient for WAL-only
     * recovery.
     */
    default boolean hasCompleteHistory() {
        return true;
    }

    default void validatePosition(WalPosition position) {
        readFrom(position);
    }

    @Override
    void close();
}
