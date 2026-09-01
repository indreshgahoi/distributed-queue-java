package io.github.indreshgahoi.queue.storage.snapshot;

import java.util.Optional;

public interface QueueSnapshotStore {

    void save(QueueSnapshot snapshot);

    Optional<QueueSnapshot> loadLatest();
}