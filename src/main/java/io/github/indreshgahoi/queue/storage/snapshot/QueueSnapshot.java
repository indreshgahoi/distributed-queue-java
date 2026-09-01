package io.github.indreshgahoi.queue.storage.snapshot;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;

import java.util.List;
import java.util.Objects;

public record QueueSnapshot(
        StorageLineage storageLineage,
        WalPosition walPosition,
        List<ReadySnapshotEntry> ready,
        List<InFlightSnapshotEntry> inFlight,
        List<DelayedSnapshotEntry> delayed,
        List<DeadLetterSnapshotEntry> deadLetters
) {
    public QueueSnapshot {
        Objects.requireNonNull(storageLineage, "storageLineage");
        Objects.requireNonNull(walPosition, "walPosition");
        ready = List.copyOf(ready);
        inFlight = List.copyOf(inFlight);
        delayed = List.copyOf(delayed);
        deadLetters = List.copyOf(deadLetters);
    }
}
