package io.github.indreshgahoi.queue.storage.replication;

import java.util.Objects;
import java.util.Optional;

public record ReplicaHardState(
        long currentTerm,
        Optional<String> votedFor,
        long commitIndex
) {
    public static final ReplicaHardState EMPTY =
            new ReplicaHardState(0, Optional.empty(), 0);

    public ReplicaHardState {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must not be negative");
        }
        Objects.requireNonNull(votedFor, "votedFor");
        if (votedFor.isPresent() && votedFor.get().isBlank()) {
            throw new IllegalArgumentException("votedFor must not be blank");
        }
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
    }
}
