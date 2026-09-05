package io.github.indreshgahoi.queue.storage.replication;

public final class HistoryReclaimedException extends ReplicaException {
    private final long lastIncludedIndex;

    public HistoryReclaimedException(long lastIncludedIndex) {
        super("Requested history was reclaimed through index " + lastIncludedIndex);
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }
}
