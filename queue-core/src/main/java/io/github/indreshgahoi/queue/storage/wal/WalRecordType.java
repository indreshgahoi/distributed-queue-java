package io.github.indreshgahoi.queue.storage.wal;

public enum WalRecordType {
    PUBLISH,
    LEASE_STARTED,
    ACK,
    NACK,
    LEASE_EXPIRED,
    DEAD_LETTER
}