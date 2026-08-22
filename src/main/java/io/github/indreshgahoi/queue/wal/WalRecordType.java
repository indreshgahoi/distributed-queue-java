package io.github.indreshgahoi.queue.wal;

public enum WalRecordType {
    PUBLISH,
    ACK,
    NACK,
    LEASE_EXPIRED,
    DEAD_LETTER
}