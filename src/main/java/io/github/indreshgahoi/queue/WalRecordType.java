package io.github.indreshgahoi.queue;

enum WalRecordType {
    PUBLISH,
    ACK,
    NACK,
    LEASE_EXPIRED,
    DEAD_LETTER
}