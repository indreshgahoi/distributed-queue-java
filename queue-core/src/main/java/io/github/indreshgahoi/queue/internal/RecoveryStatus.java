package io.github.indreshgahoi.queue.internal;

public enum RecoveryStatus {
    READY,
    IN_FLIGHT,
    DELAYED,
    DEAD_LETTER,
    DONE
}