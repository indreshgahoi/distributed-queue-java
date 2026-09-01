package io.github.indreshgahoi.queue.metadata.domain.model;

public enum QueueLifecycleState {
    PROVISIONING,
    ACTIVE,
    PROVISIONING_FAILED,
    DELETING,
    DELETE_FAILED,
    DELETED
}
