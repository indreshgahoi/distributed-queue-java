package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;

public interface RuntimeQueueFactory {
    RuntimeQueue open(PartitionPlacement placement);
}
