package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.application.service.RuntimePartitionManager;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/internal/v1/runtime")
final class RuntimePartitionController {
    private final RuntimePartitionManager partitions;

    RuntimePartitionController(RuntimePartitionManager partitions) {
        this.partitions = partitions;
    }

    @GetMapping("/partitions")
    List<RuntimePartitionResponse> partitions() {
        return partitions.partitions().stream()
                .map(RuntimePartitionResponse::from)
                .toList();
    }

    record RuntimePartitionResponse(
            UUID queueId,
            UUID generationId,
            int partitionId,
            String nodeId,
            long registrationEpoch,
            long placementEpoch,
            RuntimePartitionState state,
            String failureReason
    ) {
        static RuntimePartitionResponse from(RuntimePartitionView view) {
            RuntimePartitionIdentity identity = view.identity();
            return new RuntimePartitionResponse(
                    identity.queueId(),
                    identity.generationId(),
                    identity.partitionId(),
                    identity.nodeId(),
                    identity.registrationEpoch(),
                    identity.placementEpoch(),
                    view.state(),
                    view.failureReason()
            );
        }
    }
}
