package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.application.port.in.FollowerReplicationUseCase;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(
        "/internal/v1/replicas/{queueId}/{generationId}/"
                + "partitions/{partitionId}/wal"
)
final class FollowerReplicationController {

    private final FollowerReplicationUseCase replication;

    FollowerReplicationController(
            FollowerReplicationUseCase replication
    ) {
        this.replication = replication;
    }

    @PostMapping
    @Operation(
            summary = "Append a bounded WAL batch to a follower replica",
            description = "Internal protocol. Exact retries are idempotent; "
                    + "gaps, conflicts, wrong lineage, and stale epochs fail."
    )
    ResponseEntity<ReplicaWalBatchResponse> append(
            @PathVariable UUID queueId,
            @PathVariable UUID generationId,
            @PathVariable int partitionId,
            @Valid @RequestBody ReplicaWalBatchRequest request
    ) {
        ReplicaWalBatchResult result = replication.replicate(
                new ReplicaWalBatch(
                        new StorageLineage(
                                queueId,
                                generationId,
                                partitionId
                        ),
                        request.leaderEpoch(),
                        request.firstSequence(),
                        request.records()
                )
        );
        return ResponseEntity.ok(
                ReplicaWalBatchResponse.from(result)
        );
    }

    record ReplicaWalBatchRequest(
            @Positive long leaderEpoch,
            @Positive long firstSequence,
            @NotEmpty @Size(max = 256) List<WalRecord> records
    ) {
    }

    record ReplicaWalBatchResponse(
            long acceptedThroughSequence,
            int appendedEntries,
            int alreadyPresentEntries
    ) {
        static ReplicaWalBatchResponse from(
                ReplicaWalBatchResult result
        ) {
            return new ReplicaWalBatchResponse(
                    result.acceptedThroughSequence(),
                    result.appendedEntries(),
                    result.alreadyPresentEntries()
            );
        }
    }
}
