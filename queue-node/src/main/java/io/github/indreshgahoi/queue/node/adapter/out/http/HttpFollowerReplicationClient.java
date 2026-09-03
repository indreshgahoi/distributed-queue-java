package io.github.indreshgahoi.queue.node.adapter.out.http;

import io.github.indreshgahoi.queue.node.application.port.out.FollowerReplicationClient;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.util.List;

@Component
final class HttpFollowerReplicationClient
        implements FollowerReplicationClient {

    private final RestClient restClient;

    HttpFollowerReplicationClient(RestClient.Builder builder) {
        restClient = builder.build();
    }

    @Override
    public ReplicaWalBatchResult replicate(
            URI followerEndpoint,
            ReplicaWalBatch batch
    ) {
        ReplicaResponse response = restClient.post()
                .uri(followerEndpoint.resolve(path(batch)))
                .body(new ReplicaRequest(
                        batch.leaderEpoch(),
                        batch.firstSequence(),
                        batch.records()
                ))
                .retrieve()
                .body(ReplicaResponse.class);
        if (response == null) {
            throw new IllegalStateException(
                    "Follower returned an empty replication response"
            );
        }
        return new ReplicaWalBatchResult(
                response.acceptedThroughSequence(),
                response.appendedEntries(),
                response.alreadyPresentEntries()
        );
    }

    private String path(ReplicaWalBatch batch) {
        return "/internal/v1/replicas/"
                + batch.lineage().queueId()
                + "/" + batch.lineage().generationId()
                + "/partitions/" + batch.lineage().partitionId()
                + "/wal";
    }

    private record ReplicaRequest(
            long leaderEpoch,
            long firstSequence,
            List<WalRecord> records
    ) {
    }

    private record ReplicaResponse(
            long acceptedThroughSequence,
            int appendedEntries,
            int alreadyPresentEntries
    ) {
    }
}
