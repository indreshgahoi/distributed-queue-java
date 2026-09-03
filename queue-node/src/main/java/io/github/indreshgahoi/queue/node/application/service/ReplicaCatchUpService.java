package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.FollowerReplicationClient;
import io.github.indreshgahoi.queue.node.application.port.out.LeaderReplicationSource;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Performs one bounded catch-up attempt. Scheduling and retry backoff are kept
 * outside this class so a failed or slow follower never occupies a queue lock.
 */
public final class ReplicaCatchUpService {

    private final LeaderReplicationSource source;
    private final FollowerReplicationClient client;
    private final int maximumEntries;

    public ReplicaCatchUpService(
            LeaderReplicationSource source,
            FollowerReplicationClient client,
            int maximumEntries
    ) {
        this.source = Objects.requireNonNull(source, "source");
        this.client = Objects.requireNonNull(client, "client");
        if (maximumEntries <= 0
                || maximumEntries
                > FollowerReplicationService.MAX_BATCH_ENTRIES) {
            throw new IllegalArgumentException(
                    "maximumEntries must be between 1 and "
                            + FollowerReplicationService.MAX_BATCH_ENTRIES
            );
        }
        this.maximumEntries = maximumEntries;
    }

    public Optional<ReplicaWalBatchResult> runOnce(
            URI followerEndpoint,
            StorageLineage lineage,
            long leaderEpoch,
            long nextSequence
    ) {
        List<WalRecord> records = source.read(
                lineage,
                nextSequence,
                maximumEntries
        );
        if (records.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(client.replicate(
                followerEndpoint,
                new ReplicaWalBatch(
                        lineage,
                        leaderEpoch,
                        nextSequence,
                        records
                )
        ));
    }
}
