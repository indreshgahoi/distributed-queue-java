package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaCatchUpServiceTest {

    @Test
    void sendsOneBoundedBatchFromRequestedSequence() {
        StorageLineage lineage = new StorageLineage(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0
        );
        long[] requestedSequence = {0};
        int[] requestedLimit = {0};
        ReplicaCatchUpService service = new ReplicaCatchUpService(
                (requestedLineage, sequence, limit) -> {
                    assertEquals(lineage, requestedLineage);
                    requestedSequence[0] = sequence;
                    requestedLimit[0] = limit;
                    return List.of(record("six"), record("seven"));
                },
                (endpoint, batch) -> {
                    assertEquals(URI.create("http://follower:8081"), endpoint);
                    assertEquals(9, batch.leaderEpoch());
                    assertEquals(6, batch.firstSequence());
                    return new ReplicaWalBatchResult(7, 2, 0);
                },
                32
        );

        ReplicaWalBatchResult result = service.runOnce(
                        URI.create("http://follower:8081"),
                        lineage,
                        9,
                        6
                )
                .orElseThrow();

        assertEquals(6, requestedSequence[0]);
        assertEquals(32, requestedLimit[0]);
        assertEquals(7, result.acceptedThroughSequence());
    }

    @Test
    void makesNoNetworkCallWhenLeaderHasNoNewRecords() {
        ReplicaCatchUpService service = new ReplicaCatchUpService(
                (lineage, sequence, limit) -> List.of(),
                (endpoint, batch) -> {
                    throw new AssertionError("must not contact follower");
                },
                32
        );

        assertTrue(service.runOnce(
                URI.create("http://follower:8081"),
                new StorageLineage(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        0
                ),
                9,
                1
        ).isEmpty());
    }

    private static WalRecord record(String payload) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                "message-" + payload,
                payload,
                null,
                0,
                Instant.parse("2026-09-02T00:00:00Z")
        );
    }
}
