package io.github.indreshgahoi.queue.node.adapter.in.web;

import io.github.indreshgahoi.queue.node.application.service.FollowerReplicationService;
import io.github.indreshgahoi.queue.storage.replication.OrderedFollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.wal.InMemoryWriteAheadLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class FollowerReplicationControllerTest {

    private final UUID queueId = UUID.randomUUID();
    private final UUID generationId = UUID.randomUUID();
    private MockMvc mvc;

    @TempDir
    Path tempDirectory;

    @BeforeEach
    void setUp() {
        FollowerReplicationService service =
                new FollowerReplicationService(lineage ->
                        new OrderedFollowerReplicaLog(
                                new InMemoryWriteAheadLog(lineage),
                                tempDirectory.resolve("epoch.bin")
                        ));
        mvc = MockMvcBuilders.standaloneSetup(
                        new FollowerReplicationController(service)
                )
                .setControllerAdvice(new QueueNodeExceptionHandler())
                .build();
    }

    @Test
    void appendsBoundedBatch() throws Exception {
        mvc.perform(post(path())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson(1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acceptedThroughSequence").value(1))
                .andExpect(jsonPath("$.appendedEntries").value(1));
    }

    @Test
    void gapReturnsExpectedSequenceConflict() throws Exception {
        mvc.perform(post(path())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson(2)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.expectedSequence").value(1))
                .andExpect(jsonPath("$.suppliedSequence").value(2));
    }

    private String path() {
        return "/internal/v1/replicas/" + queueId
                + "/" + generationId
                + "/partitions/0/wal";
    }

    private String requestJson(long firstSequence) {
        return """
                {
                  "leaderEpoch": 4,
                  "firstSequence": %d,
                  "records": [{
                    "type": "PUBLISH",
                    "messageId": "message-1",
                    "payload": "payload",
                    "receiptHandle": null,
                    "attempt": 0,
                    "timestamp": "2026-09-02T00:00:00Z"
                  }]
                }
                """.formatted(firstSequence);
    }
}
