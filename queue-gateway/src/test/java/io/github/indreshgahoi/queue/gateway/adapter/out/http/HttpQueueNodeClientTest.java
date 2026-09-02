package io.github.indreshgahoi.queue.gateway.adapter.out.http;

import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNodeUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardRequest;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class HttpQueueNodeClientTest {
    private final UUID queueId = UUID.randomUUID();
    private final QueueRoute route = new QueueRoute(
            queueId,
            UUID.randomUUID(),
            0,
            "node-a",
            URI.create("http://node-a:8081"),
            4,
            7
    );

    @Test
    void preservesDownstreamCapacityResponse() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer
                .bindTo(builder)
                .build();
        String path = "/v1/queues/" + queueId + "/messages";
        server.expect(once(), requestTo("http://node-a:8081" + path))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().json("{\"payload\":\"order\"}"))
                .andRespond(withStatus(HttpStatus.TOO_MANY_REQUESTS)
                        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                        .body("{\"type\":\"urn:queue-capacity\"}"));

        ForwardResponse response = new HttpQueueNodeClient(builder).forward(
                route,
                new ForwardRequest(
                        ForwardRequest.Method.POST,
                        path,
                        "{\"payload\":\"order\"}",
                        MediaType.APPLICATION_JSON_VALUE
                )
        );

        assertEquals(429, response.statusCode());
        assertEquals(
                MediaType.APPLICATION_PROBLEM_JSON_VALUE,
                response.contentType()
        );
        assertEquals("{\"type\":\"urn:queue-capacity\"}", response.body());
        server.verify();
    }

    @Test
    void transportFailureMakesExactlyOneNodeAttempt() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer
                .bindTo(builder)
                .build();
        String path = "/v1/queues/" + queueId + "/messages";
        server.expect(once(), requestTo("http://node-a:8081" + path))
                .andRespond(withException(new IOException("response lost")));

        assertThrows(
                QueueNodeUnavailableException.class,
                () -> new HttpQueueNodeClient(builder).forward(
                        route,
                        new ForwardRequest(
                                ForwardRequest.Method.POST,
                                path,
                                "{}",
                                MediaType.APPLICATION_JSON_VALUE
                        )
                )
        );
        server.verify();
    }
}
