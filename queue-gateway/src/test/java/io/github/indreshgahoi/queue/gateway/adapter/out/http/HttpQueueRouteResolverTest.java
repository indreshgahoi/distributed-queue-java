package io.github.indreshgahoi.queue.gateway.adapter.out.http;

import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class HttpQueueRouteResolverTest {
    private final UUID queueId = UUID.randomUUID();

    @Test
    void mapsAuthoritativeRouteResponse() {
        RestClient.Builder builder = RestClient.builder()
                .baseUrl("http://metadata:8080");
        MockRestServiceServer server = MockRestServiceServer
                .bindTo(builder)
                .build();
        server.expect(requestTo(
                        "http://metadata:8080/internal/v1/routes/queues/"
                                + queueId
                ))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(routeJson(), MediaType.APPLICATION_JSON));

        QueueRoute route = new HttpQueueRouteResolver(builder.build())
                .resolveReadyRoute(queueId);

        assertEquals(queueId, route.queueId());
        assertEquals("node-a", route.nodeId());
        assertEquals(URI.create("http://node-a:8081"), route.nodeEndpoint());
        assertEquals(4, route.placementEpoch());
        assertEquals(7, route.registrationEpoch());
        server.verify();
    }

    @Test
    void distinguishesUnknownQueueFromUnavailableRoute() {
        RestClient.Builder notFoundBuilder = RestClient.builder()
                .baseUrl("http://metadata:8080");
        MockRestServiceServer notFound = MockRestServiceServer
                .bindTo(notFoundBuilder)
                .build();
        notFound.expect(requestTo(
                        "http://metadata:8080/internal/v1/routes/queues/"
                                + queueId
                ))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        assertThrows(
                QueueNotFoundException.class,
                () -> new HttpQueueRouteResolver(notFoundBuilder.build())
                        .resolveReadyRoute(queueId)
        );
        notFound.verify();

        RestClient.Builder unavailableBuilder = RestClient.builder()
                .baseUrl("http://metadata:8080");
        MockRestServiceServer unavailable = MockRestServiceServer
                .bindTo(unavailableBuilder)
                .build();
        unavailable.expect(requestTo(
                        "http://metadata:8080/internal/v1/routes/queues/"
                                + queueId
                ))
                .andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        assertThrows(
                QueueRouteUnavailableException.class,
                () -> new HttpQueueRouteResolver(unavailableBuilder.build())
                        .resolveReadyRoute(queueId)
        );
        unavailable.verify();
    }

    private String routeJson() {
        return """
                {
                  "queueId":"%s",
                  "generationId":"%s",
                  "partitionId":0,
                  "nodeId":"node-a",
                  "nodeEndpoint":"http://node-a:8081",
                  "placementEpoch":4,
                  "registrationEpoch":7
                }
                """.formatted(queueId, UUID.randomUUID());
    }
}
