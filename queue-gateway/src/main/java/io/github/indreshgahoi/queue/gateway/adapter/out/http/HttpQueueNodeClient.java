package io.github.indreshgahoi.queue.gateway.adapter.out.http;

import io.github.indreshgahoi.queue.gateway.application.port.out.QueueNodeClient;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNodeUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardRequest;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

@Component
final class HttpQueueNodeClient implements QueueNodeClient {
    private final RestClient.Builder clients;

    @Autowired
    HttpQueueNodeClient(ClientHttpRequestFactory requestFactory) {
        clients = RestClient.builder().requestFactory(requestFactory);
    }

    HttpQueueNodeClient(RestClient.Builder clients) {
        this.clients = clients;
    }

    @Override
    public ForwardResponse forward(
            QueueRoute route,
            ForwardRequest request
    ) {
        try {
            RestClient.RequestBodySpec outbound = clients.build()
                    .method(HttpMethod.valueOf(request.method().name()))
                    .uri(target(route.nodeEndpoint(), request.path()));
            if (request.contentType() != null) {
                outbound.header(HttpHeaders.CONTENT_TYPE, request.contentType());
            }
            if (request.body() != null) {
                outbound.body(request.body());
            }
            return outbound.exchange((ignored, response) -> new ForwardResponse(
                    response.getStatusCode().value(),
                    response.getHeaders().getFirst(HttpHeaders.CONTENT_TYPE),
                    publicLocation(response.getHeaders().getLocation()),
                    readBody(response.getBody())
            ));
        } catch (RuntimeException failure) {
            throw new QueueNodeUnavailableException(route.nodeId(), failure);
        }
    }

    private URI target(URI endpoint, String path) {
        String base = endpoint.toString();
        return URI.create(
                (base.endsWith("/")
                        ? base.substring(0, base.length() - 1)
                        : base) + path
        );
    }

    private URI publicLocation(URI location) {
        if (location == null || !location.isAbsolute()) {
            return location;
        }
        return URI.create(location.getRawPath());
    }

    private String readBody(java.io.InputStream body) throws IOException {
        byte[] bytes = body.readAllBytes();
        return bytes.length == 0
                ? null
                : new String(bytes, StandardCharsets.UTF_8);
    }
}
