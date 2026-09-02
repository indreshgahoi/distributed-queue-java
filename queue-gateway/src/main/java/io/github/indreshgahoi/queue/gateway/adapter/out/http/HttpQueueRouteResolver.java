package io.github.indreshgahoi.queue.gateway.adapter.out.http;

import io.github.indreshgahoi.queue.gateway.application.port.out.QueueRouteResolver;
import io.github.indreshgahoi.queue.gateway.config.QueueGatewayProperties;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.gateway.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.exception.RoutingMetadataUnavailableException;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.net.URI;
import java.util.UUID;

@Component
final class HttpQueueRouteResolver implements QueueRouteResolver {
    private final RestClient metadata;

    @Autowired
    HttpQueueRouteResolver(
            ClientHttpRequestFactory requestFactory,
            QueueGatewayProperties properties
    ) {
        metadata = RestClient.builder()
                .requestFactory(requestFactory)
                .baseUrl(
                properties.metadataBaseUrl().toString()
                ).build();
    }

    HttpQueueRouteResolver(RestClient metadata) {
        this.metadata = metadata;
    }

    @Override
    public QueueRoute resolveReadyRoute(UUID queueId) {
        try {
            QueueRouteResponse response = metadata.get()
                    .uri("/internal/v1/routes/queues/{queueId}", queueId)
                    .retrieve()
                    .body(QueueRouteResponse.class);
            if (response == null) {
                throw new RoutingMetadataUnavailableException(null);
            }
            return response.toDomain();
        } catch (RestClientResponseException failure) {
            if (failure.getStatusCode().value()
                    == HttpStatus.NOT_FOUND.value()) {
                throw new QueueNotFoundException(queueId);
            }
            if (failure.getStatusCode().value()
                    == HttpStatus.SERVICE_UNAVAILABLE.value()) {
                throw new QueueRouteUnavailableException(queueId);
            }
            throw new RoutingMetadataUnavailableException(failure);
        } catch (QueueNotFoundException | QueueRouteUnavailableException
                 | RoutingMetadataUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new RoutingMetadataUnavailableException(failure);
        }
    }

    private record QueueRouteResponse(
            UUID queueId,
            UUID generationId,
            int partitionId,
            String nodeId,
            URI nodeEndpoint,
            long placementEpoch,
            long registrationEpoch
    ) {
        QueueRoute toDomain() {
            return new QueueRoute(
                    queueId,
                    generationId,
                    partitionId,
                    nodeId,
                    nodeEndpoint,
                    placementEpoch,
                    registrationEpoch
            );
        }
    }
}
