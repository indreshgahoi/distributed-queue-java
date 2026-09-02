package io.github.indreshgahoi.queue.node.adapter.out.http;

import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

@Component
final class HttpNodeTopologyClient implements NodeTopologyClient {
    private final RestClient restClient;

    HttpNodeTopologyClient(
            RestClient.Builder builder,
            QueueNodeProperties properties
    ) {
        restClient = builder.baseUrl(
                properties.metadataBaseUrl().toString()
        ).build();
    }

    @Override
    public NodeRegistration register(
            String nodeId,
            URI endpoint,
            Duration leaseDuration
    ) {
        RegistrationResponse response = restClient.post()
                .uri("/internal/v1/nodes/registrations")
                .body(new RegisterRequest(
                        nodeId,
                        endpoint,
                        leaseDuration.toSeconds()
                ))
                .retrieve()
                .body(RegistrationResponse.class);
        return response.toDomain();
    }

    @Override
    public NodeRegistration heartbeat(
            NodeRegistration registration,
            Duration leaseDuration
    ) {
        RegistrationResponse response = restClient.post()
                .uri(
                        "/internal/v1/nodes/{nodeId}/heartbeat",
                        registration.nodeId()
                )
                .body(new HeartbeatRequest(
                        registration.registrationEpoch(),
                        leaseDuration.toSeconds()
                ))
                .retrieve()
                .body(RegistrationResponse.class);
        return response.toDomain();
    }

    private record RegisterRequest(
            String nodeId,
            URI endpoint,
            long leaseSeconds
    ) {
    }

    private record HeartbeatRequest(
            long registrationEpoch,
            long leaseSeconds
    ) {
    }

    private record RegistrationResponse(
            String nodeId,
            URI endpoint,
            long registrationEpoch,
            Instant leaseExpiresAt,
            Instant registeredAt,
            Instant updatedAt
    ) {
        NodeRegistration toDomain() {
            return new NodeRegistration(
                    nodeId,
                    registrationEpoch,
                    leaseExpiresAt
            );
        }
    }
}

