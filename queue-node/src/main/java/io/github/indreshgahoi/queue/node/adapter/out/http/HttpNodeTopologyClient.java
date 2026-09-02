package io.github.indreshgahoi.queue.node.adapter.out.http;

import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Component
final class HttpNodeTopologyClient
        implements NodeTopologyClient, RuntimeTopologyClient {
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

    @Override
    public List<PartitionPlacement> activePlacements(
            NodeRegistration registration
    ) {
        PlacementResponse[] response = restClient.get()
                .uri(
                        builder -> builder
                                .path("/internal/v1/nodes/{nodeId}/"
                                        + "runtime-placements")
                                .queryParam(
                                        "registrationEpoch",
                                        registration.registrationEpoch()
                                )
                                .build(registration.nodeId())
                )
                .retrieve()
                .body(PlacementResponse[].class);
        if (response == null) {
            return List.of();
        }
        return Arrays.stream(response)
                .map(PlacementResponse::toDomain)
                .toList();
    }

    @Override
    public void publishStatus(
            RuntimePartitionIdentity identity,
            RuntimePartitionState state,
            String failureReason
    ) {
        restClient.post()
                .uri(
                        "/internal/v1/partitions/{queueId}/{generationId}/"
                                + "{partitionId}/runtime-status",
                        identity.queueId(),
                        identity.generationId(),
                        identity.partitionId()
                )
                .body(new RuntimeStatusRequest(
                        identity.nodeId(),
                        identity.registrationEpoch(),
                        identity.placementEpoch(),
                        state,
                        failureReason
                ))
                .retrieve()
                .toBodilessEntity();
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

    private record PlacementResponse(
            UUID queueId,
            UUID generationId,
            int partitionId,
            String nodeId,
            long placementEpoch,
            long metadataVersion
    ) {
        PartitionPlacement toDomain() {
            return new PartitionPlacement(
                    queueId,
                    generationId,
                    partitionId,
                    nodeId,
                    placementEpoch,
                    metadataVersion
            );
        }
    }

    private record RuntimeStatusRequest(
            String nodeId,
            long registrationEpoch,
            long placementEpoch,
            RuntimePartitionState state,
            String failureReason
    ) {
    }
}
