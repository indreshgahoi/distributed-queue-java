package io.github.indreshgahoi.queue.metadata;

import io.github.indreshgahoi.queue.metadata.application.port.in.QueueCatalogUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueLifecycleUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueProvisioningUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.NodeTopologyUseCase;
import io.github.indreshgahoi.queue.metadata.domain.exception.ProvisioningClaimLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.NodeLeaseLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.PartitionRuntimeAuthorityLostException;
import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;
import io.github.indreshgahoi.queue.metadata.domain.exception.IdempotencyConflictException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueAlreadyExistsException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.exception.StaleQueueMetadataException;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueRoute;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@Import(PostgresQueueMetadataRepositoryTest.ClockConfiguration.class)
class PostgresQueueMetadataRepositoryTest {

    @Container
    private static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:17-alpine");

    @Autowired
    private DataSource dataSource;

    @Autowired
    private QueueCatalogUseCase catalog;

    @Autowired
    private QueueLifecycleUseCase lifecycle;

    @Autowired
    private QueueProvisioningUseCase provisioning;

    @Autowired
    private NodeTopologyUseCase topology;

    @Autowired
    private TestClock clock;

    @LocalServerPort
    private int serverPort;

    @DynamicPropertySource
    static void postgresProperties(
            DynamicPropertyRegistry registry
    ) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @BeforeEach
    void resetDatabase()
            throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(
                    "TRUNCATE queue_partition_placements, queue_nodes, "
                            + "queue_provisioning_claims, "
                            + "metadata_requests, queues CASCADE"
            );
        }
        clock.set(Instant.parse("2026-09-01T12:00:00Z"));
    }

    @Test
    void createIsDurableAndRetryReturnsSameIdentity() {
        CreateQueueCommand command =
                command("tenant-a", "orders", "request-1");

        QueueDescriptor created = catalog.createQueue(command);
        QueueDescriptor retried = catalog.createQueue(command);

        assertEquals(created, retried);
        assertEquals(
                QueueLifecycleState.PROVISIONING,
                created.lifecycleState()
        );
        assertEquals(
                created,
                catalog.getQueue("tenant-a", "orders")
                        .orElseThrow()
        );
        assertEquals(
                created.queueId(),
                created.storageLineage(0).queueId()
        );
        assertEquals(
                created.generationId(),
                created.storageLineage(0).generationId()
        );
    }

    @Test
    void idempotencyKeyCannotBeReusedForDifferentRequest() {
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );

        assertThrows(
                IdempotencyConflictException.class,
                () -> catalog.createQueue(
                        command(
                                "tenant-a",
                                "payments",
                                "request-1"
                        )
                )
        );
    }

    @Test
    void differentRequestCannotCreateSameLiveTenantQueueName() {
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );

        assertThrows(
                QueueAlreadyExistsException.class,
                () -> catalog.createQueue(
                        command(
                                "tenant-a",
                                "orders",
                                "request-2"
                        )
                )
        );

        QueueDescriptor another = catalog.createQueue(
                command("tenant-a", "payments", "request-2")
        );
        assertEquals("payments", another.queueName());
    }

    @Test
    void tenantsHaveIndependentNamespacesAndLists() {
        QueueDescriptor first = catalog.createQueue(
                command("tenant-a", "orders", "a-1")
        );
        QueueDescriptor second = catalog.createQueue(
                command("tenant-b", "orders", "b-1")
        );

        assertNotEquals(first.queueId(), second.queueId());
        assertEquals(List.of(first), catalog.listQueues("tenant-a"));
        assertEquals(List.of(second), catalog.listQueues("tenant-b"));
    }

    @Test
    void staleVersionCannotRepeatLifecycleTransition() {
        QueueDescriptor provisioning = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        QueueDescriptor active =
                lifecycle.completeProvisioning(provisioning);

        assertEquals(QueueLifecycleState.ACTIVE, active.lifecycleState());
        assertEquals(1, active.metadataVersion());
        assertThrows(
                StaleQueueMetadataException.class,
                () -> lifecycle.completeProvisioning(provisioning)
        );
    }

    @Test
    void failedLifecycleWorkCanBeReturnedToRetryableState() {
        QueueDescriptor provisioning = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        QueueDescriptor provisioningFailed =
                lifecycle.failProvisioning(provisioning);
        QueueDescriptor retryingProvisioning =
                lifecycle.retryProvisioning(provisioningFailed);
        QueueDescriptor active =
                lifecycle.completeProvisioning(retryingProvisioning);
        QueueDescriptor deleting =
                catalog.beginDeleteQueue("tenant-a", "orders");
        QueueDescriptor deleteFailed =
                lifecycle.failDeletion(deleting);
        QueueDescriptor retryingDeletion =
                lifecycle.retryDeletion(deleteFailed);

        assertEquals(
                QueueLifecycleState.DELETING,
                retryingDeletion.lifecycleState()
        );
        assertEquals(
                active.metadataVersion() + 3,
                retryingDeletion.metadataVersion()
        );
    }

    @Test
    void deleteAndRecreateUsesFreshStorageLineage() {
        QueueDescriptor provisioning = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        QueueDescriptor active =
                lifecycle.completeProvisioning(provisioning);
        QueueDescriptor deleting =
                catalog.beginDeleteQueue("tenant-a", "orders");
        QueueDescriptor deleted =
                lifecycle.completeDeletion(deleting);

        assertEquals(
                QueueLifecycleState.DELETED,
                deleted.lifecycleState()
        );
        QueueDescriptor replacement = catalog.createQueue(
                command("tenant-a", "orders", "request-2")
        );
        assertNotEquals(active.queueId(), replacement.queueId());
        assertNotEquals(
                active.generationId(),
                replacement.generationId()
        );
    }

    @Test
    void concurrentIdempotentCreatesReturnOneIdentity()
            throws Exception {
        CreateQueueCommand command =
                command("tenant-a", "orders", "request-1");
        List<Callable<QueueDescriptor>> calls =
                IntStream.range(0, 12)
                        .mapToObj(ignored ->
                                (Callable<QueueDescriptor>) () ->
                                        catalog.createQueue(command)
                        )
                        .toList();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Set<java.util.UUID> queueIds = executor.invokeAll(calls)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get().queueId();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    })
                    .collect(Collectors.toSet());

            assertEquals(1, queueIds.size());
        }
    }

    @Test
    void provisioningClaimIsExclusiveAndCompletionActivatesQueue() {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );

        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        "node-a",
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();

        assertEquals(queue.queueId(), claim.identity().queueId());
        assertEquals(1, claim.identity().fencingToken());
        assertTrue(provisioning.claim(
                new ClaimProvisioningCommand(
                        "node-a",
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).isEmpty());

        QueueDescriptor active = provisioning.complete(
                claim.identity()
        );
        assertEquals(QueueLifecycleState.ACTIVE, active.lifecycleState());
        assertEquals(1, active.metadataVersion());
        assertEquals(active, provisioning.complete(claim.identity()));
    }

    @Test
    void registrationEpochFencesPreviousProcessIncarnation() {
        NodeRegistration first = register("node-a");
        NodeRegistration second = register("node-a");

        assertEquals(
                first.registrationEpoch() + 1,
                second.registrationEpoch()
        );
        assertThrows(
                NodeLeaseLostException.class,
                () -> topology.heartbeat(
                        new NodeLeaseIdentity(
                                first.nodeId(),
                                first.registrationEpoch()
                        ),
                        Duration.ofMinutes(5)
                )
        );
        assertEquals(
                second.registrationEpoch(),
                topology.heartbeat(
                        new NodeLeaseIdentity(
                                second.nodeId(),
                                second.registrationEpoch()
                        ),
                        Duration.ofMinutes(5)
                ).registrationEpoch()
        );
    }

    @Test
    void concurrentRegistrationsReceiveUniqueIncreasingEpochs()
            throws Exception {
        List<Callable<Long>> calls = IntStream.range(0, 12)
                .mapToObj(index -> (Callable<Long>) () ->
                        register("node-a").registrationEpoch())
                .toList();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Set<Long> epochs = executor.invokeAll(calls)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    })
                    .collect(Collectors.toSet());

            assertEquals(12, epochs.size());
            assertEquals(1L, epochs.stream().min(Long::compare).orElseThrow());
            assertEquals(12L, epochs.stream().max(Long::compare).orElseThrow());
        }
    }

    @Test
    void expiredRegistrationCannotHeartbeatOrClaim() {
        NodeRegistration node = register("node-a");
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        clock.advance(Duration.ofMinutes(6));

        assertThrows(
                NodeLeaseLostException.class,
                () -> topology.heartbeat(
                        new NodeLeaseIdentity(
                                node.nodeId(),
                                node.registrationEpoch()
                        ),
                        Duration.ofMinutes(5)
                )
        );
        assertThrows(
                NodeLeaseLostException.class,
                () -> provisioning.claim(
                        new ClaimProvisioningCommand(
                                node.nodeId(),
                                node.registrationEpoch(),
                                Duration.ofSeconds(30)
                        )
                )
        );
    }

    @Test
    void onlyAssignedLiveNodeCanClaimProvisioning() {
        NodeRegistration first = register("node-a");
        NodeRegistration second = register("node-b");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );

        assertTrue(provisioning.claim(
                new ClaimProvisioningCommand(
                        second.nodeId(),
                        second.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).isEmpty());
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        first.nodeId(),
                        first.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();

        assertEquals(first.nodeId(), claim.identity().workerId());
        assertEquals(1, claim.identity().placementEpoch());
        assertEquals(queue.queueId(), topology.placements().getFirst().queueId());
        assertEquals(first.nodeId(), topology.placements().getFirst().nodeId());
    }

    @Test
    void changedPlacementEpochFencesExistingClaim()
            throws SQLException {
        NodeRegistration node = register("node-a");
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "UPDATE queue_partition_placements "
                            + "SET placement_epoch = placement_epoch + 1, "
                            + "metadata_version = metadata_version + 1"
            );
        }

        assertThrows(
                ProvisioningClaimLostException.class,
                () -> provisioning.complete(claim.identity())
        );
    }

    @Test
    void expiredClaimIsTakenOverAndOldTokenIsFenced() {
        NodeRegistration node = register("node-a");
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim first = provisioning.claim(
                new ClaimProvisioningCommand(
                        "node-a",
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();

        clock.advance(Duration.ofSeconds(31));
        ProvisioningClaim second = provisioning.claim(
                new ClaimProvisioningCommand(
                        "node-a",
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();

        assertEquals(2, second.identity().fencingToken());
        assertThrows(
                ProvisioningClaimLostException.class,
                () -> provisioning.complete(first.identity())
        );
        assertEquals(
                QueueLifecycleState.ACTIVE,
                provisioning.complete(second.identity()).lifecycleState()
        );
    }

    @Test
    void concurrentWorkersReceiveOnlyOneActiveClaim()
            throws Exception {
        NodeRegistration node = register("node-a");
        catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        List<Callable<java.util.Optional<ProvisioningClaim>>> calls =
                IntStream.range(0, 12)
                        .mapToObj(index ->
                                (Callable<java.util.Optional<ProvisioningClaim>>)
                                        () -> provisioning.claim(
                                                new ClaimProvisioningCommand(
                                                        "node-a",
                                                        node.registrationEpoch(),
                                                        Duration.ofSeconds(30)
                                                )
                                        )
                        )
                        .toList();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            long claims = executor.invokeAll(calls)
                    .stream()
                    .filter(future -> {
                        try {
                            return future.get().isPresent();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    })
                    .count();

            assertEquals(1, claims);
        }
    }

    @Test
    void currentRegistrationAndPlacementCanPublishReadyRuntime() {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());
        PartitionRuntimeIdentity identity = runtimeIdentity(queue, node, claim);

        topology.publishRuntimeStatus(
                identity,
                PartitionRuntimeState.READY,
                null
        );

        assertEquals(1, topology.runtimeStatuses().size());
        assertEquals(
                PartitionRuntimeState.READY,
                topology.runtimeStatuses().getFirst().state()
        );
        assertEquals(identity, topology.runtimeStatuses().getFirst().identity());

        QueueRoute route = topology.resolveReadyRoute(queue.queueId());
        assertEquals(queue.generationId(), route.generationId());
        assertEquals(node.nodeId(), route.nodeId());
        assertEquals(URI.create("http://node-a:8081"), route.nodeEndpoint());
        assertEquals(identity.placementEpoch(), route.placementEpoch());
        assertEquals(identity.registrationEpoch(), route.registrationEpoch());
    }

    @Test
    void routeIsUnavailableUntilRuntimePublishesReady() {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());

        assertThrows(
                QueueRouteUnavailableException.class,
                () -> topology.resolveReadyRoute(queue.queueId())
        );
    }

    @Test
    void newerNodeRegistrationInvalidatesPreviouslyReadyRoute() {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());
        topology.publishRuntimeStatus(
                runtimeIdentity(queue, node, claim),
                PartitionRuntimeState.READY,
                null
        );
        register("node-a");

        assertThrows(
                QueueRouteUnavailableException.class,
                () -> topology.resolveReadyRoute(queue.queueId())
        );
    }

    @Test
    void routeResolutionDistinguishesUnknownQueue() {
        assertThrows(
                QueueNotFoundException.class,
                () -> topology.resolveReadyRoute(java.util.UUID.randomUUID())
        );
    }

    @Test
    void runtimeDiscoveryReturnsOnlyActivePlacementsForCurrentNode() {
        NodeRegistration node = register("node-a");
        catalog.createQueue(command("tenant-a", "orders", "request-1"));
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        NodeLeaseIdentity identity = new NodeLeaseIdentity(
                node.nodeId(),
                node.registrationEpoch()
        );

        assertTrue(topology.activePlacements(identity).isEmpty());

        provisioning.complete(claim.identity());

        assertEquals(1, topology.activePlacements(identity).size());
    }

    @Test
    void newerRegistrationEpochFencesRuntimePublication() {
        NodeRegistration first = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        first.nodeId(),
                        first.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());
        topology.register(new RegisterNodeCommand(
                "node-a",
                URI.create("http://node-a:8081"),
                Duration.ofMinutes(5)
        ));

        assertThrows(
                PartitionRuntimeAuthorityLostException.class,
                () -> topology.publishRuntimeStatus(
                        runtimeIdentity(queue, first, claim),
                        PartitionRuntimeState.READY,
                        null
                )
        );
    }

    @Test
    void changedPlacementEpochFencesRuntimePublication()
            throws SQLException {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "UPDATE queue_partition_placements "
                            + "SET placement_epoch = placement_epoch + 1"
            );
        }

        assertThrows(
                PartitionRuntimeAuthorityLostException.class,
                () -> topology.publishRuntimeStatus(
                        runtimeIdentity(queue, node, claim),
                        PartitionRuntimeState.READY,
                        null
                )
        );
    }

    @Test
    void expiredNodeLeaseFencesRuntimePublication() {
        NodeRegistration node = register("node-a");
        QueueDescriptor queue = catalog.createQueue(
                command("tenant-a", "orders", "request-1")
        );
        ProvisioningClaim claim = provisioning.claim(
                new ClaimProvisioningCommand(
                        node.nodeId(),
                        node.registrationEpoch(),
                        Duration.ofSeconds(30)
                )
        ).orElseThrow();
        provisioning.complete(claim.identity());
        clock.advance(Duration.ofMinutes(6));

        assertThrows(
                PartitionRuntimeAuthorityLostException.class,
                () -> topology.publishRuntimeStatus(
                        runtimeIdentity(queue, node, claim),
                        PartitionRuntimeState.FAILED,
                        "recovery failed"
                )
        );
    }

    private PartitionRuntimeIdentity runtimeIdentity(
            QueueDescriptor queue,
            NodeRegistration node,
            ProvisioningClaim claim
    ) {
        return new PartitionRuntimeIdentity(
                queue.queueId(),
                queue.generationId(),
                0,
                node.nodeId(),
                node.registrationEpoch(),
                claim.identity().placementEpoch()
        );
    }

    private NodeRegistration register(String nodeId) {
        return topology.register(new RegisterNodeCommand(
                nodeId,
                URI.create("http://" + nodeId + ":8081"),
                Duration.ofMinutes(5)
        ));
    }

    @Test
    void httpBoundaryCreatesAndReadsQueue()
            throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI collectionUri = URI.create(
                "http://127.0.0.1:"
                        + serverPort
                        + "/api/v1/tenants/tenant-a/queues"
        );
        HttpRequest create = HttpRequest.newBuilder(collectionUri)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", "request-1")
                .POST(
                        HttpRequest.BodyPublishers.ofString(
                                "{\"queueName\":\"orders\"}"
                        )
                )
                .build();

        HttpResponse<String> created = client.send(
                create,
                HttpResponse.BodyHandlers.ofString()
        );
        HttpResponse<String> found = client.send(
                HttpRequest.newBuilder(
                        URI.create(collectionUri + "/orders")
                ).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        );

        assertEquals(201, created.statusCode());
        assertEquals(200, found.statusCode());
        assertTrue(found.body().contains("\"queueName\":\"orders\""));
        assertTrue(found.body().contains("\"lifecycleState\":\"PROVISIONING\""));
    }

    @Test
    void httpBoundaryUsesProblemDetailsForInvalidRequest()
            throws Exception {
        URI collectionUri = collectionUri();
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(collectionUri)
                                .header(
                                        "Content-Type",
                                        "application/json"
                                )
                                .POST(
                                        HttpRequest.BodyPublishers.ofString(
                                                "{\"queueName\":\"orders\"}"
                                        )
                                )
                                .build(),
                        HttpResponse.BodyHandlers.ofString()
                );

        assertEquals(400, response.statusCode());
        assertTrue(
                response.headers()
                        .firstValue("Content-Type")
                        .orElseThrow()
                        .startsWith("application/problem+json")
        );
        assertTrue(response.body().contains("invalid-request"));
    }

    @Test
    void actuatorHealthIncludesDatabaseReadiness()
            throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(
                                URI.create(
                                        "http://127.0.0.1:"
                                                + serverPort
                                                + "/actuator/health"
                                )
                        ).GET().build(),
                        HttpResponse.BodyHandlers.ofString()
                );

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"UP\""));
    }

    @Test
    void openApiDocumentAndSwaggerUiAreAvailable()
            throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        HttpResponse<String> openApi = client.send(
                HttpRequest.newBuilder(
                        URI.create(
                                "http://127.0.0.1:"
                                        + serverPort
                                        + "/v3/api-docs"
                        )
                ).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        );
        HttpResponse<String> swaggerUi = client.send(
                HttpRequest.newBuilder(
                        URI.create(
                                "http://127.0.0.1:"
                                        + serverPort
                                        + "/swagger-ui.html"
                        )
                ).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        );

        assertEquals(200, openApi.statusCode());
        assertTrue(openApi.body().contains("Queue Metadata Service API"));
        assertTrue(openApi.body().contains("create-orders-001"));
        assertTrue(openApi.body().contains("orders"));
        assertEquals(200, swaggerUi.statusCode());
        assertTrue(swaggerUi.body().contains("Swagger UI"));
    }

    private URI collectionUri() {
        return URI.create(
                "http://127.0.0.1:"
                        + serverPort
                        + "/api/v1/tenants/tenant-a/queues"
        );
    }

    private CreateQueueCommand command(
            String tenantId,
            String queueName,
            String idempotencyKey
    ) {
        return new CreateQueueCommand(
                tenantId,
                queueName,
                idempotencyKey
        );
    }

    @TestConfiguration
    static class ClockConfiguration {
        @Bean
        @Primary
        TestClock testClock() {
            return new TestClock();
        }
    }

    static final class TestClock extends Clock {
        private Instant current =
                Instant.parse("2026-09-01T12:00:00Z");

        synchronized void set(Instant instant) {
            current = instant;
        }

        synchronized void advance(Duration duration) {
            current = current.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("Only UTC is supported");
            }
            return this;
        }

        @Override
        public synchronized Instant instant() {
            return current;
        }
    }
}
