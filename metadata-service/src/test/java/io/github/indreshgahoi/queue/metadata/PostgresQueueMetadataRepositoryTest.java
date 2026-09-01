package io.github.indreshgahoi.queue.metadata;

import io.github.indreshgahoi.queue.metadata.application.port.in.QueueCatalogUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueLifecycleUseCase;
import io.github.indreshgahoi.queue.metadata.domain.exception.IdempotencyConflictException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueAlreadyExistsException;
import io.github.indreshgahoi.queue.metadata.domain.exception.StaleQueueMetadataException;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
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
                    "TRUNCATE metadata_requests, queues CASCADE"
            );
        }
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
}
