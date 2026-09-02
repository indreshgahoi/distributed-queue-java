package io.github.indreshgahoi.queue.metadata.adapter.out.postgres;

import io.github.indreshgahoi.queue.metadata.application.port.out.NodeTopologyRepository;
import io.github.indreshgahoi.queue.metadata.domain.exception.MetadataUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.exception.NodeLeaseLostException;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
class PostgresNodeTopologyRepository
        implements NodeTopologyRepository {
    private final DataSource dataSource;
    private final Clock clock;

    PostgresNodeTopologyRepository(
            DataSource dataSource,
            Clock clock
    ) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public NodeRegistration register(RegisterNodeCommand command) {
        Objects.requireNonNull(command, "command");
        Instant registeredAt = clock.instant();
        Instant leaseExpiresAt = registeredAt.plus(
                command.leaseDuration()
        );
        String sql = """
                INSERT INTO queue_nodes (
                    node_id,
                    endpoint,
                    registration_epoch,
                    lease_expires_at,
                    registered_at,
                    updated_at
                ) VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT (node_id) DO UPDATE
                SET endpoint = EXCLUDED.endpoint,
                    registration_epoch =
                        queue_nodes.registration_epoch + 1,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    registered_at = EXCLUDED.registered_at,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, command.nodeId());
            statement.setString(2, command.endpoint().toString());
            statement.setTimestamp(3, Timestamp.from(leaseExpiresAt));
            statement.setTimestamp(4, Timestamp.from(registeredAt));
            statement.setTimestamp(5, Timestamp.from(registeredAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return mapNode(resultSet);
            }
        } catch (SQLException e) {
            throw databaseFailure("register queue node", e);
        }
    }

    @Override
    public NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    ) {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "leaseDuration must be positive"
            );
        }
        Instant heartbeatAt = clock.instant();
        String sql = """
                UPDATE queue_nodes
                SET lease_expires_at = ?,
                    updated_at = ?
                WHERE node_id = ?
                  AND registration_epoch = ?
                  AND lease_expires_at > ?
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(
                    1,
                    Timestamp.from(heartbeatAt.plus(leaseDuration))
            );
            statement.setTimestamp(2, Timestamp.from(heartbeatAt));
            statement.setString(3, identity.nodeId());
            statement.setLong(4, identity.registrationEpoch());
            statement.setTimestamp(5, Timestamp.from(heartbeatAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new NodeLeaseLostException();
                }
                return mapNode(resultSet);
            }
        } catch (SQLException e) {
            throw databaseFailure("heartbeat queue node", e);
        }
    }

    @Override
    public List<NodeRegistration> nodes() {
        String sql = """
                SELECT *
                FROM queue_nodes
                ORDER BY node_id
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<NodeRegistration> nodes = new ArrayList<>();
            while (resultSet.next()) {
                nodes.add(mapNode(resultSet));
            }
            return List.copyOf(nodes);
        } catch (SQLException e) {
            throw databaseFailure("list queue nodes", e);
        }
    }

    @Override
    public List<PartitionPlacement> placements() {
        String sql = """
                SELECT *
                FROM queue_partition_placements
                ORDER BY queue_id, generation_id, partition_id
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<PartitionPlacement> placements = new ArrayList<>();
            while (resultSet.next()) {
                placements.add(new PartitionPlacement(
                        resultSet.getObject("queue_id", java.util.UUID.class),
                        resultSet.getObject(
                                "generation_id",
                                java.util.UUID.class
                        ),
                        resultSet.getInt("partition_id"),
                        resultSet.getString("node_id"),
                        resultSet.getLong("placement_epoch"),
                        resultSet.getLong("metadata_version")
                ));
            }
            return List.copyOf(placements);
        } catch (SQLException e) {
            throw databaseFailure("list partition placements", e);
        }
    }

    private NodeRegistration mapNode(ResultSet resultSet)
            throws SQLException {
        return new NodeRegistration(
                resultSet.getString("node_id"),
                URI.create(resultSet.getString("endpoint")),
                resultSet.getLong("registration_epoch"),
                resultSet.getTimestamp("lease_expires_at").toInstant(),
                resultSet.getTimestamp("registered_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant()
        );
    }

    private MetadataUnavailableException databaseFailure(
            String operation,
            SQLException cause
    ) {
        return new MetadataUnavailableException(
                "Failed to " + operation,
                cause
        );
    }
}

