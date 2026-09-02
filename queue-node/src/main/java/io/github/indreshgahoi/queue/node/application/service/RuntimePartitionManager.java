package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.in.ReconcileRuntimePartitionsUseCase;
import io.github.indreshgahoi.queue.node.application.port.out.NodeRegistrationProvider;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionView;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reconciles PostgreSQL's desired placement state with open local queue
 * runtimes. The manager deliberately revalidates authority after recovery:
 * opening a WAL can be slow, and a result produced under an old registration
 * or placement must be closed rather than becoming visible as READY.
 */
@Slf4j
public final class RuntimePartitionManager
        implements ReconcileRuntimePartitionsUseCase, AutoCloseable {
    private final String nodeId;
    private final Clock clock;
    private final NodeRegistrationProvider registrations;
    private final RuntimeTopologyClient topology;
    private final RuntimeQueueFactory queues;
    private final Map<PartitionKey, ActivePartition> active = new HashMap<>();
    private final Map<PartitionKey, RuntimePartitionView> failures =
            new HashMap<>();

    public RuntimePartitionManager(
            String nodeId,
            Clock clock,
            NodeRegistrationProvider registrations,
            RuntimeTopologyClient topology,
            RuntimeQueueFactory queues
    ) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.registrations = Objects.requireNonNull(
                registrations,
                "registrations"
        );
        this.topology = Objects.requireNonNull(topology, "topology");
        this.queues = Objects.requireNonNull(queues, "queues");
    }

    @Override
    public synchronized void runOnce() {
        Optional<NodeRegistration> current = currentLiveRegistration();
        if (current.isEmpty()) {
            // A local runtime is useful only while its process incarnation is
            // authoritative. Fail closed instead of serving during ambiguity.
            deactivateAll("registration authority unavailable");
            failures.clear();
            return;
        }

        NodeRegistration registration = current.orElseThrow();
        Map<PartitionKey, DesiredPartition> desired = desiredPartitions(
                topology.activePlacements(registration),
                registration
        );
        deactivateSuperseded(desired);
        desired.forEach((key, partition) -> {
            ActivePartition existing = active.get(key);
            if (existing == null
                    || !existing.identity().equals(partition.identity())) {
                activate(key, partition);
            }
        });
    }

    public synchronized List<RuntimePartitionView> partitions() {
        Map<PartitionKey, RuntimePartitionView> views =
                new LinkedHashMap<>();
        active.forEach((key, partition) -> views.put(
                key,
                new RuntimePartitionView(
                        partition.identity(),
                        RuntimePartitionState.READY,
                        null
                )
        ));
        failures.forEach(views::putIfAbsent);
        return views.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    private Map<PartitionKey, DesiredPartition> desiredPartitions(
            List<PartitionPlacement> placements,
            NodeRegistration registration
    ) {
        Map<PartitionKey, DesiredPartition> desired = new HashMap<>();
        placements.stream()
                .filter(placement -> nodeId.equals(placement.nodeId()))
                .forEach(placement -> {
                    RuntimePartitionIdentity identity =
                            RuntimePartitionIdentity.from(
                                    placement,
                                    registration
                            );
                    desired.put(
                            PartitionKey.from(identity),
                            new DesiredPartition(placement, identity)
                    );
                });
        return desired;
    }

    private void deactivateSuperseded(
            Map<PartitionKey, DesiredPartition> desired
    ) {
        List<PartitionKey> stale = new ArrayList<>();
        active.forEach((key, partition) -> {
            DesiredPartition wanted = desired.get(key);
            if (wanted == null
                    || !partition.identity().equals(wanted.identity())) {
                stale.add(key);
            }
        });
        stale.forEach(key -> deactivate(key, "authority superseded"));
        failures.keySet().removeIf(key -> !desired.containsKey(key));
    }

    private void activate(
            PartitionKey key,
            DesiredPartition desired
    ) {
        RuntimeQueue queue = null;
        try {
            queue = queues.open(desired.placement());

            // Recovery completed asynchronously relative to control-plane
            // changes. Check locally, then let PostgreSQL perform the final
            // atomic authority validation while publishing READY.
            if (!stillCurrent(desired.identity())) {
                closeQuietly(queue);
                return;
            }
            topology.publishStatus(
                    desired.identity(),
                    RuntimePartitionState.READY,
                    null
            );
            if (!stillCurrent(desired.identity())) {
                closeQuietly(queue);
                return;
            }
            active.put(key, new ActivePartition(desired.identity(), queue));
            failures.remove(key);
            log.info(
                    "event=runtime_partition_activated queueId={} "
                            + "generationId={} partitionId={} nodeId={} "
                            + "registrationEpoch={} placementEpoch={}",
                    desired.identity().queueId(),
                    desired.identity().generationId(),
                    desired.identity().partitionId(),
                    desired.identity().nodeId(),
                    desired.identity().registrationEpoch(),
                    desired.identity().placementEpoch()
            );
        } catch (RuntimeException failure) {
            if (queue != null) {
                closeQuietly(queue);
            }
            recordFailure(key, desired.identity(), failure);
        }
    }

    private void recordFailure(
            PartitionKey key,
            RuntimePartitionIdentity identity,
            RuntimeException failure
    ) {
        String reason = failure.getClass().getSimpleName()
                + ": " + String.valueOf(failure.getMessage());
        if (reason.length() > 2048) {
            reason = reason.substring(0, 2048);
        }
        failures.put(
                key,
                new RuntimePartitionView(
                        identity,
                        RuntimePartitionState.FAILED,
                        reason
                )
        );
        if (stillCurrent(identity)) {
            try {
                topology.publishStatus(
                        identity,
                        RuntimePartitionState.FAILED,
                        reason
                );
            } catch (RuntimeException publicationFailure) {
                log.debug(
                        "event=runtime_partition_failure_publication_rejected "
                                + "queueId={} generationId={} partitionId={}",
                        identity.queueId(),
                        identity.generationId(),
                        identity.partitionId(),
                        publicationFailure
                );
            }
        }
        log.warn(
                "event=runtime_partition_activation_failed queueId={} "
                        + "generationId={} partitionId={} nodeId={} "
                        + "registrationEpoch={} placementEpoch={}",
                identity.queueId(),
                identity.generationId(),
                identity.partitionId(),
                identity.nodeId(),
                identity.registrationEpoch(),
                identity.placementEpoch(),
                failure
        );
    }

    private boolean stillCurrent(RuntimePartitionIdentity identity) {
        return currentLiveRegistration()
                .filter(registration -> nodeId.equals(identity.nodeId()))
                .filter(registration -> registration.registrationEpoch()
                        == identity.registrationEpoch())
                .isPresent();
    }

    private Optional<NodeRegistration> currentLiveRegistration() {
        return registrations.currentRegistration()
                .filter(registration -> registration.leaseExpiresAt()
                        .isAfter(clock.instant()));
    }

    private void deactivate(PartitionKey key, String reason) {
        ActivePartition removed = active.remove(key);
        if (removed == null) {
            return;
        }
        closeQuietly(removed.queue());
        log.info(
                "event=runtime_partition_deactivated queueId={} "
                        + "generationId={} partitionId={} reason={}",
                removed.identity().queueId(),
                removed.identity().generationId(),
                removed.identity().partitionId(),
                reason
        );
    }

    private void deactivateAll(String reason) {
        List<PartitionKey> keys = List.copyOf(active.keySet());
        keys.forEach(key -> deactivate(key, reason));
    }

    private void closeQuietly(RuntimeQueue queue) {
        try {
            queue.close();
        } catch (RuntimeException failure) {
            log.warn("event=runtime_partition_close_failed", failure);
        }
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        deactivateAll("node shutdown");
        failures.clear();
    }

    private record DesiredPartition(
            PartitionPlacement placement,
            RuntimePartitionIdentity identity
    ) {
    }

    private record ActivePartition(
            RuntimePartitionIdentity identity,
            RuntimeQueue queue
    ) {
    }

    private record PartitionKey(
            java.util.UUID queueId,
            java.util.UUID generationId,
            int partitionId
    ) implements Comparable<PartitionKey> {
        static PartitionKey from(RuntimePartitionIdentity identity) {
            return new PartitionKey(
                    identity.queueId(),
                    identity.generationId(),
                    identity.partitionId()
            );
        }

        @Override
        public int compareTo(PartitionKey other) {
            int queueComparison = queueId.compareTo(other.queueId);
            if (queueComparison != 0) {
                return queueComparison;
            }
            int generationComparison = generationId.compareTo(
                    other.generationId()
            );
            return generationComparison != 0
                    ? generationComparison
                    : Integer.compare(partitionId, other.partitionId());
        }
    }
}
