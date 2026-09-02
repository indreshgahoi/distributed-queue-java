package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.in.ReconcileRuntimePartitionsUseCase;
import io.github.indreshgahoi.queue.node.application.port.out.NodeRegistrationProvider;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.exception.RuntimePartitionUnavailableException;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionView;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Owns the boundary between desired control-plane placement and locally
 * serviceable queue runtimes.
 *
 * <p>The manager monitor serializes reconciliation and lifecycle-map changes.
 * Each {@link RuntimePartitionHandle} separately orders data-plane admission
 * against closure for one partition. Queue operations therefore never hold
 * the manager monitor while performing storage I/O.
 *
 * <p>Recovery and closure may be slow. Authority is revalidated before a
 * recovered runtime becomes discoverable, and admitted operations drain before
 * its queue is closed.
 */
@Slf4j
public final class RuntimePartitionManager
        implements ReconcileRuntimePartitionsUseCase, AutoCloseable {
    private final String nodeId;
    private final Clock clock;
    private final NodeRegistrationProvider registrations;
    private final RuntimeTopologyClient topology;
    private final RuntimeQueueFactory queues;

    // Lifecycle truth. Read and written only while holding the manager monitor.
    private final Map<PartitionKey, RuntimePartitionHandle> active =
            new HashMap<>();

    // Request index. Reads are lock-free; lifecycle writers still hold the
    // manager monitor and expose only fully installed handles.
    private final ConcurrentMap<UUID, RuntimePartitionHandle> servingByQueueId =
            new ConcurrentHashMap<>();

    // Last activation failure for a desired partition that is not READY.
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

    /**
     * Reconciles one control-plane observation. Reconciliation is serialized,
     * but data-plane operations use partition handles and continue
     * independently.
     */
    @Override
    public synchronized void runOnce() {
        Optional<NodeRegistration> registration = currentLiveRegistration();
        if (registration.isEmpty()) {
            // A local runtime is useful only while its process incarnation is
            // authoritative. Fail closed instead of serving during ambiguity.
            deactivateAll("registration authority unavailable");
            failures.clear();
            return;
        }

        reconcile(registration.orElseThrow());
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

    /**
     * Executes through a permit owned by the addressed runtime partition.
     * Admission and closure of that partition are strictly ordered, but the
     * queue operation does not hold the manager monitor or another partition's
     * lifecycle lock while it performs storage I/O.
     */
    public <T> T withReadyQueue(
            UUID queueId,
            Function<RuntimeQueue, T> operation
    ) {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(operation, "operation");
        RuntimePartitionHandle handle = servingByQueueId.get(queueId);
        if (handle == null) {
            throw new RuntimePartitionUnavailableException(queueId);
        }

        RuntimePartitionHandle.Admission admission = handle.tryAcquire()
                .orElseThrow(
                        () -> new RuntimePartitionUnavailableException(queueId)
                );
        try (admission) {
            if (hasCurrentProcessAuthority(handle.identity())) {
                return operation.apply(admission.queue());
            }

            // A request may be the first observer of lease expiry. Close the
            // gate while this permit is still counted so no later request can
            // enter before lifecycle reconciliation catches up.
            handle.beginClosing();
        }
        deactivateIfCurrent(
                handle,
                "registration authority unavailable at request admission"
        );
        throw new RuntimePartitionUnavailableException(queueId);
    }

    private void reconcile(NodeRegistration registration) {
        Map<PartitionKey, DesiredPartition> desired = desiredPartitions(
                topology.activePlacements(registration),
                registration
        );
        deactivateSuperseded(desired);
        activateMissing(desired);
    }

    private void activateMissing(
            Map<PartitionKey, DesiredPartition> desired
    ) {
        desired.forEach((key, wanted) -> {
            RuntimePartitionHandle existing = active.get(key);
            if (existing == null
                    || !existing.identity().equals(wanted.identity())) {
                activate(key, wanted);
            }
        });
    }

    private Map<PartitionKey, DesiredPartition> desiredPartitions(
            List<PartitionPlacement> placements,
            NodeRegistration registration
    ) {
        Map<PartitionKey, DesiredPartition> desired = new HashMap<>();
        for (PartitionPlacement placement : placements) {
            RuntimePartitionIdentity identity = RuntimePartitionIdentity.from(
                    placement,
                    registration
            );
            desired.put(
                    PartitionKey.from(identity),
                    new DesiredPartition(placement, identity)
            );
        }
        return desired;
    }

    private void deactivateSuperseded(
            Map<PartitionKey, DesiredPartition> desired
    ) {
        List<PartitionKey> staleKeys = active.entrySet().stream()
                .filter(entry -> isSuperseded(entry, desired))
                .map(Map.Entry::getKey)
                .toList();
        closeRemovedPartitions(staleKeys, "authority superseded");
        failures.keySet().removeIf(key -> !desired.containsKey(key));
    }

    private boolean isSuperseded(
            Map.Entry<PartitionKey, RuntimePartitionHandle> activeEntry,
            Map<PartitionKey, DesiredPartition> desired
    ) {
        DesiredPartition wanted = desired.get(activeEntry.getKey());
        return wanted == null
                || !activeEntry.getValue().identity().equals(wanted.identity());
    }

    private void closeRemovedPartitions(
            List<PartitionKey> keys,
            String reason
    ) {
        // Close every gate before the first drain wait. One stuck operation
        // must not leave another stale runtime accepting new work.
        List<RuntimePartitionHandle> removed = keys.stream()
                .map(this::removeAndBeginClosing)
                .filter(Objects::nonNull)
                .toList();
        removed.forEach(handle -> closeAndLog(handle, reason));
    }

    private void activate(
            PartitionKey key,
            DesiredPartition desired
    ) {
        RuntimeQueue recoveredQueue = null;
        try {
            recoveredQueue = queues.open(desired.placement());
            if (!publishReadyWhileCurrent(desired.identity())) {
                closeQuietly(recoveredQueue);
                return;
            }

            install(
                    key,
                    new RuntimePartitionHandle(
                            desired.identity(),
                            recoveredQueue
                    )
            );
        } catch (RuntimeException failure) {
            if (recoveredQueue != null) {
                closeQuietly(recoveredQueue);
            }
            recordFailure(key, desired.identity(), failure);
            return;
        }

        failures.remove(key);
        logActivated(desired.identity());
    }

    private boolean publishReadyWhileCurrent(
            RuntimePartitionIdentity identity
    ) {
        // Recovery can outlive the authority that started it. Check before
        // publication, let PostgreSQL fence publication atomically, then check
        // again before making the runtime locally discoverable.
        if (!hasCurrentProcessAuthority(identity)) {
            return false;
        }
        topology.publishStatus(
                identity,
                RuntimePartitionState.READY,
                null
        );
        return hasCurrentProcessAuthority(identity);
    }

    private void install(
            PartitionKey key,
            RuntimePartitionHandle activated
    ) {
        UUID queueId = activated.identity().queueId();
        if (active.containsKey(key) || servingByQueueId.containsKey(queueId)) {
            activated.beginClosing();
            throw new IllegalStateException(
                    "multiple active runtimes for queue " + queueId
            );
        }

        // All writers hold the manager monitor. Publish to the concurrent
        // request index last, after the lifecycle map owns the complete handle.
        active.put(key, activated);
        servingByQueueId.put(queueId, activated);
    }

    private void logActivated(RuntimePartitionIdentity identity) {
        log.info(
                "event=runtime_partition_activated queueId={} "
                        + "generationId={} partitionId={} nodeId={} "
                        + "registrationEpoch={} placementEpoch={}",
                identity.queueId(),
                identity.generationId(),
                identity.partitionId(),
                identity.nodeId(),
                identity.registrationEpoch(),
                identity.placementEpoch()
        );
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
        if (hasCurrentProcessAuthority(identity)) {
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

    private boolean hasCurrentProcessAuthority(
            RuntimePartitionIdentity identity
    ) {
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

    private RuntimePartitionHandle removeAndBeginClosing(PartitionKey key) {
        RuntimePartitionHandle removed = active.remove(key);
        if (removed == null) {
            return null;
        }
        servingByQueueId.remove(
                removed.identity().queueId(),
                removed
        );
        removed.beginClosing();
        return removed;
    }

    private void closeAndLog(
            RuntimePartitionHandle removed,
            String reason
    ) {
        closeQuietly(removed);
        log.info(
                "event=runtime_partition_deactivated queueId={} "
                        + "generationId={} partitionId={} reason={}",
                removed.identity().queueId(),
                removed.identity().generationId(),
                removed.identity().partitionId(),
                reason
        );
    }

    private synchronized void deactivateIfCurrent(
            RuntimePartitionHandle handle,
            String reason
    ) {
        PartitionKey key = PartitionKey.from(handle.identity());
        if (active.get(key) != handle) {
            closeQuietly(handle);
            return;
        }
        RuntimePartitionHandle removed = removeAndBeginClosing(key);
        closeAndLog(removed, reason);
    }

    private void deactivateAll(String reason) {
        closeRemovedPartitions(List.copyOf(active.keySet()), reason);
    }

    private void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception failure) {
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
