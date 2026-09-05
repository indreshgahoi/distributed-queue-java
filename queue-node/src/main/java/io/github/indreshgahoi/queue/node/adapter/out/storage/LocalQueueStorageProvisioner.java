package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Component
final class LocalQueueStorageProvisioner
        implements QueueStorageProvisioner {
    private final Path storageRoot;
    private final long walSegmentBytes;

    LocalQueueStorageProvisioner(
            QueueNodeProperties properties
    ) {
        storageRoot = properties.storageRoot();
        walSegmentBytes = properties.walSegmentBytes();
    }

    @Override
    public void provision(ProvisioningAssignment assignment) {
        LocalPartitionStorageLayout storage =
                LocalPartitionStorageLayout.resolve(
                        storageRoot,
                        assignment.lineage()
                );
        try (SegmentedFileWriteAheadLog ignored =
                     new SegmentedFileWriteAheadLog(
                             storage.walDirectory(),
                             walSegmentBytes,
                             assignment.lineage()
                     )) {
            // Opening and closing durably establishes or validates lineage.
        }
    }
}
