CREATE TABLE queue_nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    endpoint VARCHAR(2048) NOT NULL,
    registration_epoch BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT node_registration_epoch_positive
        CHECK (registration_epoch > 0)
);

CREATE INDEX queue_node_lease_expiry_index
    ON queue_nodes (lease_expires_at);

CREATE TABLE queue_partition_placements (
    queue_id UUID NOT NULL REFERENCES queues (queue_id),
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    node_id VARCHAR(255) NOT NULL REFERENCES queue_nodes (node_id),
    placement_epoch BIGINT NOT NULL,
    metadata_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id),
    CONSTRAINT placement_partition_zero CHECK (partition_id = 0),
    CONSTRAINT placement_epoch_positive CHECK (placement_epoch > 0),
    CONSTRAINT placement_metadata_version_non_negative
        CHECK (metadata_version >= 0)
);

CREATE INDEX queue_partition_placement_node_index
    ON queue_partition_placements (node_id);

ALTER TABLE queue_provisioning_claims
    ADD COLUMN registration_epoch BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN placement_epoch BIGINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT provisioning_registration_epoch_positive
        CHECK (registration_epoch > 0),
    ADD CONSTRAINT provisioning_placement_epoch_positive
        CHECK (placement_epoch > 0);

