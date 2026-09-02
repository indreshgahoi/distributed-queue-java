CREATE TABLE queue_partition_runtime_status (
    queue_id UUID NOT NULL,
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    registration_epoch BIGINT NOT NULL,
    placement_epoch BIGINT NOT NULL,
    runtime_state VARCHAR(32) NOT NULL,
    failure_reason VARCHAR(2048),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id),
    FOREIGN KEY (queue_id, generation_id, partition_id)
        REFERENCES queue_partition_placements (
            queue_id,
            generation_id,
            partition_id
        ),
    CONSTRAINT runtime_registration_epoch_positive
        CHECK (registration_epoch > 0),
    CONSTRAINT runtime_placement_epoch_positive
        CHECK (placement_epoch > 0),
    CONSTRAINT runtime_state_valid
        CHECK (runtime_state IN ('READY', 'FAILED')),
    CONSTRAINT runtime_failure_reason_consistent
        CHECK (
            (runtime_state = 'READY' AND failure_reason IS NULL)
            OR (runtime_state = 'FAILED' AND failure_reason IS NOT NULL)
        )
);

CREATE INDEX queue_partition_runtime_node_index
    ON queue_partition_runtime_status (node_id);
