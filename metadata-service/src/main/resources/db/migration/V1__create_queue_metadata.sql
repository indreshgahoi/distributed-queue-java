CREATE TABLE IF NOT EXISTS queues (
    queue_id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    generation_id UUID NOT NULL,
    lifecycle_state VARCHAR(32) NOT NULL,
    partition_count INTEGER NOT NULL,
    metadata_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT queue_partition_count_positive CHECK (partition_count > 0),
    CONSTRAINT queue_lifecycle_state_valid CHECK (
        lifecycle_state IN (
            'PROVISIONING',
            'ACTIVE',
            'PROVISIONING_FAILED',
            'DELETING',
            'DELETE_FAILED',
            'DELETED'
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS queues_live_tenant_name_unique
    ON queues (tenant_id, queue_name)
    WHERE lifecycle_state <> 'DELETED';

CREATE INDEX IF NOT EXISTS queues_tenant_list_index
    ON queues (tenant_id, queue_name)
    WHERE lifecycle_state <> 'DELETED';

CREATE TABLE IF NOT EXISTS metadata_requests (
    tenant_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    operation_type VARCHAR(64) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    response_queue_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key),
    CONSTRAINT metadata_request_queue_fk
        FOREIGN KEY (response_queue_id) REFERENCES queues (queue_id)
);
