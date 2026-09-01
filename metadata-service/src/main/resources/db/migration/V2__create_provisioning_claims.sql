CREATE TABLE queue_provisioning_claims (
    queue_id UUID PRIMARY KEY REFERENCES queues (queue_id),
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    worker_id VARCHAR(255) NOT NULL,
    fencing_token BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT provisioning_partition_zero CHECK (partition_id = 0),
    CONSTRAINT provisioning_fencing_token_positive CHECK (fencing_token > 0)
);

CREATE INDEX queue_provisioning_claim_expiry_index
    ON queue_provisioning_claims (lease_expires_at);
