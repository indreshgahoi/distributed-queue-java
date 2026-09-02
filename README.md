# Distributed Queue in Java

A distributed message queue built incrementally from first
principles to explore delivery semantics, ownership, leases,
concurrency, durability, crash recovery, and replication.

## Why This Project

The goal is not to build another production message broker.

The goal is to understand which mechanisms are required
to provide specific queue guarantees and how those mechanisms
behave under failure.

## Current Scope

Latest release: v0.25.0 — stable data-plane routing gateway.

Current development: v0.26.0 — ordered follower WAL protocol. A follower now
accepts lineage-bound WAL entries in logical sequence order, durably fences
stale leader epochs, and handles exact retries without duplicating records.

This is a replication storage primitive, not replicated queue availability.
Transport, leader election, quorum commit, follower promotion, catch-up, and
multi-partition routing are not yet included.

The Maven build has five modules and three independently deployable services:

```text
queue-core        durable local queue engine and storage state machine
metadata-service Spring Boot control-plane service and PostgreSQL repository
queue-node        Spring Boot reconciliation worker and local storage adapter
queue-gateway     Spring Boot stable customer routing and forwarding service
queue-benchmarks  JMH microbenchmarks and an executable benchmark artifact
```

The metadata service keeps its domain authority independent of delivery and
storage frameworks through explicit ports-and-adapters boundaries:

```text
metadata
├── domain
│   ├── model
│   └── exception
├── application
│   ├── port.in
│   ├── port.out
│   └── service
├── adapter
│   ├── in.web
│   └── out.postgres
└── MetadataServiceApplication   Spring Boot composition root
```

Domain types and ports are deliberate public contracts. Controllers,
application-service implementations, persistence implementations, and their
constructors remain package-private so callers cannot bypass those contracts.

The gateway follows the same dependency direction while keeping transport
forwarding outside its routing policy:

```text
gateway
├── domain
│   ├── model
│   └── exception
├── application
│   ├── port.in
│   ├── port.out
│   └── service
├── adapter
│   ├── in.web
│   └── out.http
└── QueueGatewayApplication     Spring Boot composition root
```

## Design Principles

- Define guarantees before implementation.
- Derive mechanisms from concrete failure scenarios.
- Prefer explicit state machines.
- Test failure behavior, not only happy paths.
- Add complexity only when a requirement justifies it.

Repository-wide conventions for architecture, Java formatting, naming,
testing, logging, durability, API evolution, and review are defined in the
[engineering guidelines](docs/engineering-guidelines.md).

The v0.21 authority decision and end-to-end lifecycle are captured in
[ADR 0020](docs/adr/0020-fenced-runtime-partition-activation.md) and the
[runtime partition lifecycle](docs/diagrams/runtime-partition-lifecycle.md).
The v0.22 request/closure ordering decision is captured in
[ADR 0021](docs/adr/0021-authority-guarded-node-local-data-plane.md) and the
[data-plane operation lifecycle](docs/diagrams/data-plane-operation-lifecycle.md).
The v0.23 lock-scope decision is captured in
[ADR 0022](docs/adr/0022-per-partition-runtime-admission.md) and the
[per-partition runtime lifecycle](docs/diagrams/per-partition-runtime-lifecycle.md).
The v0.24 admission boundary is captured in
[ADR 0023](docs/adr/0023-bounded-retained-message-admission.md) and the
[bounded admission lifecycle](docs/diagrams/bounded-admission-lifecycle.md).
The v0.25 routing decision is captured in
[ADR 0024](docs/adr/0024-stable-data-plane-routing-gateway.md), the
[routing sequence](docs/diagrams/stable-routing-sequence.md), and the
[routing decision flow](docs/diagrams/stable-routing-flow.md).

## Roadmap

The roadmap is organized around correctness problems, not feature parity with
existing brokers. Each milestone must identify a concrete limitation, define
its invariants and failure semantics, and preserve a coherent path toward a
distributed system.

### Completed foundations

1. **Queue state machine** — FIFO publication, explicit acknowledgement,
   receipt-handle ownership, finite leases, redelivery, bounded retries,
   delayed NACK, and dead-letter transitions.
2. **Single-process concurrency** — coarse-grained locking establishes an
   understandable linearization point for compound queue transitions.
3. **Durable transitions** — WAL-first mutation ordering, failed-writer
   poisoning, framed records, versioned headers, CRC32C integrity, and
   recoverable active-tail truncation.
4. **Durable delivery ownership** — active leases, attempts, receipt handles,
   expiry, ACK, NACK, and DLQ decisions survive restart.
5. **Bounded recovery history** — snapshots, `WalPosition`, snapshot plus WAL
   suffix recovery, crash-safe snapshot replacement, segmented WAL rotation,
   and snapshot-authorized whole-segment reclamation.
6. **Compaction-aware recovery** — startup fails closed when reclaimed history
   makes the authoritative snapshot mandatory, and snapshot authority remains
   monotonic across restart.
7. **Durable filesystem authority** — snapshot/WAL publication and segment
   deletion include parent-directory durability boundaries.
8. **Automatic bounded storage** — durable WAL progress triggers serialized
   checkpoint, promotion, and reclamation cycles with observable retries.
9. **Durable storage lineage** — WAL segments and snapshots are bound to one
   queue, generation, and partition identity.
10. **Shared metadata authority** — PostgreSQL owns tenant-scoped queue
    identity, lifecycle, retry-safe creation, and optimistic-concurrency
    fencing.
11. **Lease-fenced provisioning** — queue nodes materialize lineage-bound
    storage through durable claims and fenced lifecycle publication.
12. **Fenced runtime activation** — queue nodes recover only authoritative
    `ACTIVE` placements, publish readiness through PostgreSQL fencing, and
    close runtimes when process authority is lost.
13. **Authority-guarded data plane and baseline** — HTTP queue operations enter
    only through READY runtime authority, use constant-time lookup, and have a
    checked-in JMH baseline separating queue and forced-WAL costs.

14. **Partition-scoped lifecycle concurrency** — per-partition admission
    permits order operations against draining and closure without holding a
    node-wide lock during storage I/O.
15. **Bounded retained-message admission** — message and retained-state limits
    reject overload before WAL mutation and survive recovery.
16. **Stable data-plane routing** — a gateway resolves current READY authority,
    forwards once, and preserves ambiguous mutation outcomes.

### Current milestone

**v0.26.0 — Ordered follower WAL protocol**

Establish the first distributed-storage invariant: one lineage-bound follower
accepts only the next logical WAL sequence from a non-stale leader epoch. Exact
retries are idempotent, conflicts and gaps fail closed, and the highest observed
epoch survives restart.

### Next decision area

After v0.26.0, the repository will be reviewed again before selecting a
milestone. Likely candidates are:

- **Replica transport and catch-up** — stream bounded entry batches and resume
  from follower progress without changing ordering authority.
- **Replication-aware checkpoints** — retain logical sequence identity when
  local snapshots authorize WAL reclamation.
- **Leader-side commit tracking** — distinguish local append from replication
  and quorum commit before acknowledging durable publication.

Selection will be based on correctness value, architectural dependency,
failure exposure, operational need, and distributed-systems learning value.

### Deliberately later

Leader election, safe promotion, ownership transfer, partitioning, and quorum
durability remain later phases. v0.26 does not call a follower copy committed or
allow it to serve traffic.

## Metadata Service

### Run locally with Docker Compose

#### 1. Prerequisites

- Docker Engine or Docker Desktop;
- Docker Compose v2 (`docker compose version`);
- ports `5432`, `8080`, `8081`, and `8082` available locally.

All commands below run from the repository root.

#### 2. Start PostgreSQL and the service

Build the Java 21 service images and start all containers in the background:

```bash
docker compose up --detach --build
```

Compose waits for PostgreSQL to become healthy before starting the metadata
service. Flyway creates or upgrades the schema, and the queue node continuously
reconciles newly created queues.

Follow startup logs with:

```bash
docker compose logs --follow metadata-service
```

The service is ready after the log contains
`Started MetadataServiceApplication`.

#### 3. Verify readiness

Check container state:

```bash
docker compose ps
```

`postgres` should report `healthy`; `metadata-service`, `queue-node`, and
`queue-gateway` should report `Up`.
Then verify the application health endpoint:

```bash
curl --fail http://localhost:8080/actuator/health
```

The response should contain `"status":"UP"`.

#### 4. Test through Swagger UI

Open the interactive API at:

```text
http://localhost:8080/swagger-ui.html
```

The raw OpenAPI document is available at:

```text
http://localhost:8080/v3/api-docs
```

Swagger UI contains example tenant IDs, queue names, idempotency keys, request
bodies, and responses. Select an operation, choose **Try it out**, keep or edit
the example values, and execute the request against the local service.

Recommended first request:

1. Select `POST /api/v1/tenants/{tenantId}/queues`.
2. Choose **Try it out**.
3. Use tenant ID `acme`.
4. Use idempotency key `create-orders-001`.
5. Keep the example body `{"queueName":"orders"}`.
6. Execute the request and expect `201 Created`.

The create response may initially report `PROVISIONING`. Poll the GET operation:
the queue node should create its lineage-bound WAL and promote the descriptor to
`ACTIVE`. Provisioning is asynchronous, so clients must not treat the create
response as storage readiness.

#### 5. Test from the command line

Create a queue:

```bash
curl --request POST \
  http://localhost:8080/api/v1/tenants/acme/queues \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: create-orders-001' \
  --data '{"queueName":"orders"}'
```

Read it back with:

```bash
curl http://localhost:8080/api/v1/tenants/acme/queues/orders
```

List all queues in the tenant:

```bash
curl http://localhost:8080/api/v1/tenants/acme/queues
```

Repeat the create request with the same idempotency key and body to verify
retry safety: it returns the same queue identity rather than creating another
queue.

#### 6. Stop or reset the environment

Stop containers while preserving PostgreSQL data:

```bash
docker compose down
```

To also remove PostgreSQL metadata and queue-node storage and start empty:

```bash
docker compose down --volumes
```

#### Troubleshooting: `UnknownHostException: postgres`

If startup fails with `UnknownHostException: postgres`, the containers are no
longer attached to a healthy Compose network. Recreate the containers and
network without deleting the PostgreSQL volume:

```bash
docker compose down
docker compose up --detach --build
docker compose ps
```

Both services should report `Up`, and PostgreSQL should report `healthy`. Do
not add `--volumes` when recovering the network unless discarding local
metadata is intentional.

If `docker compose` is unavailable but `docker-compose version` succeeds, use
`docker-compose` in place of `docker compose` in the commands above.

### Run the service from Maven

To run Java on the host while PostgreSQL remains in Docker, start only the
database:

```bash
docker compose up --detach postgres
mvn -pl metadata-service -am install -DskipTests
mvn -pl metadata-service spring-boot:run
```

The metadata service reads these environment variables when their defaults are
not suitable:

```text
METADATA_DATABASE_URL=jdbc:postgresql://localhost:5432/queue_metadata
METADATA_DATABASE_USER=queue
METADATA_DATABASE_PASSWORD=queue
METADATA_HTTP_PORT=8080
```

The queue node reads:

```text
QUEUE_NODE_ID=local-node-1
QUEUE_NODE_ENDPOINT=http://localhost:8081
METADATA_SERVICE_URL=http://localhost:8080
QUEUE_STORAGE_ROOT=./queue-data
NODE_REGISTRATION_LEASE_DURATION=PT30S
NODE_HEARTBEAT_DELAY=PT10S
PROVISIONING_LEASE_DURATION=PT30S
PROVISIONING_POLL_DELAY=PT1S
RUNTIME_PARTITION_POLL_DELAY=PT1S
QUEUE_NODE_PORT=8081
QUEUE_WAL_SEGMENT_BYTES=16777216
QUEUE_MAX_MESSAGE_BYTES=262144
QUEUE_MAX_RETAINED_MESSAGES=100000
QUEUE_MAX_RETAINED_BYTES=1073741824
QUEUE_GATEWAY_PORT=8082
GATEWAY_CONNECT_TIMEOUT=PT2S
GATEWAY_REQUEST_TIMEOUT=PT10S
```

Flyway applies versioned schema migrations before the service becomes ready.
The REST resources are:

```text
POST   /api/v1/tenants/{tenantId}/queues
GET    /api/v1/tenants/{tenantId}/queues/{queueName}
GET    /api/v1/tenants/{tenantId}/queues
DELETE /api/v1/tenants/{tenantId}/queues/{queueName}
GET    /actuator/health
```

The queue node uses the trusted, non-customer provisioning endpoints under
`/internal/v1/provisioning`. They are intentionally unauthenticated for this
local learning deployment and must not be exposed outside a trusted network.
Node registration, heartbeat, and topology inspection use `/internal/v1/nodes`
and `/internal/v1/placements` under the same trust assumption. Runtime
activation uses the fenced node-specific placement and status endpoints; local
runtime inspection is available from the queue node:

```text
GET  /internal/v1/nodes/{nodeId}/runtime-placements?registrationEpoch={epoch}
POST /internal/v1/partitions/{queueId}/{generationId}/{partitionId}/runtime-status
GET  /internal/v1/runtime/partitions                metadata observation
GET  http://localhost:8081/internal/v1/runtime/partitions
                                                     node-local observation
```

Customer message operations use the stable queue gateway at
`http://localhost:8082`. Gateway Swagger UI is available at
`http://localhost:8082/swagger-ui.html`; queue-node Swagger remains available
at `http://localhost:8081/swagger-ui.html` for trusted internal diagnosis.

```text
POST /v1/queues/{queueId}/messages
POST /v1/queues/{queueId}/messages/receive
POST /v1/queues/{queueId}/messages/{receiptHandle}/ack
POST /v1/queues/{queueId}/messages/{receiptHandle}/nack
```

Publish accepts `{"payload":"process-order-123"}`. NACK accepts an ISO-8601
duration such as `{"retryDelay":"PT30S"}`. An empty receive returns `204 No
Content`; a queue without a currently READY route returns `503 Service
Unavailable`. Oversized payloads return `413 Payload Too Large`; exhausted
retained count or byte capacity returns `429 Too Many Requests`. Both publish
rejections occur before WAL append. The gateway makes one metadata lookup and
one node call per operation; it never automatically retries an ambiguous
mutation.

See the [provisioning sequence](docs/diagrams/provisioning-claim-sequence.md)
and [decision flow](docs/diagrams/provisioning-claim-flow.md) for the complete
lease and fencing protocol. See the
[runtime lifecycle](docs/diagrams/runtime-partition-lifecycle.md) for recovery,
readiness, failure, and deactivation transitions.

`POST` requires an `Idempotency-Key` header and a JSON body such as
`{"queueName":"orders"}`. It returns `201 Created`, a resource `Location`,
and normally a `PROVISIONING` descriptor. Validation and domain failures use
standard `application/problem+json` responses. The queue node later transitions
the descriptor to `ACTIVE` through a lease-fenced internal API.
PostgreSQL integration tests use an ephemeral Testcontainers database and are
skipped when Docker is unavailable.
