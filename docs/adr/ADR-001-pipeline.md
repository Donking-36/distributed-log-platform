# ADR-001: Core log pipeline and component boundaries

- Status: Accepted
- Date: 2026-07-30

## Context

The source requirement sends Kubernetes logs through Filebeat and Kafka to
Elasticsearch, but it does not identify who consumes Kafka. It also allows an
optional Go query layer for Grafana. Leaving both gaps open would either break
the data path or add duplicate services.

The local project must remain small, teach the purpose of Kafka and include a
meaningful Go service without adopting production-scale infrastructure.

## Decision

Use this fixed `v0.1.0` flow:

```text
Pod stdout
  → node container log file
  → Filebeat DaemonSet
  → Kafka logs.<service>
  → Go log-processor
  → Elasticsearch logs-stage3-*
  → Grafana
```

Component boundaries:

1. `demo-app` only emits predictable JSON to stdout. It never writes Kafka or
   Elasticsearch directly.
2. Filebeat collects only the target namespace/workloads, excludes its own and
   infrastructure logs, enriches Kubernetes metadata and routes by an
   allowlisted Pod `service` label. It performs no business aggregation and
   never writes Elasticsearch.
3. Kafka buffers and decouples collection from processing. It is not long-term
   search storage.
4. Go `log-processor` is the only Kafka-to-Elasticsearch business path. It
   validates, normalizes, derives the event ID and writes Elasticsearch.
5. Elasticsearch owns indexing and aggregation. It is not a message queue.
6. Grafana queries Elasticsearch directly and does not store primary log data.
   A Go query API requires a later, accepted requirement.

For the initial acceptance dataset:

- the same demo image is deployed as `demo-api` and `demo-worker`;
- topics are `logs.demo-api` and `logs.demo-worker`;
- each business topic has three partitions and replication factor one;
- stable Pod UID is the partition key;
- unknown or missing service labels go only to fixed
  `logs.unclassified`, never to dynamically constructed topics.

Three partitions per service permit a later consumer-scaling experiment without
changing the initial topic contract. Replication factor one is explicitly a
single-node development choice, not a high-availability claim.

`log-processor` subscribes only to the explicit business-topic allowlist; it
does not consume `logs.unclassified` or `logs.dlq`. The fallback topic's message
count and sampled records are the observable rejection evidence for invalid
routing labels.

## Consequences

### Positive

- The required pipeline is closed end to end.
- Kafka has a clear buffering and replay role.
- The Go service owns meaningful distributed-system behavior.
- Grafana provisioning stays simple and avoids a duplicate query model.
- Controlled topic names prevent label values from creating unbounded topics.

### Negative

- The processor must handle Kafka offsets, retries, poison messages and
  Elasticsearch partial failures.
- Grafana is coupled to the Elasticsearch mapping in `v0.1.0`.
- Single-node Kafka and Elasticsearch cannot demonstrate node-level high
  availability.
- A fixed fallback topic requires explicit monitoring and cleanup.

## Rejected alternatives

- **Filebeat → Elasticsearch directly:** rejects Kafka and bypasses the required
  Go processing path.
- **`demo-app` writes Kafka or Elasticsearch directly:** couples the producer to
  infrastructure and bypasses node-level collection evidence.
- **Add Logstash:** duplicates the processor role and weakens the Go learning
  objective.
- **Go query API in `v0.1.0`:** duplicates Grafana/Elasticsearch querying
  without a current authorization or isolation requirement.
- **Topic name from arbitrary label text:** risks topic explosion and makes
  routing acceptance non-deterministic.
- **One topic for every log:** removes the required per-service routing
  evidence.
- **Elasticsearch as a queue or Kafka as long-term search:** assigns durability
  and query responsibilities to the wrong component.
- **Production multi-node Kafka/Elasticsearch now:** exceeds the local learning
  scope; it is not implied by the single-node recovery tests.

## Validation obligations

- `demo-api` and `demo-worker` each emit 20 numbered events with one unique
  `test_run_id`.
- A temporary Kafka consumer observes all 20 expected unique sequence numbers
  in the correct service topic, with no cross-routing or Filebeat recursion.
- A missing or unknown service label reaches only `logs.unclassified`; it
  creates no arbitrary topic and is not consumed by the normal processor.
- No manual write to Elasticsearch is needed for demo events to arrive.
- For one fixed `test_run_id`, N logical unique inputs produce N unique
  Elasticsearch documents and are queryable through the provisioned Grafana
  path within the declared test boundary.
- Grafana views use the same Elasticsearch source and can be restored from
  repository provisioning files.
