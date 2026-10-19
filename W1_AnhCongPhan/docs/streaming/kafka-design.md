# Kafka design — v1 (DRAFT W1)

| Item | Value |
|---|---|
| Owner | Streaming & Serving Lead |
| Reviewers | Lead (interfaces), Batch & ML Lead (cross-review) |
| Status | Draft, pending Lead sign-off at the end-of-W1 meeting |
| Related | [event-schema-draft.md](event-schema-draft.md), [nosql-access-patterns.md](nosql-access-patterns.md), `configs/kafka/*` |

## 1. Kafka's role in the architecture

```
PaySim CSV ──► Spark ETL (batch) ──► Processed Parquet (with transaction_id, event_time)
                                        │
                                        ▼
                         producer.py (replay, W2) ──► [fraud.transactions.v1]
                                                              │
                                                              ▼
                                          Spark Structured Streaming (W4)
                                          ├─ parse + validate ──► invalid ──► [fraud.transactions.dlq.v1]
                                          └─ score model ──► MongoDB predictions / alerts (W5)
                                                         └─(optional)─► [fraud.predictions.v1]
                                                                            │
                                                  API / dashboard  ◄────────┘ (reads MongoDB)
```

Kafka carries **only the data used for scoring**. The ground-truth label (`isFraud`) **never** travels through the input topic. Model evaluation uses a separate path: predictions in MongoDB are joined with the labels in Parquet on `transaction_id`.

## 2. Naming convention

```
<domain>.<entity>[.<qualifier>].v<major>
```

- Lowercase, dot-separated, no spaces, no environment name in the topic (dev and prod are separated by cluster).
- `v<major>` is bumped only for **breaking changes**: renaming or removing a field, changing a data type, or changing what the key means. Adding an optional field keeps the existing topic and bumps `schema_version` to `1.x`.
- Consumer groups: `<app>.<purpose>`, e.g. `fraud-consumer-test.validate`. Structured Streaming does not use the group's committed offsets. Spark stores offsets in its **checkpoint**, so to re-read from the beginning, change or delete the checkpoint rather than resetting the group.

## 3. Topics

| Topic | Required? | Key | Value | Partitions (dev) | Retention | Producer → Consumer |
|---|---|---|---|---|---|---|
| `fraud.transactions.v1` | Yes | `source_account` | TransactionEvent v1 (JSON) | 3 | 7 days | producer.py → streaming app, consumer_test.py |
| `fraud.transactions.dlq.v1` | Yes | original key (may be null) | DLQ envelope (JSON) | 1 | 14 days | streaming app → investigation / tests |
| `fraud.predictions.v1` | Optional | `transaction_id` | PredictionEvent v1 (JSON) | 3 | 7 days | streaming app → secondary consumers (decoupled sink, benchmark) |

Replication factor = 1 because the dev environment has a single broker. On a multi-broker cluster (W7), use RF = 3 and `min.insync.replicas = 2`. `auto.create.topics.enable = false` prevents accidentally creating a topic through a typo.

## 4. Message key and partitioning

**Proposal: key = `source_account` (`nameOrig`).**

| Criterion | key = `source_account` (chosen) | key = `transaction_id` | key = null (round-robin) |
|---|---|---|---|
| Per-account ordering | ✅ all transactions of an account land on the same partition | ❌ | ❌ |
| Supports stateful per-account features (W5–W6) | ✅ easy to extend | ❌ | ❌ |
| Load distribution | ✅ PaySim has millions of distinct `nameOrig` values, so skew is minimal | ✅ | ✅ |
| Dedupe by key in Kafka | not needed, the sink dedupes on `transaction_id` | possible | no |

- **3 partitions:** enough for Spark to run 3 tasks in parallel on a personal machine. The partition count can only be increased, never decreased, and increasing it changes the key → partition mapping. So it should be fixed early, and not increased before benchmark results are available (W6).
- **Note:** Kafka guarantees ordering only **within a partition**. Ordering across accounts is not guaranteed, so streaming must rely on `event_time` and watermarks, not on arrival order.

## 5. Message format

- **Value:** UTF-8 JSON conforming to [transaction_event.v1.schema.json](../../schemas/transaction_event.v1.schema.json). Avro and Schema Registry are not used in the MVP, to reduce the number of services to deploy. They are recorded as a future upgrade.
- **Key:** UTF-8 string.
- **Headers:** `schema_version=1.0`, `content_type=application/json`, `producer=paysim-replay`, `replay_id=<id>`.
- **Size:** roughly 300–400 bytes per message, far below the 1 MB default limit.

## 6. Proposed producer configuration (W2)

| Property | Value | Reason |
|---|---|---|
| `acks` | `all` | No message loss when the broker restarts |
| `enable.idempotence` | `true` | Producer retries do not create duplicates **within Kafka** (not end-to-end) |
| `retries` / `delivery.timeout.ms` | default / 120000 | Tolerates short broker restarts |
| `linger.ms` | 10–20 | Batches messages to raise throughput while keeping latency low |
| `compression.type` | `lz4` (if the client library supports it), otherwise `none` | Less network and disk usage |
| Bootstrap | read from env `KAFKA_BOOTSTRAP_SERVERS` | No hard-coded broker address |

**Replay parameters** (`configs/streaming.env.example`): `REPLAY_RATE_EPS`, `REPLAY_MAX_EVENTS`, `REPLAY_LOOP`, `REPLAY_INPUT_PATH`. The producer emits events in ascending `event_time` order. A "shuffle / delay" mode for late-event testing (W6) will be added later as a parameter, without modifying the source data.

## 7. Connecting to Kafka from the host and from containers (avoiding wrong broker addresses)

A client first connects to the **bootstrap server**. The broker then returns its list of **advertised listeners**, and the client connects directly to the address it was given. If the broker returns `localhost:9092` to a client running in another container, that client calls its own `localhost` and hangs or times out. This is the most common mistake when running Kafka in Docker.

The broker therefore exposes **two listeners** (see [docker-compose.kafka.yml](../../configs/kafka/docker-compose.kafka.yml)):

| Where the client runs | `KAFKA_BOOTSTRAP_SERVERS` | Listener |
|---|---|---|
| Windows host (producer.py, Spark local mode, consumer_test.py) | `localhost:9092` | EXTERNAL |
| WSL2 (Docker Desktop, default networking) | `localhost:9092`; if that fails, use the Windows host IP | EXTERNAL |
| Container on the `fraud-net` network (Spark container, API) | `kafka:29092` | INTERNAL |
| Kubernetes (W7) | `<kafka-service>.<namespace>.svc.cluster.local:9092` | reconfigured per chart |

Rules:
1. Code **never** hard-codes the broker address; it always reads it from env or config.
2. Verify connectivity before debugging code: `kafka-broker-api-versions.sh --bootstrap-server <addr>` or `kafka-topics.sh --list`.
3. Spark needs the package `org.apache.spark:spark-sql-kafka-0-10_2.12:<matching Spark version>`. A version mismatch causes `ClassNotFound` or `NoSuchMethod` errors.
4. No credentials in code. Dev uses PLAINTEXT. If SASL is enabled later, configure it through env or secrets.

## 8. Commands to create and check topics (to be run in W2 with evidence recorded)

```bash
docker compose -f configs/kafka/docker-compose.kafka.yml up -d
docker cp configs/kafka/create-topics.sh kafka:/tmp/create-topics.sh
docker exec kafka bash /tmp/create-topics.sh
# Send/read test messages with the console tools
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic fraud.transactions.v1 --property parse.key=true --property key.separator='|'
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic fraud.transactions.v1 --from-beginning --property print.key=true
```

## 9. Dead-letter queue (DLQ)

Messages that fail to parse or violate the schema are **never dropped silently**. The streaming app writes them to `fraud.transactions.dlq.v1` with an envelope (see [dlq_event.sample.json](../../schemas/samples/dlq_event.sample.json)):
`error_type` (`PARSE_ERROR`, `SCHEMA_VIOLATION`, `FORBIDDEN_FIELD`), `error_message`, `source_topic/partition/offset`, `raw_value`, `detected_at`. The number of invalid messages per minute is counted and shown on the dashboard.

## 10. Guarantees — what is **not** promised

- The idempotent producer only prevents duplicates **within Kafka** for retries within one producer session. Replaying the whole file again intentionally produces duplicate messages.
- Spark + checkpoint guarantees each offset is processed **at least once** across restarts. Duplicates at the sink are eliminated by **upserting on `transaction_id`** (see the NoSQL note). The team **does not claim end-to-end exactly-once** until it has been tested.

## 11. Unverified assumptions

| # | Assumption | Verified by | When |
|---|---|---|---|
| A1 | The batch ETL generates `transaction_id` and `event_time` following the rules in event-schema-draft | Batch & ML Lead + Lead | W1–W2 |
| A2 | The team runs Kafka with Docker (image `apache/kafka:3.8.0`, KRaft) | Lead | W1 |
| A3 | Replay throughput is a few hundred to a few thousand events/s, so 3 partitions are enough | Streaming & Serving Lead | W6 benchmark |
| A4 | JSON without Schema Registry is sufficient for the MVP | Lead | W1 |
