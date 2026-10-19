# W1 — Streaming & Serving

W1 deliverables for plan section 5.3: *"Kafka, serving and UI design"*.
The folder layout matches the team repository (`configs/`, `docs/`, `schemas/`, `mock-dashboard/`), so the files can be copied into the repo as-is, e.g. on a `docs/streaming-design` branch.

## Required deliverables

| Deliverable | File |
|---|---|
| kafka-design.md | [docs/streaming/kafka-design.md](docs/streaming/kafka-design.md) |
| Schema draft | [docs/streaming/event-schema-draft.md](docs/streaming/event-schema-draft.md) (TransactionEvent v1, PredictionEvent v1, PaySim field mapping, test cases) + [schemas/](schemas/) (JSON Schema files and samples) |
| NoSQL access-pattern note | [docs/streaming/nosql-access-patterns.md](docs/streaming/nosql-access-patterns.md) |
| Mock dashboard | [mock-dashboard/index.html](mock-dashboard/index.html) (open in a browser, nothing to install) |
| Kafka host/container connectivity requirements | Section 7 of kafka-design.md + [configs/kafka/docker-compose.kafka.yml](configs/kafka/docker-compose.kafka.yml) |

## Supporting files (to get W2 started quickly)

| File | Purpose |
|---|---|
| [schemas/transaction_event.v1.schema.json](schemas/transaction_event.v1.schema.json) | JSON Schema for the input event; `additionalProperties: false` rejects label fields |
| [schemas/prediction_event.v1.schema.json](schemas/prediction_event.v1.schema.json) | JSON Schema for the prediction/alert event |
| [schemas/samples/](schemas/samples/) | Valid events (must PASS), invalid events with one error each (must FAIL), a prediction sample and a DLQ record sample; W2 test fixtures |
| [configs/kafka/docker-compose.kafka.yml](configs/kafka/docker-compose.kafka.yml) | Single-node Kafka (KRaft) with separate host and container listeners |
| [configs/kafka/create-topics.sh](configs/kafka/create-topics.sh) | Creates the topics defined in kafka-design.md |
| [configs/streaming.env.example](configs/streaming.env.example) | Environment variables for producer / streaming / API; no secrets |
| [configs/mongodb/init-indexes.js](configs/mongodb/init-indexes.js) | Draft MongoDB collections and indexes for W5, each index mapped to an access pattern |
| [mock-dashboard/screenshot.png](mock-dashboard/screenshot.png) | Screenshot of the mock dashboard |

## Checks performed

- All schema and sample JSON files parse correctly; in the invalid-sample set, only the intentionally truncated line fails to parse.
- The mock dashboard was rendered with headless Edge in light and dark mode, at desktop and narrow widths.

## Not done in W1 (by design)

- Kafka and MongoDB have not been run yet. The `docker-compose` file and scripts are drafts, to be verified in W2 (Kafka) and W5 (MongoDB).
- Sample events have not yet been validated with a JSON Schema library; this will be done with the W2 schema tests.
- PaySim figures (row count, fraud rate) come from the public dataset description and are **pending confirmation by the Batch & ML Lead's profiling**.
