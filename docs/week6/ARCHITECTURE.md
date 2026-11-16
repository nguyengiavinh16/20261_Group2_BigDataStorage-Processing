# Kiến trúc release phạm vi được hỗ trợ

```text
PaySim CSV (ngoài Git; SHA + physical line)              Offline labels (sidecar, offline only)
        |                                                           |
        v                                                           |
  Immutable snapshot/parts ----------------------------------------+ (train/eval only)
        |
        v
  Spark + Iceberg Bronze (occurrences)
        |
        v
  Spark + Iceberg Silver (unique canonical transaction IDs)
        |                                        \
        v                                         \→ Gold(destination × step, pinned cutoff)
  A / A+H offline features                            
        |
        v
  Spark ML Logistic Regression [frozen artifact + threshold]
        |
Kafka replay producer (schema v2; label-free; ordered by destination)
        |
        +-- archive Spark query --> Iceberg (own checkpoint)
        |
        +-- scoring query (A or stateful A+H)
                  |
                  v
           MongoDB idempotent sink `(run_id, transaction_id)`
                  |
                  v
            FastAPI --> Dashboard
                  |
       monitoring / validators / gate G3 G4 G5
```

- HDFS/Iceberg stores snapshot/checkpoint and artifacts; Kafka RF1 and one-host cluster are not HA guarantees. Mongo unique key gives idempotent upsert scope, not atomic multi-sink exactly-once.
- Batch/stream parity explicitly depends on same window, ordered routing, cutoff and versions. Compaction must preserve logical IDs.
- **Optional only:** Redis and Airflow were deferred in plan unless G4 core PASS; **no claim implemented** in these patches.
- All diagrams are **documentation**; actual runtime proof needs service configs, executor logs, manifest, checksums and successful validation on user's environment.
