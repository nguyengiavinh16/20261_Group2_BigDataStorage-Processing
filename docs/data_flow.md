# Data Flow and Event Contract

## Planned transaction event
The event contract will be finalized after inspecting the actual source schema.
At minimum, each event needs a stable `transaction_id`, a source event-time value,
and the feature fields needed by the selected model. Producer and consumer must
agree on field names, types, nullability, and serialization.

## Time semantics
Keep:
- event time: timestamp or documented conversion of source simulation time;
- ingestion time: when the event enters the pipeline;
- processing time: when Spark processes it.

If PaySim only provides a relative `step`, do not invent a real-world transaction
timestamp. Document the conversion and its limitations.

## Duplicate handling
Use a stable event ID and deterministic prediction/alert identifiers. Database
writes should replace/upsert the same logical record instead of creating a new
record on each retry. Kafka delivery, Spark checkpointing, and database idempotency
are separate mechanisms.
