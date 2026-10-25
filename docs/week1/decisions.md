# Decisions log

| ID | Question | Proposal / rule | Owner | Due | Status |
|---|---|---|---|---|---|
| D-01 | Actual dataset and columns | Start with PaySim assumption; inspect real header, source/version, terms, checksum | Đạt + Thái | Week 1 Day 2 | OPEN |
| D-02 | Event-time mapping | If PaySim `step` semantics are confirmed, use configured simulation anchor + step hours; preserve `event_time_step` | Thái | Week 1 Day 5 | OPEN |
| D-03 | Stable transaction ID | Source file version/checksum + stable row locator; document dependence on stable source ordering | You + Đạt | Week 1 Day 5 | OPEN |
| D-04 | NoSQL choice | MongoDB proposed for document-shaped alerts and flexible filtering | Công Anh | Week 1 Day 5 | OPEN |
| D-05 | Artifact format | Versioned directory with model/pipeline + `metadata.json` (feature order, preprocessing, threshold, data version, metrics); ensure Spark inference compatibility | You + Đạt | Week 1 Day 6 | OPEN |
| D-06 | Initial threshold | Configurable provisional value only; final threshold selected on validation data based on precision/recall trade-off | Thái + Đạt | Before Week 3 sign-off | OPEN |
| D-07 | HDFS deployment | Decide supported local/container deployment after checking OS and version compatibility; local `data/processed/` is a development fallback, not a replacement for the target | You | Week 1 Day 6 | OPEN |
| D-08 | Kafka topic | `transaction-events`; key by `transaction_id`; confirm partitions and broker config | Công Anh | Week 1 Day 6 | OPEN |
| D-09 | NoSQL layout | Start with one `predictions` collection and filter `alert_triggered=true`, unless access patterns justify separate alerts collection | Công Anh | Week 1 Day 6 | OPEN |
| D-10 | Labels in events | Exclude `isFraud`/`isFlaggedFraud` from scoring event; labels remain offline evaluation data | Thái + Đạt | Week 1 Day 5 | PROPOSED |
| D-11 | Data provenance | Record source URL, retrieval date, terms, checksum/version | Đức Anh + Đạt | Week 1 Day 6 | OPEN |

An item is closed only after the owner confirms it and the affected code/docs are updated. Adjust due dates to the team's calendar.
