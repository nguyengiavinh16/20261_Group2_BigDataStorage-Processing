# Streaming detector

## M02 · Kafka input

The source reads the Kafka topic for the selected mode and exposes a PyFlink
DataStream with `record_key`, `record_value`, `source_topic`, `source_partition`
and `source_offset`. Key and value use Kafka's original bytes so the validation
stage can inspect malformed input and retain its source position.

| Mode | Topic | Consumer group | Offsets |
|---|---|---|---|
| `live` | `canonical.trades.v1` | `cg-detector-live` | Start at committed offsets, falling back to the earliest available offsets. Continue tailing. |
| `replay` | `replay.canonical.trades.v1` | `cg-detector-replay` | Start at the earliest available offsets and finish at the end offsets captured at startup. Leave the replay group's offsets untouched. |

For bounded replay, finish producing data before launching the job. Automatic
Kafka offset commits are disabled for the preview. The live preview stops on Ctrl+C.

Use the project development environment with the dependencies in
`streaming/requirements.txt`, a compatible Java runtime and a local Kafka
broker. From the repository root on Linux or WSL, set `CG_FLINK_JARS` to the Kafka
connector file URI. The streaming image already carries the connector.

```bash
export CG_FLINK_JARS=file:///absolute/path/to/flink-sql-connector-kafka-3.3.0-1.20.jar
export PYTHONPATH=streaming/detector/src
python -m detector.job --mode replay
```

The integration test writes fixture records to the replay topic. Point it at a
disposable local broker. The test checks the original bytes and Kafka offsets,
including a malformed UTF-8 payload.

```bash
CG_DETECTOR_TEST_KAFKA=localhost:32100 pytest -q -m integration streaming/detector/tests/test_source.py
```

## M03 · Validation and deduplication

Before event-time assignment, the job validates UTF-8 JSON against
`canonical_trade.v1`, checks that the Kafka key equals the trade symbol and
checks the mode against `replay_run_id`. Replay requires a non-empty run ID.
Live accepts an absent or null run ID. Accepted trades retain the original
Kafka record for downstream processing.

Flink keyed state deduplicates live trades by event ID and replay trades by
run ID plus event ID. The TTL defaults to 600 seconds of processing time
from the first accepted record, matching the normalizer's documented window.
Override it with `CG_DETECTOR__DEDUP_TTL_SECONDS`.

Validation failures and duplicates go to the `dlq_record.v1` side stream with
their Kafka coordinates. The preview prints accept or reject status and error type.
Kafka DLQ publishing is part of M09.

Input counters are `cg_detector_input_records_total`,
`cg_detector_accepted_records_total`, `cg_detector_invalid_records_total`
and `cg_detector_duplicate_records_total`.

```bash
pytest -q streaming/detector/tests/test_validation.py
pytest -q -m integration streaming/detector/tests/test_validation.py
```
