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

For bounded replay, finish producing data before launching the job. The current
entry point prints each record's topic, partition and offset. Automatic Kafka
offset commits are disabled for the preview. The live preview stops on Ctrl+C.

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
