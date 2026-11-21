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

## M04 · Event-time windows

After M03, Flink assigns event time from `event_ts_us`. Flink receives whole
milliseconds and the trades retain their original microseconds. Windows
are 60-second UTC intervals `[start, end)` keyed by replay run and symbol.
Each snapshot contains the input trades for M05 to compute venue and ALL
features. A bounded replay flushes its final window at end of input.
The watermark tracks the whole Flink input. Use a replay input scoped to one
run when evaluating runs with different event-time ranges.

The detector profile controls `watermark_seconds` and
`allowed_lateness_seconds`. An idle watermark subtask stops blocking
active subtasks after 10 seconds by default. Override this with
`CG_DETECTOR__WATERMARK_IDLE_TIMEOUT_SECONDS`.

Flink emits an initial snapshot when the watermark reaches the window
boundary. A trade accepted during allowed lateness triggers an updated
snapshot with its exchange in `late_exchanges` and `is_complete=false`.
These snapshots are provisional. A trade arriving after the grace period
produces one `LATE_EVENT` in the DLQ side stream and increments
`cg_detector_late_events_total`. It can be traced to its original window
using its trade timestamp. Gap detection and how to publish feature
corrections are integration decisions for the subsequent modules.

The job preview includes one `status=window` line for each emitted
snapshot, alongside the accepted and rejected record lines.

## M05 · Venue and ALL features

Each window snapshot produces one `symbol_features_1m.v1` record for each
exchange with trades and one for `ALL`, using the shared
`cryptoguard_common.features.window_features()` helper. ALL uses the raw
trades across venues. For equal event timestamps and trade IDs, the detector
orders ties by exchange and event ID. R4 should use the same tie order for
batch parity.

An exchange with an accepted late trade and the corresponding ALL row have
`is_complete=false`. A later snapshot for the same window uses the same
run, symbol, exchange and window timestamps. Kafka correction handling is
part of M09. M06 fills in baseline fields after feature construction.

The job prints `status=feature` with a nested feature record in live and
replay previews.

## M06 · Redis baseline and EMA

The detector reads `baseline:<symbol>:<exchange>:<hour>` using the UTC hour
of the window start. It scores volume with the shared `volume_zscore()` helper.
Redis snapshots must contain a finite positive mean, a finite non-negative
standard deviation, at least two windows and a date no more than three days
before the feature window. Replay requires a snapshot from a prior UTC date
because `as_of_date` has day precision. Set `CG_REDIS__URL` to a Redis instance
containing the historical snapshot when replaying old data.

Each worker caches up to 512 keys for 60 seconds, separated by replay run.
Redis connection and read timeouts default to 0.25 seconds. If Redis has no
usable snapshot, a Flink keyed EMA uses the previous windows for that
run, symbol and exchange. EMA uses alpha 0.2 and warms up on five windows
with known USD volume. A revision of the latest window replaces its sample
without counting it twice. An older revision can use Redis, while its EMA
score stays null to avoid incorporating later windows.

Feature records keep the existing JSON schema. The preview adds
`baseline_source` and `redis_status` alongside the record. M07 can use the
source when building alert evidence. Without a usable baseline the output
has `volume_z=null`. The associated counters are
`cg_detector_baseline_missing_total` and `cg_detector_redis_errors_total`.

Override the defaults through `CG_DETECTOR__BASELINE__<NAME>` using
`MAX_AGE_DAYS`, `MIN_WINDOWS`, `CACHE_SECONDS`, `CACHE_MAX_ENTRIES`,
`SOCKET_TIMEOUT_SECONDS`, `EMA_ALPHA` or `EMA_MIN_WINDOWS`.
