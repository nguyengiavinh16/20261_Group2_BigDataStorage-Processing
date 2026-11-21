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

For bounded replay, finish producing data before launching the job. Use
`--preview` to print the pipeline output without publishing or committing
Kafka source offsets. The live preview stops on Ctrl+C.

Use the project development environment with the dependencies in
`streaming/requirements.txt`, a compatible Java runtime and a local Kafka
broker. From the repository root on Linux or WSL, set `CG_FLINK_JARS` to the Kafka
connector file URI. The streaming image already carries the connector.

```bash
export CG_FLINK_JARS=file:///absolute/path/to/flink-sql-connector-kafka-3.3.0-1.20.jar
export PYTHONPATH=streaming/detector/src
python -m detector.job --mode replay --preview
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

## M07 · Pump and dump rules

`evaluate_scored_feature()` requires a usable M06 baseline and a volume
z-score before it evaluates the rules in `detector.pump_rule` and
`detector.dump_rule`. A dump also requires an open pump case. Hits carry
the configured rule version, risk, reasons and baseline source. A hit
from an incomplete window includes `INCOMPLETE_WINDOW`. When an alert
is built, the baseline source is recorded in `evidence.baseline_source`.

Rule calibration against real cases uses the DET-01 replay dataset.

## M08 · Pump and dump cases

Flink keeps case state per replay run, symbol and exchange. Live input
has no run ID. Venue alerts and `ALL` alerts have separate cases. The
first pump window supplies the ID `<symbol>:<exchange>:<first_window_start_ts_us>`.
A dump joins an open case when its window starts at most
`detector.case_merge_minutes` after the end of the last alert window.
Each alert extends that interval. A pump beyond the interval starts a new case.

The case timer uses event time and keeps state through the end and
allowed lateness of the last eligible window. State also has a fallback
processing-time TTL of at least 24 hours since its last update.
The managed state and event-time timers can be snapshotted with the
Flink job. Deployment and durable checkpoint storage are part of M11.

The detector emits the first alert per window and key. A revision of
the latest window can trigger its first alert. Older windows and windows
that already alerted still produce features on revision.

In preview mode, live and replay print `status=alert` with a nested
`alert.v1` record.

```bash
pytest -q streaming/detector/tests/test_cases.py
pytest -q -m integration streaming/detector/tests/test_cases.py
```

## M09 · Kafka outputs

Publishing is the default for `python -m detector.job --mode live` and
`python -m detector.job --mode replay`. Create the topics from the
project registry before starting the job.

| Payload | Live topic | Replay topic | Kafka key |
|---|---|---|---|
| `symbol_features_1m.v1` | `features.symbol_1m.v1` | `replay.features.symbol_1m.v1` | `symbol` |
| `alert.v1` | `alerts.v1` | `replay.alerts.v1` | `case_id` |
| `dlq_record.v1` | `dlq.detector.v1` | Local rejection log | `source_topic` |

Live DLQ includes input validation, duplicate and expired-window errors.
It preserves the original topic, partition, offset and bounded raw payload
preview. Replay rejection lines include the input coordinates and error
type. A replay DLQ topic requires coordination with R2 and R4.

Flink publishes with at-least-once delivery and checkpoints every
`flink.checkpoint_interval_seconds`. Live can commit Kafka source offsets on
completed checkpoints. Replay leaves its group offsets untouched. A
restart can publish a record again when the broker accepted it after
the last completed checkpoint. Durable checkpoint storage and deployment
recovery are part of M11.

The local recovery check restored a checkpoint taken after the pump and
then replayed the dump that Kafka had already received. The consumer saw
one pump and two deliveries of the dump, with the same case and window and
different alert UUIDs.

Kafka values contain the full contract payload. Feature corrections are
appended with the same window identity `(run ID, symbol, exchange, window start)`.
For comparing alerts across attempts, use `(run ID, case ID, alert type, window start)`
because rebuilding an alert generates a new UUID. R5 needs to apply the
feature update and alert deduplication policy when storing these records.

Run the replay integration test against disposable Kafka and Redis
instances with the replay input and output topics created:

```bash
CG_DETECTOR_TEST_KAFKA=localhost:32100 \
CG_DETECTOR_TEST_REDIS_URL=redis://localhost:30379/0 \
pytest -q -m integration streaming/detector/tests/test_outputs.py
```

## M10 · Metrics and diagnostics

| Metric suffix | Meaning |
|---|---|
| `cg_detector_windows_total{exchange}` | First observation of each populated window per run, symbol and exchange, including `ALL`. Late feature corrections keep the same window count. |
| `cg_detector_alerts_total{alert_type,severity}` | Alerts built by the detector in live or replay. |
| `cg_detector_late_events_total` | Source trades sent to DLQ after window expiry. One trade counts once before the venue and ALL feature branches. |
| `cg_detector_baseline_missing_total` | Scored feature snapshots without a usable Redis or EMA baseline. |
| `cg_detector_redis_errors_total` | Failed Redis baseline requests, including timeouts. |
| `cg_detector_latency_ms` | Live window-end to alert latency in whole milliseconds. |

Flink adds its metric scope prefix and its job and task labels. The latency
histogram uses cumulative `_bucket{le}`, `_count` and `_sum` counters.
PyFlink 1.20 exposes these components as gauge samples through the Flink
reporter. They increase during each task attempt. A negative live sample
appears in the alert diagnostic and is excluded from the histogram.

Set `CG_DETECTOR__METRICS_PORT=9249` for a local mini-cluster with
the Prometheus reporter JAR on the JVM classpath. The reporter can bind
to port 9249 or 9250. With a PyFlink wheel, set `FLINK_HOME` to the
installed `pyflink` directory and point `FLINK_LIB_DIR` at a directory
containing the standard Flink library JARs and the reporter JAR from
`plugins/metrics-prometheus/`.

```bash
CG_DETECTOR__METRICS_PORT=9249 python -m detector.job --mode live
# Run in a second shell
curl -fsS http://127.0.0.1:9249/metrics | grep cg_detector_
curl -fsS http://127.0.0.1:9250/metrics | grep cg_detector_
```

For R1's deployment reporter, set
`metrics.reporter.prom.filterLabelValueCharacters: "false"` to preserve
the `le="+Inf"` label. Compute window-end-to-alert p95 in milliseconds
using the bucket series.

```promql
histogram_quantile(0.95, sum by (le) (
  rate({__name__=~".*_cg_detector_latency_ms_bucket",job_name="cg-detector-live"}[5m])
))
```

The JSON diagnostics `alert_detected`, `trade_rejected` and
`redis_baseline_error` include the replay run ID when known. Rejections
also include the Kafka topic, partition and offset. Run IDs and offsets
are log fields. Compare late-event totals for O7 against `LATE_EVENT`
records written to the live detector DLQ.

### Local load check

A local Kafka, Flink 1.20.5 and Redis run used one Flink subtask,
a five-second watermark and two-second checkpoints. The synthetic
burst contained 569 trades across nine symbols. It produced 16
schema-valid alerts and 18 first window observations across Binance
and ALL. The latest trade used as the latency reference was two seconds
before the window end for each alert.

| Latency | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|
| Window end to alert | 8875.26 | 8898.48 | 8898.48 |
| Latest trade event to alert | 10875.26 | 10898.48 | 10898.48 |

Percentiles use nearest rank over the 16 alert payloads. The observed
burst input rate was 370.3 messages/s from producer start until the
input counter reached 569. The peak sampled Flink backpressure was
0 ms/s. Kafka consumer-group lag returned to 0 records after checkpoint.

The cold symbol added two baseline misses. A Redis pause produced two
timeout errors. A late correction produced two feature revisions and
left the window and alert totals at 18 and 16. One expired source
trade added one late count and one live DLQ record at partition 0,
offset 571.

## M11 · Deploy and recover on kind

Use the local kind cluster with the Flink 1.10 operator, Kafka topics from
the registry, Redis and the MinIO bucket `cg-lake` ready. R1 supplies the
`flink` service account and MinIO credentials for the detector's prefixes.
The detector image pins Flink and PyFlink 1.20.5, Java 17, Python 3.11 and
Kafka SQL connector 3.3.0-1.20. Its Hadoop S3 and Prometheus plugins are
installed in the Flink image. Build from the repository root.

```bash
docker build -t cg-detector:dev -f streaming/detector/Dockerfile .
docker run --rm --entrypoint sh cg-detector:dev -c \
  'java -version && python -c "import importlib.metadata as m; print(m.version(\"apache-flink\"))"'
kind load docker-image cg-detector:dev --name cryptoguard

detector_kube() { kubectl --context kind-cryptoguard -n cg-stream "$@"; }
detector_kube get serviceaccount flink
# Provision once with the credential file supplied by R1.
detector_kube create secret generic cg-detector-checkpoints \
  --from-env-file "$DETECTOR_S3_ENV_FILE"
detector_kube apply -f streaming/k8s/detector.flinkdeployment.yaml
detector_kube apply -f streaming/k8s/detector-replay.flinkdeployment.yaml
detector_kube get flinkdeployment
```

The credential file contains `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. Keep the file outside the repository. Allow
read, write, delete and list access needed by Flink in
`cg-lake/flink/detector/`. Both jobs use a 30-second checkpoint interval
and Kubernetes HA.

| Mode | Deployment | Kafka group | MinIO prefix under `cg-lake/flink/detector/` |
|---|---|---|---|
| Live | `cg-detector` | `cg-detector-live` | `live/` |
| Replay | `cg-detector-replay` | `cg-detector-replay` | `replay/` |

Each prefix has separate `checkpoints/`, `savepoints/` and `ha/`
paths. The live job starts running. The replay manifest starts suspended
to allow fixture production and run selection first. The outputs still
follow the M09 topic table.

### Run one bounded replay

Produce the canonical fixture or the R2 replay into
`replay.canonical.trades.v1` with a unique `replay_run_id` on every
record. Finish publishing and record the earliest and latest Kafka
offsets per partition from a Kafka client. The source starts at the
retained earliest offsets and captures exclusive end offsets at startup.
Keep the relevant input within Kafka retention for the duration of the
run and record the offset range with the run ID. Limit the fixture to
the local data budget.

```bash
detector_kube create configmap cg-detector-replay-run \
  --from-literal="run-id=$REPLAY_RUN_ID" --dry-run=client -o yaml | detector_kube apply -f -
detector_kube patch flinkdeployment cg-detector-replay --type merge \
  -p '{"spec":{"job":{"state":"running"}}}'
detector_kube get flinkdeployment cg-detector-replay -w
```

`CG_REPLAY_RUN_ID` selects the validated run before dedup and
watermark assignment. Valid trades from other runs increment
`cg_detector_replay_filtered_total`. Keep the same run ID when resuming
from a snapshot. To start an independent run, remove the previous
replay deployment after collecting its output and any snapshots you
need. Reapply the suspended replay manifest, select a new run ID, then
start it. Reuse the existing Kafka topic and leave the live deployment
running during the replay.

For an isolation check on a disposable kind cluster, send the same
canonical event IDs twice to the replay topic with different run IDs.
Select one run per deployment and read new records from the offsets
recorded on all four feature and alert topics. Each replay should produce
only its selected run. With simultaneous live input, live features and
alerts should have null run IDs. The fixture and baseline values from
`tests/test_outputs.py` provide a reference for two pump and two dump
alerts per replay.

### Verify recovery after a TaskManager replacement

Use the live job in the test cluster for this check. Send the pump
portion of a synthetic case before taking the checkpoint. Advance event
time with later trades so the pump window closes. Compare new Kafka
records by querying from the output offsets recorded before the run.

1. Run `detector_kube port-forward svc/cg-detector-rest 8081:8081`
   and open the Flink UI.
   Record the job ID, the completed checkpoint ID and its
   `s3a://cg-lake/flink/detector/live/checkpoints/` location. The
   checkpoint details are available at `/jobs/<job-id>/checkpoints`.
   Record the committed input offsets for `cg-detector-live`.
2. List the target pods with
   `detector_kube get pods -l app=cg-detector,component=taskmanager`.
   Delete the named live TaskManager pod with
   `detector_kube delete pod "$LIVE_TASKMANAGER_POD"` and observe the
   replacement pod, job logs and restored checkpoint.
3. Send the dump and advance event time again once the job is running.
   Compare Kafka records with a control run using the M09 business keys.
   Confirm that the restored dump carries the original pump case ID.
   Record missing and repeated output counts, including any new UUIDs
   for repeated alerts under at-least-once delivery.

For a deliberate redeploy from a retained snapshot, set
`spec.job.initialSavepointPath` to the completed snapshot location and
increment `spec.job.savepointRedeployNonce`. Use the same job mode and
replay selection as the snapshot, then verify the restored offsets and
case state before continuing.

### Local verification

Both manifests passed validation against the Flink Operator 1.10 CRD.
A local Flink PythonDriver run with Kafka, Redis and an S3 emulator
stored checkpoint 2 after the two pump alerts at source offset 25.
A separate JVM restored that checkpoint. The two subsequent dump
alerts matched the original Binance and ALL case IDs.

Kind acceptance requires building the detector image, applying both
deployments with the R1 MinIO credentials and running the TaskManager
replacement check above.
