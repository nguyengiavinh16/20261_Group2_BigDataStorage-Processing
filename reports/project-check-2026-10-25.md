# Project verification — 25 October 2026

The selected Docker and ETL support files from `dat/week2-spark-etl` are now
present in the local `main` working tree, based on commit `3ddb62a4`. They have
not been committed or pushed. The tested normal paths pass, with the ingestion
edge cases below still unresolved.

| Check | Actual result | Evidence |
|---|---|---|
| All unit/integration tests in Linux Spark container | **20/20 PASS**, 32.618 s | [Docker test log](runtime-check-2026-10-25/docker-after-import-tests.log) |
| Schema and data quality tests in Conda `bigdata` | **13/13 PASS** | [Data quality](runtime-check-2026-10-25/after-import/data-quality-tests.log), [schemas](runtime-check-2026-10-25/after-import/schema-tests.log) |
| Profiling notebook | **15/15 cells PASS**, 104.745 s | [Executed copy](runtime-check-2026-10-25/after-import/notebook-runs/01_data_profiling.ipynb) |
| Basic EDA notebook | **11/11 cells PASS**, 34.963 s | [Executed copy](runtime-check-2026-10-25/after-import/notebook-runs/paysim_basic_eda.ipynb) |
| Label/evaluation notebook | **19/19 cells PASS**, 75.844 s | [Executed copy](runtime-check-2026-10-25/after-import/notebook-runs/w1_eda_label_evaluation.ipynb) |
| Real producer → Kafka → smoke consumer | **2/2 events received**, valid schema and keys; temporary topic deleted | [Kafka results](runtime-check-2026-10-25/after-import/kafka-results.json) |
| ETL CLI import/help in Conda | **PASS** after restoring schema/config | [CLI log](runtime-check-2026-10-25/after-import/etl-help.log) |
| Docker Compose configuration | **PASS** (`docker compose config --quiet`) | [Compose file](../docker-compose.yml) |
| Static parsing | **PASS**: 12 Python files, 15 JSON files, 5 YAML files; 45 notebook code cells compile | [Static results](runtime-check-2026-10-25/after-import/final-static-results.json) |
| Editable architecture | **PASS**: structural/semantic validation; 0 geometry findings | [Updated diagram](../docs/diagrams/system-architecture.drawio) |

Conda `bigdata` used Python 3.11.17, pandas 2.2.3, NumPy 2.4.6, PySpark 4.2.0,
and kafka-python 2.0.2. The Docker tests used Python 3.10.12, Spark 4.2.0 and
OpenJDK 21.0.12.1. The Spark tests exercised invalid records, four Parquet
outputs, readback, stable IDs, reruns, limits, and wrong headers.

The notebooks ran using the original 493,534,783-byte CSV recovered from the
existing local backup branch into a temporary source snapshot. A hard link
provided the alternate raw filename expected by the W1 notebook. Original
notebooks, historical reports and dataset outputs were not overwritten. See
[machine-readable results](runtime-check-2026-10-25/after-import/results.json).

## Files imported from the W2 branch

`Dockerfile.jupyter`, `docker-compose.yml`, `.dockerignore`, `README.md`,
`configs/README.md`, `configs/etl.json`, `configs/paysim_schema.json`,
`configs/data_profiling.json`, `src/processing/schema.py`, `tests/test_etl.py`,
`tests/fixtures/paysim_etl.csv`, and `reports/week2-unit-tests.log`.

The copied October 17 log is historical evidence. The October 25 test results
in the table above come from the new runs.

## Remaining reproducible ingestion issues

1. **Default producer logging fails on a fresh checkout.** `logs/` is absent;
   `FileHandler` raises `FileNotFoundError` before replay starts
   ([producer.py](../src/ingestion/producer.py#L35)). The successful Kafka run
   supplied `--log-file` pointing to an existing report directory.
2. **Nonfinite optional balances pass validation.** Source balances `NaN` or
   `Infinity` become floats, are accepted, and the serializer permits nonstandard
   JSON tokens ([mapping](../src/ingestion/producer.py#L22),
   [serializer](../src/ingestion/producer.py#L46)).
3. **Malformed rows can abort replay.** A truncated balance field represented
   as `None` raises `AttributeError`; an oversized positive `step` raises
   `OverflowError`. Neither exception is handled by the current replay loop
   ([timestamp mapping](../src/ingestion/producer.py#L14),
   [exception handling](../src/ingestion/producer.py#L61)).
4. **Consumer timeout can be exceeded under continuous traffic.** The deadline
   is checked outside the inner message loop, so reaching the message count can
   take longer than the requested timeout
   ([consumer](../src/ingestion/consumer_test.py#L18)).

[Contract probes](runtime-check-2026-10-25/contract-probes.json) also show that
`validate_transaction` checks only part of the JSON Schema contract. Its
acceptance of wrong versions, timestamps or extra fields is a helper-level gap;
the producer itself generates its version/time fields and excludes labels.

## Scope and runtime state

Spark directly on Windows/Conda did not finish the environment smoke within
50 seconds, so the ETL tests used the project's configured Linux Docker runtime.
No environment dependencies were added to Conda `bigdata` by the test setup.
The existing notebook installation cell found its packages already installed.

The full dataset was used for the notebooks; the new ETL verification used
fixtures, not another full-dataset ETL run. Training, streaming inference,
NoSQL persistence, FastAPI and dashboard remain planned, and no end-to-end
scoring or exactly-once claim is made.

Docker Desktop, `fraud-spark` and the existing `spark-kafka` broker were started
for these checks and remain running. The default `transactions` topic was not
used for test writes. No Git commits or remote pushes were performed.
