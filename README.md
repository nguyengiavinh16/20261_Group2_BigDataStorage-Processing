# Real-time Financial Fraud Detection and Alerting System

An academic prototype for PaySim fraud detection with batch processing, model training, Kafka replay, Spark Structured Streaming, MongoDB persistence, FastAPI serving, Streamlit UI, Docker Compose and starter Kubernetes manifests. This is not a production banking system.

## Current project structure

- `scripts/inspect_data.py`: schema/profile inspection of a bounded sample.
- `scripts/run_etl.py`: local CSV-to-Parquet conversion.
- `scripts/run_spark_etl.py`: Spark CSV-to-Parquet job.
- `src/fraud_detection/processing/`: shared feature handling and ETL.
- `src/fraud_detection/modeling/train.py`: model training and metrics.
- `src/fraud_detection/ingestion/kafka_producer.py`: replay transactions to Kafka.
- `src/fraud_detection/streaming/spark_stream.py`: streaming inference and MongoDB sink.
- `src/fraud_detection/serving/`: predictor and FastAPI.
- `app/dashboard.py`: Streamlit transaction-scoring UI.
- `docker-compose.yml`: Kafka, MongoDB, API and dashboard.
- `deployment/kubernetes/`: starter namespace, deployment and service manifests.
- `docs/implementation_guide_vi.md`: step-by-step instructions in Vietnamese.

## Requirements

- Python 3.10 (recommended for this project) or 3.11.
- For Docker deployment: Docker Desktop with Docker Compose.
- For Spark jobs: a compatible Java installation, PySpark and the matching Kafka connector for streaming.
- Full PaySim CSV placed at `data/raw/paysim_dataset.csv`.

Do not commit the full dataset, `.env`, credentials, virtual environments, checkpoints, or generated model artifacts to Git.

## Setup (Windows PowerShell)

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev,dashboard,batch,streaming]"
Copy-Item .env.example .env
```

If Python 3.10 is not installed, install it first or use an installed supported version. Check `python --version` after activating the virtual environment.

## Week 1: inspect and test

```powershell
python scripts/inspect_data.py --input data/raw/paysim_dataset.csv --sample-rows 100000 --output reports/paysim_inspection.json
python -m pytest -q
```

The inspection report describes only the rows read by the configured sample limit. It is not automatically a full-file audit.

## Week 2: ETL

```powershell
python scripts/run_etl.py --input data/raw/paysim_dataset.csv --output data/processed/paysim.parquet
```

For a Spark environment:

```powershell
python scripts/run_spark_etl.py --input data/raw/paysim_dataset.csv --output data/processed/paysim_parquet
```

## Week 3: train model

```powershell
python -m fraud_detection.modeling.train --input data/raw/paysim_dataset.csv --algorithm logistic
```

Try the Random Forest baseline:

```powershell
python -m fraud_detection.modeling.train --input data/raw/paysim_dataset.csv --algorithm forest
```

Outputs: `artifacts/fraud_model.joblib` and `reports/model_metrics.json`. The baseline uses a stratified random split. Compare with a temporal split by `step` before making claims about future performance. Threshold `0.5` is a starting point, not a tuned operational threshold.

## Week 4: Kafka replay and streaming

Start Kafka with `docker compose up -d kafka`, then in separate terminals run:

```powershell
python -m fraud_detection.ingestion.kafka_producer --input data/raw/paysim_dataset.csv --limit 10000 --delay-ms 20
python -m fraud_detection.streaming.spark_stream
```

Spark streaming needs a Spark Kafka connector compatible with the installed Spark version. The Compose stack does not create a Spark cluster or HDFS cluster.

## Week 5: API and dashboard

First ensure the trained model artifact exists, then:

```powershell
uvicorn fraud_detection.serving.api:app --reload --app-dir src
```

Open `http://127.0.0.1:8000/docs`. Start the UI in another terminal:

```powershell
streamlit run app/dashboard.py
```

Open `http://localhost:8501`.

## Week 6: basic benchmark

```powershell
python scripts/benchmark_api.py --url http://localhost:8000/health --requests 100
python -m pytest -q
```

This is a simple latency/error probe, not a distributed load test.

## Week 7: Docker Compose

Train the model on the host first so `artifacts/fraud_model.joblib` exists, then run:

```powershell
docker compose up --build -d
```

- API: `http://localhost:8000/health`
- API docs: `http://localhost:8000/docs`
- Dashboard: `http://localhost:8501`
- Kafka: `localhost:9092`
- MongoDB: `localhost:27017`

The Compose file starts Kafka, MongoDB, API and dashboard. Spark streaming is launched separately in a compatible Spark environment.

## Week 8: validation and report

Run tests, record model metrics and threshold, measure API latency and end-to-end streaming delay, test restart behavior, and document actual results. Do not report unmeasured performance values.

## Important limitations

- Academic prototype only; no authentication, secrets management, human review workflow, or production-grade alert delivery.
- `isFraud` is a label and must never be used as an inference feature.
- The baseline excludes `nameOrig` and `nameDest` from model inputs to avoid huge identifier cardinality.
- API MongoDB persistence is best-effort. Production systems need retries, idempotency, monitoring, access control and audit logs.
- Kubernetes manifests are a starting template. Configure image registry, model delivery, secrets, resources, networking, Kafka/MongoDB services and security before using them.
- Dataset terms and licensing remain the responsibility of the dataset provider.

## Documentation

See `docs/implementation_guide_vi.md`, `docs/architecture.md`, `docs/data_dictionary.md`, `docs/data_flow.md`, `docs/testing_strategy.md`, and `docs/performance_evaluation.md`.
