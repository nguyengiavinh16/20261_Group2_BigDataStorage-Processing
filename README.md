# Real-time Financial Fraud Detection and Alerting System

An academic prototype for historical fraud analysis and replay-based near-real-time fraud detection. It uses public or synthetic data; it is not a production banking system.

## Current milestone

Week 1 foundation:
- project scope and Lambda Architecture documentation
- configuration and environment templates
- PaySim schema inspection utility
- a small synthetic fixture for smoke testing
- basic unit tests and package layout

Later milestones add Spark/HDFS, Kafka, model training, Structured Streaming, MongoDB, FastAPI, Streamlit, Docker Compose, and Kubernetes.

## Requirements

- Python 3.10 or 3.11
- Git
- A virtual environment
- The full PaySim CSV is optional for Week 1; use the included synthetic fixture to test the inspector.

Java, Spark, Hadoop, Kafka, and MongoDB versions will be pinned and validated before their integration milestones.

## Setup

### Windows PowerShell

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
Copy-Item .env.example .env
python scripts/inspect_data.py --input data/sample/transactions_sample.csv
pytest
```

### Linux/macOS

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
cp .env.example .env
python scripts/inspect_data.py --input data/sample/transactions_sample.csv
pytest
```

The sample is synthetic and intentionally small. It only verifies schema inspection mechanics; it is not suitable for training or evaluating a fraud model.

## Full PaySim dataset

Obtain PaySim from a reputable dataset host such as its Kaggle dataset page, following the host's current terms and access procedure. Place the extracted CSV under `data/raw/`, for example:

```text
data/raw/PS_20174392719_1491204439457_log.csv
```

Then run:

```bash
python scripts/inspect_data.py --input data/raw/PS_20174392719_1491204439457_log.csv
```

Do not commit the full dataset, credentials, local virtual environments, or model artifacts to Git.

## Repository status

Generated Week 1 files are a starting foundation. No external services are started by this milestone. Do not describe Spark, Kafka, HDFS, MongoDB, API, dashboard, or Kubernetes as implemented until their milestones are coded and verified.

## Project documentation

- `docs/project_charter.md`
- `docs/architecture.md`
- `docs/data_dictionary.md`
- `docs/data_flow.md`
- `docs/weekly_roadmap.md`
- `docs/testing_strategy.md`
- `docs/performance_evaluation.md`

## License and data

The source code license is in `LICENSE`. Dataset licensing and usage are governed by the dataset provider and must be checked separately.
