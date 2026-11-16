# CryptoGuard developer entry points. Run from WSL2/Linux/macOS shell.
SHELL := /bin/bash
PY ?= python3.11
VENV ?= .venv
ACT := source $(VENV)/bin/activate
CLUSTER ?= cryptoguard

.PHONY: help dev-install lint fmt test contracts gen-topics check-generated \
        up up-stream up-batch up-serving up-monitoring status down \
        image-% seed-samples

help:
	@grep -E '^[a-zA-Z_%-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n",$$1,$$2}'

# ---------- Python ----------
dev-install: ## Create venv and install libs/common + dev tools
	$(PY) -m venv $(VENV)
	$(ACT) && pip install -U pip && pip install -r requirements-dev.txt

lint: ## ruff check
	$(ACT) && ruff check . && ruff format --check .

fmt: ## ruff format + autofix
	$(ACT) && ruff format . && ruff check --fix .

test: contracts ## Unit tests for all components (no cluster needed)
	$(ACT) && pytest -q

contracts: ## Validate contracts/samples against contracts/schemas
	$(ACT) && pytest -q contracts/tests

gen-topics: ## Generate Strimzi KafkaTopic CRs from contracts/kafka-topics.yaml
	$(ACT) && python infra/scripts/gen_topics.py --profile local --out infra/k8s/generated/topics.local.yaml
	$(ACT) && python infra/scripts/gen_topics.py --profile cloud --out infra/k8s/generated/topics.cloud.yaml

check-generated: gen-topics ## Fail if generated files are stale (used by CI)
	git diff --exit-code infra/k8s/generated/

# ---------- Local cluster (kind) ----------
up: ## kind cluster + core (namespaces, Kafka, MinIO, Postgres)
	bash infra/scripts/up-local.sh core

up-stream: ## + Flink Kubernetes Operator
	bash infra/scripts/up-local.sh stream

up-batch: ## + Spark Operator + Airflow
	bash infra/scripts/up-local.sh batch

up-serving: ## + MongoDB + Redis
	bash infra/scripts/up-local.sh serving

up-monitoring: ## + kube-prometheus-stack (Prometheus, Grafana)
	bash infra/scripts/up-local.sh monitoring

status: ## Pods across cg-* namespaces
	kubectl get pods -A -l 'app.kubernetes.io/part-of=cryptoguard' -o wide || true
	kubectl get pods -A | grep -E '^cg-' || true

down: ## Delete the local kind cluster (data in PVCs is lost)
	kind delete cluster --name $(CLUSTER)

# ---------- Images ----------
# Usage: make image-ingest | image-streaming | image-batch | image-airflow | image-serving
image-%: ## Build image for a component and load it into kind
	bash infra/scripts/build-image.sh $*

seed-samples: ## Publish contracts/samples/*.jsonl into their Kafka topics (local)
	$(ACT) && python infra/scripts/seed_samples.py
