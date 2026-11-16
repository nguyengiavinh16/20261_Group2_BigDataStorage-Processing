#!/usr/bin/env bash
# Bring up the local kind cluster by profile. Idempotent: safe to re-run.
# Usage: up-local.sh core|stream|batch|serving|monitoring
set -euo pipefail

PROFILE="${1:-core}"
CLUSTER="${CLUSTER:-cryptoguard}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
K8S="$ROOT/infra/k8s"
VALUES="$ROOT/infra/helm-values"

# Pin versions here (INF-03/INF-06). VERIFY latest stable before P1 and keep these in sync with docs/local-setup.md.
STRIMZI_INSTALL_URL="${STRIMZI_INSTALL_URL:-https://strimzi.io/install/latest?namespace=cg-kafka}"
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.10.0}"

log() { printf '\n\033[1;34m[up-local:%s]\033[0m %s\n' "$PROFILE" "$*"; }

ensure_cluster() {
  if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    log "creating kind cluster $CLUSTER"
    kind create cluster --name "$CLUSTER" --config "$ROOT/infra/local/kind-config.yaml"
  fi
  kubectl config use-context "kind-$CLUSTER" >/dev/null
  kubectl apply -f "$K8S/base/namespaces.yaml"
}

up_core() {
  ensure_cluster
  log "Strimzi operator"
  kubectl apply --server-side -f "$STRIMZI_INSTALL_URL" -n cg-kafka
  kubectl -n cg-kafka rollout status deploy/strimzi-cluster-operator --timeout=300s

  log "Kafka cluster (KRaft, 1 node)"
  kubectl apply -f "$K8S/kafka/kafka.local.yaml"
  kubectl -n cg-kafka wait kafka/cg-kafka --for=condition=Ready --timeout=600s

  log "Kafka topics (generated from contracts/kafka-topics.yaml)"
  kubectl apply -f "$K8S/generated/topics.local.yaml"

  log "MinIO + Postgres"
  kubectl apply -f "$K8S/storage/postgres.local.yaml"
  kubectl apply -f "$K8S/storage/minio.local.yaml"
  kubectl -n cg-storage rollout status deploy/cg-postgres --timeout=300s
  kubectl -n cg-storage rollout status deploy/cg-minio --timeout=300s
}

up_stream() {
  ensure_cluster
  log "Flink Kubernetes Operator $FLINK_OPERATOR_VERSION (webhook disabled → no cert-manager needed locally)"
  helm repo add flink-operator-repo \
    "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" --force-update
  helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
    -n cg-stream --set webhook.create=false --set "watchNamespaces={cg-stream}" --wait
}

up_batch() {
  ensure_cluster
  log "Spark Operator"
  helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
  helm upgrade --install spark-operator spark-operator/spark-operator \
    -n cg-batch --set "spark.jobNamespaces={cg-batch}" --wait

  if docker image inspect cg-airflow:dev >/dev/null 2>&1; then
    log "Airflow (image cg-airflow:dev)"
    kind load docker-image cg-airflow:dev --name "$CLUSTER"
    helm repo add apache-airflow https://airflow.apache.org --force-update
    helm upgrade --install airflow apache-airflow/airflow -n cg-batch -f "$VALUES/airflow.local.yaml" --timeout 15m
  else
    log "SKIP Airflow: build it first with 'make image-airflow' (INF-10)"
  fi
}

up_serving() {
  ensure_cluster
  log "MongoDB + Redis"
  kubectl apply -f "$K8S/serving/mongodb.local.yaml"
  kubectl apply -f "$K8S/serving/redis.local.yaml"
  kubectl -n cg-serving rollout status deploy/cg-mongo --timeout=300s
  kubectl -n cg-serving rollout status deploy/cg-redis --timeout=300s
}

up_monitoring() {
  ensure_cluster
  log "kube-prometheus-stack"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
  helm upgrade --install kps prometheus-community/kube-prometheus-stack \
    -n cg-monitoring -f "$VALUES/kube-prometheus-stack.local.yaml" --wait --timeout 10m
}

case "$PROFILE" in
  core) up_core ;;
  stream) up_stream ;;
  batch) up_batch ;;
  serving) up_serving ;;
  monitoring) up_monitoring ;;
  *) echo "unknown profile: $PROFILE (core|stream|batch|serving|monitoring)"; exit 2 ;;
esac
log "done. 'make status' to inspect."
