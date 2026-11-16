#!/usr/bin/env bash
# Build a component image with the repo root as context, then load it into the kind cluster.
# Usage: build-image.sh ingest|streaming|batch|airflow|serving
set -euo pipefail

COMPONENT="$1"
CLUSTER="${CLUSTER:-cryptoguard}"
TAG="${TAG:-dev}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

case "$COMPONENT" in
  ingest) DOCKERFILE=ingest/Dockerfile ;;
  streaming) DOCKERFILE=streaming/Dockerfile ;;   # normalizer + detector
  batch) DOCKERFILE=batch/Dockerfile ;;
  airflow) DOCKERFILE=batch/airflow/Dockerfile ;;
  serving) DOCKERFILE=serving/Dockerfile ;;
  *) echo "unknown component: $COMPONENT"; exit 2 ;;
esac

IMAGE="cg-$COMPONENT:$TAG"
echo "building $IMAGE from $DOCKERFILE"
docker build -t "$IMAGE" -f "$ROOT/$DOCKERFILE" "$ROOT"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind load docker-image "$IMAGE" --name "$CLUSTER"
  echo "loaded $IMAGE into kind/$CLUSTER"
fi
