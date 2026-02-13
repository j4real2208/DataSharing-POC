#!/usr/bin/env bash
set -euo pipefail

CTX="${1:-minikube-a}"
IMAGE="${2:-local/kafka-connect:3.4.1}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to build the Kafka Connect image" >&2
  exit 1
fi

if ! command -v minikube >/dev/null 2>&1; then
  echo "minikube is required to load the image into the cluster" >&2
  exit 1
fi

docker build -t "$IMAGE" \
  --build-arg DEBEZIUM_VERSION=3.4.1.Final \
  connect-image
minikube -p "$CTX" image load "$IMAGE"
minikube -p minikube-b image load "$IMAGE"


echo "Loaded $IMAGE into $CTX"
