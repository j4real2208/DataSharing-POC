#!/usr/bin/env bash
set -euo pipefail

CTX="${1:-minikube-a}"

kubectl --context "$CTX" -n database get pods
kubectl --context "$CTX" -n messaging get kafka,kafkaconnect,kafkamirrormaker2
kubectl --context "$CTX" -n gateway get pods
