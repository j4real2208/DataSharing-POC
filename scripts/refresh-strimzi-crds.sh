#!/usr/bin/env bash
set -euo pipefail

CTX="${1:-}"

if [[ -z "$CTX" ]]; then
  echo "Usage: $0 <kube-context>" >&2
  exit 1
fi

if [[ "${CONFIRM:-}" != "YES" ]]; then
  echo "This will delete Strimzi CRDs in $CTX and remove all Strimzi custom resources." >&2
  echo "Re-run with CONFIRM=YES to proceed." >&2
  exit 1
fi

crds=(
  kafkas.kafka.strimzi.io
  kafkanodepools.kafka.strimzi.io
  kafkatopics.kafka.strimzi.io
  kafkausers.kafka.strimzi.io
  kafkaconnects.kafka.strimzi.io
  kafkaconnectors.kafka.strimzi.io
  kafkabridges.kafka.strimzi.io
  kafkamirrormakers.kafka.strimzi.io
  kafkamirrormaker2s.kafka.strimzi.io
  kafkarebalances.kafka.strimzi.io
)

for crd in "${crds[@]}"; do
  kubectl --context "$CTX" delete crd "$crd" >/dev/null 2>&1 || true
done

echo "Strimzi CRDs removed from $CTX. Re-run the deploy script to reinstall." 
