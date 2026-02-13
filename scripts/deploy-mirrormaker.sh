#!/usr/bin/env bash
set -euo pipefail

SOURCE_CTX="${1:-minikube-a}"
SINK_BOOTSTRAP="${2:-}"

if [[ -z "$SINK_BOOTSTRAP" ]]; then
  echo "Usage: $0 <source-context> <sink-bootstrap-host:port>" >&2
  exit 1
fi

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT

sed "s|SINK_BOOTSTRAP|$SINK_BOOTSTRAP|g" poc/kafka/mirrormaker2-source-to-sink.yaml > "$tmpfile"

kubectl --context "$SOURCE_CTX" apply -f "$tmpfile"
