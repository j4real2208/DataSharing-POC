#!/usr/bin/env bash
set -euo pipefail

SOURCE_CTX="${1:-minikube-a}"

decode_base64() {
  if base64 --decode >/dev/null 2>&1 </dev/null; then
    base64 --decode
  else
    base64 -D
  fi
}

escape_sed() {
  printf '%s' "$1" | sed -e 's/[&|]/\\&/g'
}

delete_strimzi_rolebindings() {
  local ns
  for ns in $(kubectl --context "$SOURCE_CTX" get ns -o jsonpath='{.items[*].metadata.name}'); do
    kubectl --context "$SOURCE_CTX" -n "$ns" delete rolebinding \
      strimzi-cluster-operator-watched \
      strimzi-cluster-operator \
      strimzi-cluster-operator-entity-operator-delegation >/dev/null 2>&1 || true
  done
}
#
kubectl --context "$SOURCE_CTX" apply -f poc/00-namespaces.yaml

helm repo add zalando https://opensource.zalando.com/postgres-operator/charts/postgres-operator >/dev/null 2>&1 || true
helm repo add apisix https://charts.apiseven.com >/dev/null 2>&1 || true
helm repo update >/dev/null

helm upgrade --install postgres-operator zalando/postgres-operator \
  -n database --kube-context "$SOURCE_CTX" --create-namespace

kubectl --context "$SOURCE_CTX" apply -f poc/postgres/source-postgres.yaml

helm upgrade --install apisix apisix/apisix \
  -n gateway --kube-context "$SOURCE_CTX" -f poc/apisix/values.yaml

delete_strimzi_rolebindings

helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  -n messaging --kube-context "$SOURCE_CTX" \
  --set watchNamespaces="{messaging}" \
  --set installCRDs=true

kubectl --context "$SOURCE_CTX" apply -f poc/kafka/kafka-source.yaml
kubectl --context "$SOURCE_CTX" apply -f poc/kafka/kafka-source-nodepool.yaml
kubectl --context "$SOURCE_CTX" apply -f poc/kafka/topics.yaml
kubectl --context "$SOURCE_CTX" apply -f poc/connect/kafka-connect-debezium.yaml

for _ in {1..60}; do
  if kubectl --context "$SOURCE_CTX" -n database get secret \
    postgres.source-postgres.credentials.postgresql.acid.zalan.do >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

SOURCE_DB_PASSWORD=$(kubectl --context "$SOURCE_CTX" -n database get secret \
  postgres.source-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | decode_base64)

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT
SOURCE_DB_PASSWORD_ESCAPED=$(escape_sed "$SOURCE_DB_PASSWORD")
sed "s|SOURCE_DB_PASSWORD|$SOURCE_DB_PASSWORD_ESCAPED|g" \
  poc/connect/debezium-connector.yaml > "$tmpfile"
kubectl --context "$SOURCE_CTX" apply -f "$tmpfile"
