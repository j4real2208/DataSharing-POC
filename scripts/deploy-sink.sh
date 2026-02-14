#!/usr/bin/env bash
set -euo pipefail

SINK_CTX="${1:-minikube-b}"

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
  for ns in $(kubectl --context "$SINK_CTX" get ns -o jsonpath='{.items[*].metadata.name}'); do
    kubectl --context "$SINK_CTX" -n "$ns" delete rolebinding \
      strimzi-cluster-operator-watched \
      strimzi-cluster-operator \
      strimzi-cluster-operator-entity-operator-delegation >/dev/null 2>&1 || true
  done
}


startup_message_and_execute() {
  echo "Deploying sink cluster components to context '$SINK_CTX'..."
  kubectl --context "$SINK_CTX" apply -f poc/00-namespaces.yaml

  helm repo add zalando https://opensource.zalando.com/postgres-operator/charts/postgres-operator >/dev/null 2>&1 || true
  helm repo add apisix https://charts.apiseven.com >/dev/null 2>&1 || true
  helm repo update >/dev/null

  helm upgrade --install postgres-operator zalando/postgres-operator \
    -n database --kube-context "$SINK_CTX" --create-namespace

  kubectl --context "$SINK_CTX" apply -f poc/postgres/sink-postgres.yaml

  helm upgrade --install apisix apisix/apisix \
    -n gateway --kube-context "$SINK_CTX" -f poc/apisix/values.yaml

  delete_strimzi_rolebindings

  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
    -n messaging --kube-context "$SINK_CTX" \
    --set watchNamespaces="{messaging}" \
    --set installCRDs=true

}

startup_message_and_execute
exit

kubectl --context "$SINK_CTX" apply -f poc/kafka/kafka-sink.yaml
kubectl --context "$SINK_CTX" apply -f poc/kafka/kafka-sink-nodepool.yaml
MINIKUBE_SINK_IP="$(minikube -p minikube-b ip)"
sed "s|<MINIKUBE_SINK>|$MINIKUBE_SINK_IP|g" poc/connect/kafka-connect-jdbc-sink.yaml \
  | kubectl --context "$SINK_CTX" apply -f -

for _ in {1..60}; do
  if kubectl --context "$SINK_CTX" -n database get secret \
    sink-user.sink-postgres.credentials.postgresql.acid.zalan.do >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

SINK_DB_PASSWORD=$(kubectl --context "$SINK_CTX" -n database get secret \
  sink-user.sink-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | decode_base64)

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT
SINK_DB_PASSWORD_ESCAPED=$(escape_sed "$SINK_DB_PASSWORD")
sed "s|SINK_DB_PASSWORD|$SINK_DB_PASSWORD_ESCAPED|g" \
  poc/connect/jdbc-sink-connector.yaml > "$tmpfile"
kubectl --context "$SINK_CTX" apply -f "$tmpfile"
