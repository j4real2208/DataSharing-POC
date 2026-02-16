#!/usr/bin/env bash
set -euo pipefail

SINK_CTX="${1:-minikube-b}"
SOURCE_CTX="${2:-minikube-a}"
CONNECT_IMAGE="${3:-local/kafka-connect:3.4.1}"
MINIKUBE_SINK_IP="$(minikube -p "$SINK_CTX" ip)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

wait_for_deployment_rollout() {
  local ctx="$1"
  local ns="$2"
  local deploy="$3"
  local timeout="${4:-300s}"
  kubectl --context "$ctx" -n "$ns" rollout status "deployment/$deploy" --timeout="$timeout"
}

wait_for_secret() {
  local ctx="$1"
  local ns="$2"
  local secret_name="$3"
  local retries="${4:-60}"
  local sleep_seconds="${5:-5}"

  for _ in $(seq 1 "$retries"); do
    if kubectl --context "$ctx" -n "$ns" get secret "$secret_name" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "Timed out waiting for secret '$secret_name' in namespace '$ns' on context '$ctx'." >&2
  return 1
}

wait_for_sink_postgres_pod_ready() {
  local pod_name=""

  echo "Waiting for sink Postgres pod to be created..."
  for _ in $(seq 1 120); do
    pod_name="$(kubectl --context "$SINK_CTX" -n database get pods \
      -l cluster-name=sink-postgres \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"

    if [ -n "$pod_name" ]; then
      echo "Waiting for Postgres pod '$pod_name' to become Ready..."
      kubectl --context "$SINK_CTX" -n database wait \
        --for=condition=Ready "pod/$pod_name" --timeout=600s
      return 0
    fi

    sleep 5
  done

  echo "Timed out waiting for sink Postgres pod (label cluster-name=sink-postgres)." >&2
  kubectl --context "$SINK_CTX" -n database get pods || true
  return 1
}

wait_for_kafka_ready() {
  kubectl --context "$SINK_CTX" -n messaging wait \
    --for=condition=Ready kafka/sink-kafka --timeout=600s
}

wait_for_kafkaconnect_ready() {
  kubectl --context "$SINK_CTX" -n messaging wait \
    --for=condition=Ready kafkaconnect/sink-connect --timeout=600s
}

wait_for_kafkaconnector_ready() {
  kubectl --context "$SINK_CTX" -n messaging wait \
    --for=condition=Ready kafkaconnector/sink-jdbc-connector --timeout=600s
}

wait_for_mirrormaker_ready() {
  kubectl --context "$SOURCE_CTX" -n messaging wait \
    --for=condition=Ready kafkamirrormaker2/source-to-sink --timeout=600s
}

ensure_minikube_shared_network() {
  local network_name="minikube-shared"
  local network_subnet="172.30.0.0/16"

  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required for network preflight ($network_name)." >&2
    return 1
  fi

  if docker network inspect "$network_name" >/dev/null 2>&1; then
    echo "Docker network '$network_name' already exists."
    return 0
  fi

  echo "Creating docker network '$network_name' with subnet '$network_subnet'..."
  docker network create --subnet="$network_subnet" "$network_name" >/dev/null
}

ensure_connect_image_available_and_loaded() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to check/build Kafka Connect image '$CONNECT_IMAGE'." >&2
    return 1
  fi

  if docker image inspect "$CONNECT_IMAGE" >/dev/null 2>&1; then
    echo "Kafka Connect image '$CONNECT_IMAGE' found in local Docker."
  else
    echo "Kafka Connect image '$CONNECT_IMAGE' not found locally. Building image..."
    "$SCRIPT_DIR/build-connect-image.sh" "$SINK_CTX" "$CONNECT_IMAGE"
  fi

  echo "Loading Kafka Connect image '$CONNECT_IMAGE' into '$SOURCE_CTX' and '$SINK_CTX'..."
  minikube -p "$SOURCE_CTX" image load "$CONNECT_IMAGE"
  minikube -p "$SINK_CTX" image load "$CONNECT_IMAGE"
}

deploy_and_check_apicurio_registry() {
  local apicurio_url
  local kafka_pod

  kubectl --context "$SINK_CTX" create namespace messaging --dry-run=client -o yaml | \
    kubectl --context "$SINK_CTX" apply -f -

  kubectl --context "$SINK_CTX" apply -f poc/apicurio/apicurio-registry.yaml
  wait_for_deployment_rollout "$SINK_CTX" messaging apicurio-registry 300s

  apicurio_url="http://${MINIKUBE_SINK_IP}:32080/apis/registry/v2/system/info"
  echo "Checking Apicurio endpoint from sink Kafka pod: $apicurio_url"

  kafka_pod="$(kubectl --context "$SINK_CTX" -n messaging get pods \
    -l strimzi.io/cluster=sink-kafka,strimzi.io/name=sink-kafka-kafka \
    -o jsonpath='{.items[0].metadata.name}')"

  if [ -z "$kafka_pod" ]; then
    echo "Could not find sink Kafka pod to run Apicurio reachability check." >&2
    return 1
  fi

  for _ in {1..30}; do
    if kubectl --context "$SINK_CTX" -n messaging exec "$kafka_pod" -- /bin/bash -ec "
      if command -v curl >/dev/null 2>&1; then
        curl -fsS '$apicurio_url' >/dev/null
      elif command -v wget >/dev/null 2>&1; then
        wget -qO- '$apicurio_url' >/dev/null
      else
        echo 'curl/wget not available in Kafka pod image' >&2
        exit 1
      fi
    " >/dev/null 2>&1; then
      echo "Apicurio endpoint reachable from pod '$kafka_pod'."
      return 0
    fi
    sleep 2
  done

  echo "Apicurio endpoint not reachable from pod '$kafka_pod': $apicurio_url" >&2
  return 1
}

startup_message_and_execute() {
  echo "Deploying sink cluster components to context '$SINK_CTX'..."
  kubectl --context "$SINK_CTX" apply -f poc/00-namespaces.yaml

  helm repo add zalando https://opensource.zalando.com/postgres-operator/charts/postgres-operator >/dev/null 2>&1 || true
  helm repo add apisix https://charts.apiseven.com >/dev/null 2>&1 || true
  helm repo update >/dev/null

  helm upgrade --install postgres-operator zalando/postgres-operator \
    -n database --kube-context "$SINK_CTX" --create-namespace
  wait_for_deployment_rollout "$SINK_CTX" database postgres-operator 300s

  kubectl --context "$SINK_CTX" apply -f poc/postgres/sink-postgres.yaml
  wait_for_sink_postgres_pod_ready

  helm upgrade --install apisix apisix/apisix \
    -n gateway --kube-context "$SINK_CTX" -f poc/apisix/values.yaml

  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
    -n messaging --kube-context "$SINK_CTX" \
    --set watchNamespaces="{messaging}" \
    --set installCRDs=true
  wait_for_deployment_rollout "$SINK_CTX" messaging strimzi-cluster-operator 300s
}

deploy_mirrormaker_and_wait() {
  local sink_bootstrap
  local sink_port

  sink_port="$(kubectl --context "$SINK_CTX" -n messaging get svc sink-kafka-kafka-external-bootstrap -o jsonpath='{.spec.ports[0].nodePort}')"
  sink_bootstrap="${MINIKUBE_SINK_IP}:${sink_port}"

  echo "Deploying MirrorMaker2 from '$SOURCE_CTX' to sink bootstrap '$sink_bootstrap'..."
  "$SCRIPT_DIR/deploy-mirrormaker.sh" "$SOURCE_CTX" "$sink_bootstrap"
  wait_for_mirrormaker_ready
}

print_sink_topic_messages() {
  local sink_connect_pod

  sink_connect_pod="$(kubectl --context "$SINK_CTX" -n messaging get pods \
    -l strimzi.io/cluster=sink-connect,strimzi.io/kind=KafkaConnect \
    -o jsonpath='{.items[0].metadata.name}')"

  if [ -z "$sink_connect_pod" ]; then
    echo "Could not find sink Kafka Connect pod to consume mirrored messages." >&2
    return 1
  fi

  echo "Consuming mirrored Avro messages from sink topic 'source.source.public.weather_readings'..."
  kubectl --context "$SINK_CTX" -n messaging exec "$sink_connect_pod" -- /bin/bash -lc '
CLASSPATH="/opt/kafka/libs/*:/opt/kafka/plugins/apicurio-converters/*" \
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server sink-kafka-kafka-bootstrap:9092 \
  --topic source.source.public.weather_readings \
  --from-beginning \
  --group debug-sink-avro-$(date +%s) \
  --skip-message-on-error \
  --timeout-ms 30000 \
  --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer \
  --property value.deserializer.apicurio.registry.url=http://'"$MINIKUBE_SINK_IP"':32080/apis/registry/v2 \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=" | "
'
}

validate_sink_weather_table() {
  local sink_db_password
  local sink_postgres_pod
  local rows

  sink_db_password="$(kubectl --context "$SINK_CTX" -n database get secret \
    sink-user.sink-postgres.credentials.postgresql.acid.zalan.do \
    -o jsonpath='{.data.password}' | decode_base64)"

  sink_postgres_pod="$(kubectl --context "$SINK_CTX" -n database get pods \
    -l cluster-name=sink-postgres \
    -o jsonpath='{.items[0].metadata.name}')"

  if [ -z "$sink_postgres_pod" ]; then
    echo "Could not find sink Postgres pod to validate table data." >&2
    return 1
  fi

  echo "Waiting for data in sink table public.weather_readings..."
  for _ in $(seq 1 30); do
    rows="$(kubectl --context "$SINK_CTX" -n database exec "$sink_postgres_pod" -- /bin/bash -ec "
      export PGPASSWORD='$sink_db_password'
      psql -h localhost -U sink-user -d sink_db -tAc \"SELECT COUNT(*) FROM public.weather_readings;\"
    " 2>/dev/null | tr -d '[:space:]' || true)"

    if [ -n "$rows" ] && [ "$rows" -gt 0 ] 2>/dev/null; then
      echo "Sink table has $rows row(s)."
      kubectl --context "$SINK_CTX" -n database exec "$sink_postgres_pod" -- /bin/bash -ec "
        export PGPASSWORD='$sink_db_password'
        psql -h localhost -U sink-user -d sink_db \
          -P border=2 -P format=aligned \
          -c \"SELECT id, city, temperature_c, observed_at FROM public.weather_readings ORDER BY id;\"
      "
      return 0
    fi

    sleep 5
  done

  echo "No rows found in sink table public.weather_readings after waiting." >&2
  return 1
}


ensure_minikube_shared_network
ensure_connect_image_available_and_loaded
startup_message_and_execute

kubectl --context "$SINK_CTX" apply -f poc/kafka/kafka-sink.yaml
kubectl --context "$SINK_CTX" apply -f poc/kafka/kafka-sink-nodepool.yaml
wait_for_kafka_ready

deploy_and_check_apicurio_registry

sed "s|<MINIKUBE_SINK>|$MINIKUBE_SINK_IP|g" poc/connect/kafka-connect-jdbc-sink.yaml \
  | kubectl --context "$SINK_CTX" apply -f -
wait_for_kafkaconnect_ready

wait_for_secret "$SINK_CTX" database sink-user.sink-postgres.credentials.postgresql.acid.zalan.do 60 5

SINK_DB_PASSWORD=$(kubectl --context "$SINK_CTX" -n database get secret \
  sink-user.sink-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | decode_base64)

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT
SINK_DB_PASSWORD_ESCAPED=$(escape_sed "$SINK_DB_PASSWORD")
sed "s|SINK_DB_PASSWORD|$SINK_DB_PASSWORD_ESCAPED|g" \
  poc/connect/jdbc-sink-connector.yaml > "$tmpfile"
kubectl --context "$SINK_CTX" apply -f "$tmpfile"
wait_for_kafkaconnector_ready

deploy_mirrormaker_and_wait
print_sink_topic_messages
validate_sink_weather_table
