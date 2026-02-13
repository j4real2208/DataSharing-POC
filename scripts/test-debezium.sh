#!/usr/bin/env bash
set -euo pipefail

CTX="${1:-minikube-a}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required" >&2
  exit 1
fi

decode_base64() {
  if base64 --decode >/dev/null 2>&1 </dev/null; then
    base64 --decode
  else
    base64 -D
  fi
}

if ! kubectl --context "$CTX" -n messaging get kafkaconnector source-postgres-connector >/dev/null 2>&1; then
  echo "KafkaConnector source-postgres-connector not found in $CTX" >&2
  exit 1
fi

echo "Connector status:"
kubectl --context "$CTX" -n messaging get kafkaconnector source-postgres-connector

SOURCE_DB_PASSWORD=$(kubectl --context "$CTX" -n database get secret \
  postgres.source-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | decode_base64)

kubectl --context "$CTX" -n database run -it --rm psql \
  --image=postgres:15 --restart=Never \
  --env="PGPASSWORD=$SOURCE_DB_PASSWORD" -- \
  psql -h source-postgres -U postgres -d source_db <<'SQL'
CREATE TABLE IF NOT EXISTS public.weather_readings (
  id serial PRIMARY KEY,
  city text NOT NULL,
  temperature_c numeric(5,2) NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville', 12.3);
SQL

echo "Waiting for CDC topic to receive at least one event..."

kubectl --context "$CTX" -n messaging exec -it source-kafka-dual-role-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic source.public.weather_readings --from-beginning --max-messages 1
