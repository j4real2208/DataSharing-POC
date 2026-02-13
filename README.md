# Two-Cluster Data Sharing POC (Source -> Sink)

This POC keeps only the essentials:
- Postgres (Zalando operator) in each cluster
- APISIX in each cluster
- Kafka (Strimzi, KRaft mode) in each cluster
- MirrorMaker2 to replicate topics from source to sink
- Debezium on source and Debezium JDBC sink on sink to move data from Postgres -> Kafka -> Postgres

## Architecture
```
                         Minikube A (source)
        ┌─────────────────────────────────────────────┐
        │                                             │
        │  Postgres (source_db)                       │
        │        │                                    │
        │        │ Debezium CDC                        │
        │        ▼                                    │
        │  Kafka (source-kafka)                       │
        │        │                                    │
        │        │ MirrorMaker2                        │
        │        ▼                                    │
        │  Kafka (sink-kafka)  ───────────────────────────────┐
        │                                             │       │
        └─────────────────────────────────────────────┘       │
                                                              │
                                      Minikube B (sink)        │
        ┌─────────────────────────────────────────────┐       │
        │                                             │       │
        │  Kafka (sink-kafka) <───────────────────────────────┘
        │        │                                    │
        │        │ JDBC Sink                           │
        │        ▼                                    │
        │  Postgres (sink_db)                         │
        │                                             │
        └─────────────────────────────────────────────┘

        APISIX runs in both clusters (gateway namespace).
```

## Repository layout
```
.
├── README.md
├── connect-image/
│   └── Dockerfile
├── poc/
│   ├── 00-namespaces.yaml
│   ├── apisix/
│   │   └── values.yaml
│   ├── connect/
│   │   ├── debezium-connector.yaml
│   │   ├── jdbc-sink-connector.yaml
│   │   ├── kafka-connect-debezium.yaml
│   │   └── kafka-connect-jdbc-sink.yaml
│   ├── kafka/
│   │   ├── kafka-source.yaml
│   │   ├── kafka-source-nodepool.yaml
│   │   ├── kafka-sink.yaml
│   │   ├── kafka-sink-nodepool.yaml
│   │   ├── mirrormaker2-source-to-sink.yaml
│   │   └── topics.yaml
│   └── postgres/
│       ├── source-postgres.yaml
│       └── sink-postgres.yaml
└── scripts/
    ├── build-connect-image.sh
    ├── deploy-mirrormaker.sh
    ├── deploy-source.sh
    ├── deploy-sink.sh
    ├── refresh-strimzi-crds.sh
    ├── test-debezium.sh
    ├── verify.sh
    └── poc/
        └── 00-namespaces.yaml
```

## 1) Prereqs
- `minikube`, `kubectl`, `helm` installed
- Two minikube profiles: `minikube-a` (source) and `minikube-b` (sink)
- If using the Docker driver, create a shared network for the clusters:
  ```bash
  docker network create --subnet=172.30.0.0/16 minikube-shared
  ```

## 2) Start clusters
```bash
minikube start -p minikube-a --cpus=3 --memory=6144 --kubernetes-version=v1.29.0 --network minikube-shared
minikube start -p minikube-b --cpus=3 --memory=6144 --kubernetes-version=v1.29.0 --network minikube-shared
```

## 3) Deploy source and sink
Build the Kafka Connect image (includes Debezium + JDBC connectors) and load it into each cluster:
```bash
./scripts/build-connect-image.sh minikube-a
./scripts/build-connect-image.sh minikube-b
```

```bash
./scripts/deploy-source.sh minikube-a
./scripts/deploy-sink.sh minikube-b
```

If you see errors like `unknown field "spec.kafka.kraft"` your Strimzi CRDs are too old for KRaft.
Refresh the CRDs (this deletes existing Strimzi resources) and re-run deploy:
```bash
CONFIRM=YES ./scripts/refresh-strimzi-crds.sh minikube-a
CONFIRM=YES ./scripts/refresh-strimzi-crds.sh minikube-b
```

## 4) Configure MirrorMaker2
Get the sink bootstrap address (NodePort on the sink cluster):
```bash
SINK_IP=$(minikube -p minikube-b ip)
SINK_PORT=$(kubectl --context minikube-b -n messaging get svc sink-kafka-kafka-external-bootstrap -o jsonpath='{.spec.ports[0].nodePort}')

echo "$SINK_IP:$SINK_PORT"
```
Deploy MirrorMaker2 on the source cluster:
```bash
./scripts/deploy-mirrormaker.sh minikube-a "$SINK_IP:$SINK_PORT"
```

## 5) Create source table and insert data
Create a table in source Postgres and insert a few rows.
```bash
SOURCE_DB_PASSWORD=$(kubectl --context minikube-a -n database get secret \
  postgres.source-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | base64 -D)

kubectl --context minikube-a -n database run -it --rm psql \
  --image=postgres:15 --restart=Never \
  --env="PGPASSWORD=$SOURCE_DB_PASSWORD" -- \
  psql -h source-postgres -U postgres -d source_db <<'SQL'
CREATE TABLE IF NOT EXISTS public.weather_readings (
  id serial PRIMARY KEY,
  city text NOT NULL,
  temperature_c numeric(5,2) NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.weather_readings (city, temperature_c) VALUES
  ('Seattle', 14.3),
  ('Austin', 27.8),
  ('Chicago', 9.1);
SQL
```

Debezium will emit CDC events into Kafka (topics with prefix `source`).

## 6) Verify data landed in sink Postgres
```bash
SINK_DB_PASSWORD=$(kubectl --context minikube-b -n database get secret \
  sink-user.sink-postgres.credentials.postgresql.acid.zalan.do \
  -o jsonpath='{.data.password}' | base64 -D)

kubectl --context minikube-b -n database run -it --rm psql \
  --image=postgres:15 --restart=Never \
  --env="PGPASSWORD=$SINK_DB_PASSWORD" -- \
  psql -h sink-postgres -U sink-user -d sink_db -c "SELECT * FROM public.weather_readings;"
```

## Notes
- Update Kafka Connect images if your environment requires custom connector bundles:
  - `poc/connect/kafka-connect-debezium.yaml`
  - `poc/connect/kafka-connect-jdbc-sink.yaml`
- APISIX is deployed but not required in the data path for this POC.
- On Linux, replace `base64 -D` with `base64 -d` in the commands above.

## Sample CDC event (Debezium)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "city": "Testville",
      "temperature_c": "BM4=",
      "observed_at": "2026-02-13T13:44:02.234712Z"
    },
    "source": {
      "connector": "postgresql",
      "name": "source",
      "db": "source_db",
      "schema": "public",
      "table": "weather_readings"
    },
    "op": "c",
    "ts_ms": 1770990242726
  }
}
```
`temperature_c` uses the Kafka Connect Decimal logical type and is base64-encoded.
