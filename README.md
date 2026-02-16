# Two-Cluster Data Sharing POC (Source -> Sink)

This POC keeps only the essentials:
- Postgres (Zalando operator) in each cluster
- APISIX in each cluster
- Kafka (Strimzi, KRaft mode) in each cluster
- MirrorMaker2 to replicate topics from source to sink
- Kafka Connect with Debezium source on Minikube A and JDBC sink on Minikube B
- Apicurio Registry for Avro schema and serialization/deserialization

## Architecture
```
                         Minikube A (source)
        ┌─────────────────────────────────────────────┐
        │                                             │
        │  Postgres (source_db)                       │
        │        │                                    │
        │        │ Debezium CDC (Kafka Connect + Avro)│
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
        │        │ JDBC Sink (Kafka Connect)          │
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
│   ├── apicurio/
│   │   └── apicurio-registry.yaml
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
│   ├── postgres/
│   │   ├── init-weather-readings-job.yaml
│   │   ├── source-postgres.yaml
│   │   └── sink-postgres.yaml
│   └── redpanda/
│       └── redpanda-console.yaml
└── scripts/
    ├── build-connect-image.sh
    ├── deploy-mirrormaker.sh
    ├── deploy-source.sh
    ├── deploy-sink.sh
    ├── refresh-strimzi-crds.sh
    ├── test-debezium.sh
    └── verify.sh
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

### Option B: run the Kubernetes init job
`./scripts/deploy-source.sh minikube-a` now runs this job automatically.  
Use these commands to run/re-run it manually.
This uses `poc/postgres/init-weather-readings-job.yaml` and runs the SQL from inside the cluster against `source-postgres`.
```bash
kubectl --context=minikube-a apply -f poc/postgres/init-weather-readings-job.yaml
kubectl --context=minikube-a -n database logs -f job/init-weather-readings
```

Re-run the job:
```bash
kubectl --context=minikube-a -n database delete job init-weather-readings
kubectl --context=minikube-a apply -f poc/postgres/init-weather-readings-job.yaml
```

### Option C: read decoded CDC messages from the existing source Connect pod
Use the running Kafka Connect pod (`source-connect`) to consume and print decoded Avro messages from `source.public.weather_readings`.
```bash
CONNECT_POD=$(kubectl --context=minikube-a -n messaging get pods \
  -l strimzi.io/cluster=source-connect,strimzi.io/kind=KafkaConnect \
  -o jsonpath='{.items[0].metadata.name}')

kubectl --context=minikube-a -n messaging exec "$CONNECT_POD" -- /bin/bash -lc '
CLASSPATH="/opt/kafka/libs/*:/opt/kafka/plugins/apicurio-converters/*" \
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server source-kafka-kafka-bootstrap:9092 \
  --topic source.public.weather_readings \
  --from-beginning \
  --group debug-avro-all-$(date +%s) \
  --skip-message-on-error \
  --timeout-ms 30000 \
  --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer \
  --property value.deserializer.apicurio.registry.url=http://172.17.0.3:32080/apis/registry/v2 \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=" | "
'
```
You should see rows printed as key/value records (or no output if the topic has no matching messages yet).

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

## 7) Redpanda Console + Apicurio Registry (optional)
Deploy Apicurio Registry (with PVC) plus Redpanda Console:
```bash
kubectl --context minikube-b -n messaging apply -f poc/apicurio/apicurio-registry.yaml
kubectl --context minikube-b -n messaging apply -f poc/redpanda/redpanda-console.yaml
```

Open the Console UI and Registry locally:
```bash
kubectl --context minikube-b -n messaging port-forward svc/redpanda-console 8080:8080
kubectl --context minikube-b -n messaging port-forward svc/apicurio-registry 8081:8080
```

Console is configured to read from `sink-kafka-kafka-bootstrap:9092` and the registry at `http://apicurio-registry:8080`.
Kafka Connect workers are configured to use Apicurio with the Avro converter; rebuild and reload the Connect image after changes:
```bash
./scripts/build-connect-image.sh minikube-a
./scripts/build-connect-image.sh minikube-b
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
