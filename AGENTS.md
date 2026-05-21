# AGENTS Guide: Two-Cluster CDC POC

## What this repo does
- This is a two-cluster Minikube POC: CDC from source Postgres -> source Kafka -> MirrorMaker2 -> sink Kafka -> JDBC sink -> sink Postgres.
- Core flow is defined across `poc/connect/debezium-connector.yaml`, `poc/kafka/mirrormaker2-source-to-sink.yaml`, and `poc/connect/jdbc-sink-connector.yaml`.
- APISIX is deployed (`poc/apisix/values.yaml`) but is not in the CDC data path.

## Service boundaries and ownership
- `poc/postgres/*`: Zalando Postgres clusters plus seed job (`init-weather-readings-job.yaml`).
- `poc/kafka/*`: Strimzi Kafka clusters, node pools, topics, MirrorMaker2.
- `poc/connect/*`: KafkaConnect workers and connector CRs for Debezium + JDBC sink.
- `poc/apicurio/*` and `poc/redpanda/*`: schema registry + UI on sink side.
- `scripts/*`: the real operational entrypoints; prefer scripts over raw `kubectl apply` for full bring-up.

## Critical workflow (preferred bring-up)
- Source deploy: `./scripts/deploy-source.sh minikube-a minikube-b local/kafka-connect:3.4.1`
- Sink deploy (also deploys MM2, Redpanda Console, Apicurio Registry, and validation): `./scripts/deploy-sink.sh minikube-b minikube-a local/kafka-connect:3.4.1`
- These scripts include readiness gates (`kubectl wait`/rollout checks), secret waits, and end-to-end validation.
- If Strimzi CRD schema errors appear (for example around KRaft fields), run `CONFIRM=YES ./scripts/refresh-strimzi-crds.sh <context>` then redeploy.

## Project-specific config patterns
- Manifests are templates with placeholders replaced at deploy time:
  - `<MINIKUBE_SINK>` in `poc/connect/kafka-connect-*.yaml` and `poc/redpanda/redpanda-console.yaml`
  - `SOURCE_DB_PASSWORD` / `SINK_DB_PASSWORD` in connector CRs
  - `SINK_BOOTSTRAP` in MirrorMaker2 manifest
- Scripts render templates with `sed` + temporary files; preserve this pattern when adding new env-specific values.
- Base64 decode is cross-platform: use `base64 --decode` on Linux, `base64 -D` on macOS. Scripts include helper functions for both.

### Kubernetes resource naming and selectors
- **Namespaces**: `database` (Postgres), `messaging` (Kafka/Connect), `gateway` (APISIX).
- **Postgres pods**: Use selector `cluster-name=source-postgres` (or `sink-postgres`).
- **Kafka pods**: Use selector `strimzi.io/cluster=source-kafka,strimzi.io/name=source-kafka-kafka`.
- **Connect pods**: Use selector `strimzi.io/cluster=source-connect,strimzi.io/kind=KafkaConnect`.
- **Secrets** (Zalando operator): `postgres.source-postgres.credentials.postgresql.acid.zalan.do` (password in `.data.password` field).
- **KafkaConnector resources**: `source-postgres-connector` (source), `sink-jdbc-connector` (sink).
- **KafkaMirrorMaker2 resource**: `source-to-sink`.

### Helm repositories
- `zalando` (postgres-operator): `https://opensource.zalando.com/postgres-operator/charts/postgres-operator`
- `apisix`: `https://charts.apiseven.com`
- `strimzi`: OCI chart `oci://quay.io/strimzi-helm/strimzi-kafka-operator` (use `installCRDs=true`)

### Script parameter conventions
- All scripts take kubectl context names as positional arguments in order: `SOURCE_CTX`, `SINK_CTX`, `CONNECT_IMAGE`.
- Use `--context <name>` explicitly in all `kubectl` calls for multi-cluster operations.
- Scripts use trap + temp files for template rendering (e.g., `tmpfile="$(mktemp)"` + `trap 'rm -f "$tmpfile"' EXIT`).

## Integration contracts that must stay aligned
- Debezium emits with `topic.prefix: source` and `table.include.list: public.weather_readings`.
- MirrorMaker2 uses `topicsPattern: ".*"`; sink sees mirrored topic `source.source.public.weather_readings`.
- JDBC sink consumes `source.source.public.weather_readings` and writes to `public.weather_readings` in `sink_db`.
- Both Connect clusters use Apicurio Avro converters and must resolve registry at `http://<MINIKUBE_SINK>:32080/apis/registry/v2`.

### Critical version alignment
- **Dockerfile versions**: `DEBEZIUM_VERSION=3.4.1.Final`, `APICURIO_VERSION=3.1.0`, `STRIMZI_VERSION=0.50.0`, `KAFKA_VERSION=4.1.1`.
- **build-connect-image.sh versions**: `DEBEZIUM_VERSION=3.4.1.Final`, `APICURIO_VERSION=3.1.7` (note: different from Dockerfile; be careful when updating).
- When updating connector or plugin versions in `connect-image/Dockerfile`, also update the corresponding version in `scripts/build-connect-image.sh` line 18-19.

## Build/debug commands agents should use
- Rebuild/load Connect image: `./scripts/build-connect-image.sh minikube-a` and `./scripts/build-connect-image.sh minikube-b`
- Quick resource sanity: `./scripts/verify.sh minikube-a` (or `minikube-b`)
- Debezium smoke check: `./scripts/test-debezium.sh minikube-a`
- For data-path debugging, follow script logic: consume from Connect pod, then query sink Postgres row count.
- Manual MirrorMaker2 deployment: `./scripts/deploy-mirrormaker.sh <source-context> <sink-bootstrap>`.

### Common debugging patterns
- **Extract secrets and connect to Postgres**: Decode Zalando secret, then run `psql` pod exec:
  ```bash
  SECRET_NAME="postgres.source-postgres.credentials.postgresql.acid.zalan.do"
  PASSWORD=$(kubectl --context=minikube-a -n database get secret "$SECRET_NAME" \
    -o jsonpath='{.data.password}' | base64 -D)  # use base64 --decode on Linux
  kubectl --context=minikube-a -n database run -it --rm psql \
    --image=postgres:15 --restart=Never --env="PGPASSWORD=$PASSWORD" -- \
    psql -h source-postgres -U postgres -d source_db -c "SELECT * FROM public.weather_readings;"
  ```
- **Check pod readiness by resource kind**: Use `kubectl wait --for=condition=Ready <kind>/<name> --timeout=600s` (e.g., `kafka/source-kafka`, `kafkaconnect/source-connect`, `kafkaconnector/source-postgres-connector`).
- **Find pod by label and exec**: 
  ```bash
  POD=$(kubectl --context=minikube-a -n messaging get pods \
    -l strimzi.io/cluster=source-kafka,strimzi.io/name=source-kafka-kafka \
    -o jsonpath='{.items[0].metadata.name}')
  kubectl --context=minikube-a -n messaging exec "$POD" -- /bin/bash
  ```
- **Debug timezone issues**: CDC timestamps use UTC; ensure receiver expects UTC or correct in connectors.

## Safe change rules for this repo
- Keep resource names stable (`source-connect`, `sink-connect`, `source-to-sink`, `source-postgres-connector`, `sink-jdbc-connector`) because scripts select by fixed names/labels.
- If you change topic/table naming, update all three layers together: Debezium connector (`topic.prefix`, `table.include.list`), MirrorMaker2 (`topicsPattern`), JDBC sink connector (input topic).
- If you change connector/plugin versions:
  - Update `connect-image/Dockerfile` ARG values (DEBEZIUM_VERSION, APICURIO_VERSION, etc.)
  - Check `scripts/build-connect-image.sh` lines 18-19 and ensure versions match or are intentionally different.
  - Rebuild and test both clusters: `./scripts/build-connect-image.sh minikube-a && ./scripts/build-connect-image.sh minikube-b`
- Prefer extending `deploy-source.sh` / `deploy-sink.sh` validation gates over adding ad-hoc manual steps.
- Ensure Docker network preflight (`ensure_minikube_shared_network`) is called before multi-cluster deploy if using Docker driver.
- **Do not rename namespaces** (`database`, `messaging`, `gateway`) without updating all three deployment scripts and manifests.
- **Do not change Postgres secret naming pattern** without updating all scripts that decode them (use grep to find all uses of `.credentials.postgresql`).

## Automated validation steps
- `deploy-sink.sh` performs the following automatically:
  - Deploys and validates Redpanda Console and Apicurio Registry.
  - Deploys MirrorMaker2 and waits for readiness.
  - Consumes mirrored Avro messages from `source.source.public.weather_readings`.
  - Validates data in `sink_db.public.weather_readings` by querying the sink Postgres pod.

## Known issues and troubleshooting
- **Strimzi CRD schema errors** (e.g., "unknown field spec.kafka.kraft"): CRDs are out of date. Run `CONFIRM=YES ./scripts/refresh-strimzi-crds.sh minikube-a` and `CONFIRM=YES ./scripts/refresh-strimzi-crds.sh minikube-b`, then redeploy.
- **Docker network failures on multi-cluster setups**: If clusters cannot reach each other, run `docker network create --subnet=172.30.0.0/16 minikube-shared` (scripts do this, but failing silently is possible).
- **Apicurio registry unreachable from Connect pods**: Ensure registry is deployed and its nodePort 32080 is accessible. Check: `kubectl --context minikube-b -n messaging get svc apicurio-registry`.
- **Kafka consumer group rebalancing**: If testing consumer groups, use unique `--group` names to avoid rebalancing with other consumers.
- **Decimal type encoding in messages**: `temperature_c` uses Kafka Decimal logical type (base64-encoded in raw output). Apicurio Avro deserializer decodes it automatically.

