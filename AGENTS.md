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
- Sink deploy (also deploys MM2 and validation): `./scripts/deploy-sink.sh minikube-b minikube-a local/kafka-connect:3.4.1`
- These scripts include readiness gates (`kubectl wait`/rollout checks), secret waits, and end-to-end validation.
- If Strimzi CRD schema errors appear (for example around KRaft fields), run `CONFIRM=YES ./scripts/refresh-strimzi-crds.sh <context>` then redeploy.

## Project-specific config patterns
- Manifests are templates with placeholders replaced at deploy time:
  - `<MINIKUBE_SINK>` in `poc/connect/kafka-connect-*.yaml` and `poc/redpanda/redpanda-console.yaml`
  - `SOURCE_DB_PASSWORD` / `SINK_DB_PASSWORD` in connector CRs
  - `SINK_BOOTSTRAP` in MirrorMaker2 manifest
- Scripts render templates with `sed` + temporary files; preserve this pattern when adding new env-specific values.
- Base64 decode is cross-platform via helper functions (`base64 --decode` fallback to `base64 -D`).

## Integration contracts that must stay aligned
- Debezium emits with `topic.prefix: source` and `table.include.list: public.weather_readings`.
- MirrorMaker2 uses `topicsPattern: ".*"`; sink sees mirrored topic `source.source.public.weather_readings`.
- JDBC sink consumes `source.source.public.weather_readings` and writes to `public.weather_readings` in `sink_db`.
- Both Connect clusters use Apicurio Avro converters and must resolve registry at `http://<MINIKUBE_SINK>:32080/apis/registry/v2`.

## Build/debug commands agents should use
- Rebuild/load Connect image: `./scripts/build-connect-image.sh minikube-a` and `./scripts/build-connect-image.sh minikube-b`
- Quick resource sanity: `./scripts/verify.sh minikube-a` (or `minikube-b`)
- Debezium smoke check: `./scripts/test-debezium.sh minikube-a`
- For data-path debugging, follow script logic: consume from Connect pod, then query sink Postgres row count.

## Safe change rules for this repo
- Keep resource names stable (`source-connect`, `sink-connect`, `source-to-sink`) because scripts select by fixed names/labels.
- If you change topic/table naming, update all three layers together: Debezium connector, MirrorMaker2 expectations, JDBC sink connector.
- If you change connector/plugin versions, update both `connect-image/Dockerfile` and `scripts/build-connect-image.sh` args together.
- Prefer extending `deploy-source.sh` / `deploy-sink.sh` gates over adding ad-hoc manual steps in README.

