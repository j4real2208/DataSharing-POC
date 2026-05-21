#!/bin/bash

# Demo script to showcase live data flow from Postgres to Kafka topic
# Ensure the environment is set up and the necessary clusters are running

set -e

# Define contexts and configurations
SOURCE_CONTEXT="minikube-a"
SINK_CONTEXT="minikube-b"

# Step 1: Get the source Postgres pod using correct labels
SOURCE_POD=$(kubectl --context=$SOURCE_CONTEXT get pods -n database \
  -l cluster-name=source-postgres \
  -o jsonpath='{.items[0].metadata.name}')

if [ -z "$SOURCE_POD" ]; then
  echo "Error: Could not find source Postgres pod. Make sure deploy-source.sh has completed."
  exit 1
fi

echo "Found source Postgres pod: $SOURCE_POD"

# Insert live demo data into the source Postgres database
echo "Inserting live demo data into source Postgres..."

# Function to generate random temperature
generate_random_temp() {
  # Generate random number between 10 and 30 with one decimal place
  local int_part=$((RANDOM % 20 + 10))
  local decimal_part=$((RANDOM % 10))
  echo "${int_part}.${decimal_part}"
}

for city in "Dubai" "Berlin" "Munich"; do
  temp=$(generate_random_temp)
  kubectl --context=$SOURCE_CONTEXT exec -n database $SOURCE_POD -- psql -U postgres -d source_db -c \
    "INSERT INTO public.weather_readings (city, temperature_c) VALUES ('$city', $temp);"
  echo "Inserted data for $city with temperature $temp°C"
  sleep 1
done

echo ""
echo "Data inserted into source Postgres. Waiting for Debezium to capture changes..."
sleep 3

# Step 2: Get the source Kafka Connect pod using correct labels
CONNECT_POD=$(kubectl --context=$SOURCE_CONTEXT get pods -n messaging \
  -l strimzi.io/cluster=source-connect,strimzi.io/kind=KafkaConnect \
  -o jsonpath='{.items[0].metadata.name}')

if [ -z "$CONNECT_POD" ]; then
  echo "Error: Could not find source Kafka Connect pod. Make sure deploy-source.sh has completed."
  exit 1
fi

echo "Found source Kafka Connect pod: $CONNECT_POD"
echo ""
echo "Consuming CDC messages from Kafka topic..."
echo ""

# Consume data from the Kafka topic using Apicurio deserializer
MINIKUBE_SINK_IP="$(minikube -p "$SINK_CONTEXT" ip)"
REGISTRY_URL="http://${MINIKUBE_SINK_IP}:32080/apis/registry/v2"

kubectl --context=$SOURCE_CONTEXT exec -n messaging $CONNECT_POD -- /bin/bash -lc "
CLASSPATH=\"/opt/kafka/libs/*:/opt/kafka/plugins/apicurio-converters/*\" \
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server source-kafka-kafka-bootstrap:9092 \
  --topic source.public.weather_readings \
  --from-beginning \
  --group demo-consumer-\$(date +%s) \
  --skip-message-on-error \
  --timeout-ms 30000 \
  --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer \
  --property value.deserializer.apicurio.registry.url=${REGISTRY_URL} \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=\" | \"
"

echo ""
echo "Demo complete! Data has been pushed to Postgres and consumed from Kafka topic."
