ARG STRIMZI_VERSION=0.50.0
ARG KAFKA_VERSION=4.1.1
ARG DEBEZIUM_VERSION=3.4.1.Final

FROM quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}

USER root

RUN microdnf install -y tar && microdnf clean all

RUN mkdir -p /opt/kafka/plugins

# Debezium Postgres connector (includes dependencies)
RUN curl -fsSL -o /tmp/debezium.tgz \
  https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.4.1.Final/debezium-connector-postgres-3.4.1.Final-plugin.tar.gz \
  && tar -xzf /tmp/debezium.tgz -C /opt/kafka/plugins \
  && rm -f /tmp/debezium.tgz

# Debezium JDBC sink connector (includes dependencies)
RUN curl -fsSL -o /tmp/debezium-jdbc.tgz \
  https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-jdbc/3.4.1.Final/debezium-connector-jdbc-3.4.1.Final-plugin.tar.gz \
  && tar -xzf /tmp/debezium-jdbc.tgz -C /opt/kafka/plugins \
  && rm -f /tmp/debezium-jdbc.tgz


USER 1001
