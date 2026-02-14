ARG STRIMZI_VERSION=0.50.0
ARG KAFKA_VERSION=4.1.1
ARG DEBEZIUM_VERSION=3.4.1.Final
ARG APICURIO_VERSION=3.1.0

FROM quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}

ARG APICURIO_VERSION=3.1.0

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

RUN mkdir -p /opt/kafka/plugins/apicurio-converters \
  && curl -fsSL -o /tmp/apicurio-converters.tar.gz \
  https://repo.maven.apache.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/${APICURIO_VERSION}/apicurio-registry-distro-connect-converter-${APICURIO_VERSION}.tar.gz \
  && tar -xzf /tmp/apicurio-converters.tar.gz -C /opt/kafka/plugins/apicurio-converters \
  && rm -f /tmp/apicurio-converters.tar.gz

# Apicurio Registry Kafka Connect converters (distro bundle)
#RUN mkdir -p /opt/kafka/plugins/apicurio \
#  && curl -fsSL -o /opt/kafka/plugins/apicurio/apicurio-registry-distro-connect-converter-${APICURIO_VERSION}.jar \
#  https://repo.maven.apache.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/${APICURIO_VERSION}/apicurio-registry-distro-connect-converter-${APICURIO_VERSION}.jar


USER 1001
