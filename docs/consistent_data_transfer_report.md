# Consistent Data Transfer Between Heterogeneous Data Stores

## Using Apache Kafka Connect and MirrorMaker 2

*A Proof-of-Concept Report*

**Author:** [Author Name]

**Institution / Course:** [Institution / Course Name]

**Submission Date:** [Submission Date]

## Abstract

This report presents the design rationale and proof-of-concept (PoC)
implementation of an end-to-end pipeline for consistent and reliable
data transfer between heterogeneous database systems. In project terms,
the PoC targets efficient transfer of city weather-sensor data through a
Civitas Connect integration path, where sensor readings are captured,
transported across cluster boundaries, and materialised for downstream
city-data use. The evaluated scenario is explicitly constrained to two
isolated city environments with one-way transfer only from a designated
source city instance to a designated sink city instance.

The architecture uses Apache Kafka [1] as the messaging backbone, Kafka
Connect source connectors with Change Data Capture (CDC) [7] to ingest
source-database changes, Apache MirrorMaker 2 (MM2) [4] to replicate
topics across cluster boundaries, and a Kafka Connect sink connector
[9] to materialise records in an independent destination database.
Shared schema governance is provided by Apicurio Registry using Avro
serialisation.

This report motivates the architecture through concrete use cases,
identifies the key technical challenges for this class of problem,
positions the PoC against those challenges, and presents scientific
background grounding the design decisions. The PoC demonstrates an
at-least-once pipeline [3] with idempotent-equivalent destination
outcomes (via primary-key upsert) as a pragmatic correctness strategy
for practical workloads.

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Motivating Use Cases](#2-motivating-use-cases)
- [3. PoC Requirements and Traceability](#3-poc-requirements-and-traceability)
  - [3.1 Operational Requirements](#31-operational-requirements)
  - [3.2 Requirement-to-Implementation Traceability](#32-requirement-to-implementation-traceability)
- [4. Challenges](#4-challenges)
  - [4.1 Challenge Register](#41-challenge-register)
  - [4.2 Challenges Outside Current PoC Scope](#42-challenges-outside-current-poc-scope)
- [5. Scientific Background](#5-scientific-background)
  - [5.1 Distributed Logs as Integration Infrastructure](#51-distributed-logs-as-integration-infrastructure)
  - [5.2 Change Data Capture](#52-change-data-capture)
  - [5.3 Cross-Cluster Replication](#53-cross-cluster-replication)
  - [5.4 Delivery Semantics and Idempotent Sink Design](#54-delivery-semantics-and-idempotent-sink-design)
  - [5.5 Schema Governance](#55-schema-governance)
- [6. Scope Note](#6-scope-note)
- [7. System Architecture](#7-system-architecture)
- [8. Implementation (Proof-of-Concept)](#8-implementation-proof-of-concept)
  - [8.1 Environment](#81-environment)
  - [8.2 Source Connector Configuration](#82-source-connector-configuration)
  - [8.3 MirrorMaker 2 Configuration](#83-mirrormaker-2-configuration)
  - [8.4 Sink Connector Configuration](#84-sink-connector-configuration)
- [9. Results and Discussion](#9-results-and-discussion)
  - [9.1 Data Consistency](#91-data-consistency)
  - [9.2 Replication Lag](#92-replication-lag)
  - [9.3 Schema Evolution](#93-schema-evolution)
  - [9.4 Viability Assessment for City Weather Data Transfer](#94-viability-assessment-for-city-weather-data-transfer)
  - [9.5 Consistency and Efficiency Position](#95-consistency-and-efficiency-position)
- [10. Conclusion](#10-conclusion)
- [11. References](#11-references)

## 1. Introduction

Modern smart-city platforms aggregate data produced by heterogeneous
sensors and operational systems. A recurring integration problem is how
to move data from one city environment to another - or from a source
operational store to a downstream analytics or backup environment -
reliably and without tight coupling between the source and destination
systems. The naive approach of direct database replication creates
vendor and version coupling, requires firewall exceptions between
environments, and does not naturally support fan-out to multiple
consumers or replay of historical data [3].

This report documents the design rationale and implemented state of a
two-cluster, source-to-sink-only Civitas Connect proof of concept for
city weather-sensor data transfer using an event-driven streaming
architecture. The central hypothesis is that Change Data Capture (CDC)
combined with a Kafka messaging backbone and cross-cluster replication
(MirrorMaker 2) can deliver a reliable, decoupled, and reproducible
data transfer pipeline without custom application code in the data path.

The implemented data path in this repository is:

`source Postgres -> source Kafka (CDC) -> MirrorMaker 2 -> sink Kafka -> JDBC sink -> sink Postgres`

The representative dataset is `public.weather_readings`. The objective
is to capture active architecture, deployed configuration, and observed
operational outcomes, while grounding the design in scientific and
technical literature. Section 2 motivates the work through concrete use
cases. Section 4 identifies key challenges. Section 5 provides
scientific background.

## 2. Motivating Use Cases

The following use cases represent concrete scenarios in which reliable,
decoupled, cross-environment data transfer has operational value for a
Civitas Connect-style city platform.

### 2.1 City Operations Dashboards

Municipal operations teams require near-real-time access to weather
sensor readings (temperature, humidity, air quality) to support
situational awareness and emergency response. Providing this data
through direct queries to source operational databases creates load on
production systems and makes the dashboard tightly coupled to the source
schema. A streaming CDC pipeline decouples source reads from dashboard
consumption, lowering source load and enabling replay of recent history
without burdening the operational database.

### 2.2 Cross-Environment Data Sharing Without Direct Database Links

City institutions regularly exchange operational data with partner
organisations, national agencies, or cloud analytics platforms. Direct
database links between organisations are operationally risky, require
network-level access grants, and make schema changes in one environment
immediately visible - and potentially breaking - to the other. An
event-driven pipeline with a well-defined schema contract (enforced
via a shared Apicurio Registry) provides a structured integration
boundary that insulates each side from the other's internal changes.

### 2.3 Disaster Recovery and Data Replication

Maintaining a warm copy of weather-reading data in an independent
Kafka-and-database stack provides a recovery path if the source
environment becomes unavailable. Because Kafka retains the full topic
log, the sink environment can replay from any retained offset, making
the sink both a current copy and a replayable audit trail. MirrorMaker 2
offset checkpointing provides the translation layer needed for consumers
in the sink environment to resume from equivalent positions after a
source-side failure [4].

### 2.4 Platform Decoupling and Fan-Out

A CDC-to-Kafka pipeline naturally supports multiple independent
consumers: a database sink, a search index, a real-time dashboard, and
a data warehouse can all consume the same topic without any of them
loading the source database. Adding a new consumer requires no change to
the source system and no coordination with other consumers. This
decoupling is a core motivation for the log-centric integration
architecture described in [2].

### 2.5 Summary

Across these use cases, the common requirements are: low coupling
between source and destination systems, reliable delivery with recovery
semantics, schema discipline that allows independent evolution, and
operational reproducibility. These use cases directly motivate the
requirements in Section 3 and the challenge analysis in Section 4.

## 3. PoC Requirements and Traceability

This section lists the operational requirements for the implemented PoC
and maps them to repository evidence.

### 3.1 Operational Requirements

-   **R1 - Continuous propagation:** capture source row changes and
    propagate them to sink database tables with low operational delay.
-   **R2 - Isolated environment transfer:** move data across two
    isolated city environments without direct source-to-sink database
    coupling (addresses use cases 2.1-2.4).
-   **R3 - Recoverable transport path:** use Kafka log retention and
    connector offsets to support restart and recovery behaviour (use
    case 2.3).
-   **R4 - Practical sink correctness:** tolerate duplicate delivery
    (inherent in at-least-once pipelines) and converge through sink
    upsert semantics (use case 2.1, 2.3).
-   **R5 - Shared schema contract:** use a common registry-backed
    serialisation/deserialisation contract to insulate source and sink
    from each other's internal changes (use case 2.2).
-   **R6 - Reproducible operations:** deploy and verify the PoC via
    scripted workflow to ensure repeatable evidence.

Operational boundary: this PoC is strictly unidirectional
(`source city -> sink city`). Reverse replication is not enabled.

### 3.2 Requirement-to-Implementation Traceability

| Requirement | Implementation evidence in this repo | Status |
| --- | --- | --- |
| R1 | Debezium source connector captures `public.weather_readings` and publishes `source.public.weather_readings`; MM2 mirrors to `source.source.public.weather_readings` on the sink cluster; JDBC sink writes to destination Postgres. | Implemented |
| R2 | Two-cluster topology (`minikube-a` and `minikube-b`) with MM2 bridging source Kafka to sink Kafka, avoiding direct source->sink DB coupling. | Implemented |
| R3 | Kafka topics provide retained log semantics; scripts include readiness checks and operational validation flows. | Implemented |
| R4 | JDBC sink uses upsert by primary key (`insert.mode=upsert`) to make replayed events converge to equivalent final row state. | Implemented |
| R5 | Both Connect clusters use Apicurio Avro converters and resolve shared registry on sink side. | Implemented |
| R6 | `scripts/deploy-source.sh` and `scripts/deploy-sink.sh` provide gated bring-up and verification flow. | Implemented |

## 4. Challenges

Distributing data across heterogeneous stores through a multi-hop
streaming pipeline introduces a well-known set of technical challenges.
This section identifies the challenges relevant to this class of
problem, characterises their impact, and records the current PoC
coverage of each. This challenge profile distinguishes what is addressed
in the current implementation from what remains for future hardening.

### 4.1 Challenge Register

| Challenge | Why it matters | Current PoC coverage |
| --- | --- | --- |
| **Delivery semantics across hops** | Each hop (source DB -> source Kafka, source Kafka -> sink Kafka via MM2, sink Kafka -> sink DB) independently determines whether messages can be duplicated or lost. The aggregate guarantee is at most as strong as the weakest hop. | **Addressed (partial):** the pipeline is configured for at-least-once delivery end-to-end. Sink upsert semantics provide idempotent-equivalent convergence for tested inserts. Exactly-once source support (`exactly.once.source.support`) is not currently enabled in the Connect worker. |
| **Transaction boundary preservation** | A source commit may affect multiple rows. If these rows arrive at the sink as independent events applied at different times, the sink can briefly be in a state that never existed in the source - a transient inconsistency. | **Partially addressed:** the JDBC sink processes one record at a time. Row-level CDC is functional, but no transaction-aware buffering is implemented to atomically apply multi-row source transactions at the sink. |
| **Schema evolution governance** | If the source schema changes (e.g., a column is added or removed), the consumer must be able to deserialise both old and new messages without breakage. Without an enforced compatibility policy, silent consumer failures are possible. | **Partially addressed:** a shared Apicurio Registry is deployed and used by both Connect workers for Avro serialisation/deserialisation. Explicit schema compatibility policy (e.g., `BACKWARD`, `FORWARD`) is not yet configured in the registry, so schema governance relies on developer discipline rather than automated enforcement. |
| **Cross-cluster consumer offset translation** | When the sink-side consumer restarts after a source-side failure, it must be able to resume from an equivalent offset on the sink cluster without re-processing the entire topic. MM2's checkpoint connector provides offset translation, but it must be actively maintained. | **Partially addressed:** the MirrorCheckpointConnector is present in the MM2 deployment. However, group offset sync is currently disabled (`sync.group.offsets.enabled: "false"`), meaning automatic offset sync for sink-side consumers is not active in this PoC. |
| **Throughput mismatch and backpressure** | If the sink connector or sink database cannot keep up with the inbound event rate, topic consumer lag grows. Without visibility into lag and a remediation plan, the pipeline can fall arbitrarily far behind. | **Partially addressed:** the functional data flow is validated in normal PoC runs. A benchmark harness measuring p50/p95 latency and throughput under controlled load has not been implemented. |
| **Ordering guarantees under parallelism** | Kafka preserves order within a partition, but multi-partition topics and multi-task connectors can reorder events for the same entity if partitioning is not consistent. | **Partially addressed:** topic partitioning by primary key is assumed in the current configuration, preserving per-entity ordering. Multi-task or multi-partition stress testing has not been performed. |
| **Delete propagation semantics** | In a full-CRUD CDC pipeline, source deletes must be propagated to the sink; otherwise deleted source rows remain indefinitely at the sink. Tombstone messages (null-value records) are the Kafka convention for signalling deletes. | **Not addressed in current PoC:** delete and tombstone records are actively dropped (`delete.handling.mode=drop`, `drop.tombstones=true` in the JDBC sink configuration). This is a deliberate simplification; delete propagation remains a pending design decision. |

### 4.2 Challenges Outside Current PoC Scope

The following challenges are relevant for production deployments but are
outside the scope of this PoC:

- **Bidirectional synchronisation:** when both city environments accept
  writes, conflict resolution (last-writer-wins, vector clocks, or
  application-level merge) becomes necessary. This PoC is strictly
  unidirectional.
- **Multi-tenant isolation:** in a shared Kafka cluster serving multiple
  city datasets, topic ACLs, quota enforcement, and schema-registry
  namespace isolation are required. This PoC uses a single-dataset
  topology.
- **End-to-end encryption and access control:** production deployments
  require mutual TLS on all Kafka connections and fine-grained connector
  credential management. This is not configured in the PoC.

## 5. Scientific Background

This section provides the technical and scientific context that grounds
the architecture choices made in this PoC. Each subsection links
literature to a specific design decision.

### 5.1 Distributed Logs as Integration Infrastructure

Kafka was introduced by Kreps et al. [1] as a high-throughput,
durable, distributed commit log for log processing at scale. Its core
abstraction - an immutable, ordered, partitioned log of records with
configurable retention - makes it fundamentally different from
traditional message queues: consumers can re-read historical data, new
consumers can be added without coordination, and the log acts as a
shared truth across diverse downstream systems.

Kreps [2] later articulated the broader architectural implication: a
centralised, replayable log can serve as the integration backbone for an
entire data platform, replacing point-to-point integrations with a
single publish-and-subscribe model. This is the architectural pattern
directly applied in this PoC: all city weather-sensor changes are
published once to a Kafka topic and consumed independently by any
registered subscriber, without any dependency on the source database
being available at consumption time.

This log-centric approach directly addresses use cases 2.2 and 2.4
(cross-environment sharing and fan-out decoupling).

### 5.2 Change Data Capture

Change Data Capture (CDC) is the mechanism by which database row changes
are extracted and published as a stream of events. Log-based CDC
(as used by Debezium [7]) reads directly from the database replication
log (PostgreSQL's `pgoutput` logical replication protocol in this PoC),
which avoids polling overhead, captures all changes including deletes,
and preserves the original commit order of transactions.

The Debezium project [7] provides a production-quality Kafka Connect
source connector for PostgreSQL that translates `pgoutput` replication
stream entries into Kafka records. Each record carries the full before
and after state of a row, plus metadata including the source transaction
ID, commit timestamp, and schema version. This rich event envelope
enables downstream consumers to reconstruct source state, detect
schema changes, and maintain audit trails without any additional
instrumentation of the source application.

CDC directly enables R1 (continuous propagation) and R2 (decoupled
transfer) from Section 3.

### 5.3 Cross-Cluster Replication

MirrorMaker 2 [4] is the Kafka-native solution for replicating topics
across cluster boundaries. It is built on the Kafka Connect framework
and uses internal source connectors (MirrorSourceConnector,
MirrorCheckpointConnector, MirrorHeartbeatConnector) to replicate
records, translate consumer group offsets, and monitor replication lag.

A key motivation for MM2 over simpler replication approaches is offset
translation: when a consumer in the sink cluster needs to resume
consumption after a source-cluster failure, the checkpoint connector
provides a mapping from source-cluster offsets to equivalent sink-cluster
offsets, enabling near-seamless failover [4], [6]. The current PoC
deploys the checkpoint connector but does not yet activate group offset
sync (R3 partially addressed).

KIP-656 [5] extended MM2 with exactly-once semantics for intra-cluster
replication scenarios; the cross-cluster case retains at-least-once
characteristics in the current PoC configuration (see Section 4.1,
challenge row for delivery semantics).

### 5.4 Delivery Semantics and Idempotent Sink Design

Kleppmann [3] provides a comprehensive treatment of delivery semantics
in distributed stream processing pipelines. The central insight is that
achieving exactly-once semantics end-to-end across multiple hops is
extremely difficult and often unnecessary: at-least-once delivery
combined with an idempotent consumer (one where re-processing the same
message produces the same result) achieves equivalent final-state
correctness for most practical workloads.

In this PoC, the JDBC sink connector is configured with
`insert.mode=upsert` and `primary.key.mode=record_value`. This means
that if the same Debezium change event is delivered more than once
(due to connector restart, rebalance, or redelivery), the resulting
database row will be identical - the upsert converges to the correct
state rather than creating duplicates. This directly implements R4.

Margara et al. [8] survey distributed stream processing systems and
identify idempotent sink design as a standard industrial pattern for
tolerating at-least-once delivery in production pipelines, confirming
that this PoC's approach is consistent with the broader literature.

### 5.5 Schema Governance

In a multi-hop Avro-serialised pipeline, the serialised byte format on
the Kafka topic must be interpretable by every consumer across the
lifetime of the topic. Without a schema registry, schema evolution
(adding or removing fields) silently breaks consumers that depend on
the old schema. A schema registry (Apicurio Registry in this PoC) acts
as a shared, versioned schema store: producers register schemas before
writing, consumers look up schemas by ID at deserialisation time, and
the registry can enforce compatibility policies to prevent breaking
changes from being published.

This architecture directly implements R5 (shared schema contract). The
current PoC deploys the registry and uses it at both the source and
sink Connect workers; explicit compatibility policies (backward,
forward, or full) remain as a future hardening item (see Section 4.1).

## 6. Scope Note

This document covers currently implemented PoC state: deployed topology,
active connector and MM2 configuration, and observed operational
outcomes. Detailed challenge analysis, known limitations, and future
work backlog items are maintained in
`docs/implementation_shortcomings_and_background.md`.

## 7. System Architecture

The end-to-end pipeline consists of seven logical components arranged in
a linear topology across two isolated city environments:

1.  **Source Database:** The authoritative operational data store
    (PostgreSQL, Zalando operator). All writes originate here.

2.  **Kafka Connect Source Cluster (Debezium CDC Connector):** Monitors
    the source database transaction log via PostgreSQL logical
    replication (`pgoutput` protocol) and publishes row-level change
    events (insert, update, delete) as Avro-serialised messages to a
    Kafka topic in the source cluster. Apicurio Registry is used by
    the serialiser to register and resolve schemas [7].

3.  **Source Kafka Cluster:** Stores the change event stream as a
    durable, partitioned, replicated log. Topics are partitioned by
    primary key to preserve per-entity ordering.

4.  **MirrorMaker 2:** Consumes from the source Kafka cluster and
    produces to the destination Kafka cluster [4]. Configured with broad
    topic replication (`topicsPattern: ".*"`) and a checkpoint connector;
    consumer group offset sync is disabled in the current PoC
    (`sync.group.offsets.enabled=false`). Topic renaming conventions
    prevent naming collisions in the sink namespace.

5.  **Destination Kafka Cluster:** Receives the replicated topic.
    Consumer groups reading from this cluster have access to translated
    offset checkpoints for failover support when offset sync is enabled.

6.  **Kafka Connect Sink Cluster (JDBC Sink Connector):** Consumes from
    the destination Kafka cluster and applies changes to the destination
    database using upsert semantics keyed by primary key [9].

7.  **Destination Database:** The target data store (PostgreSQL, Zalando
    operator). Receives a consistent, eventually-synchronised copy of
    the source data.

Apicurio Registry is deployed as a shared service accessible to both
the source and destination Kafka Connect workers, ensuring that the same
schema definitions govern serialisation at the source and
deserialisation at the sink.

## 8. Implementation (Proof-of-Concept)

### 8.1 Environment

The PoC is implemented on two Kubernetes clusters (two Minikube
profiles) and deployed primarily through gated shell scripts
(`scripts/deploy-source.sh` and `scripts/deploy-sink.sh`).
Operationally, these clusters represent two isolated Civitas Connect
city instances: a source-city side (`minikube-a`) and a sink-city side
(`minikube-b`). The deployment includes source and destination
PostgreSQL clusters (Zalando operator), source and destination Kafka
clusters (Strimzi, KRaft mode), two Kafka Connect clusters (Debezium
source and Debezium JDBC sink), a MirrorMaker 2 deployment, and an
Apicurio Registry instance on the sink side shared by both Connect
clusters.

### 8.2 Source Connector Configuration

The Debezium PostgreSQL source connector is configured to use pgoutput
logical replication, publish change events to topics named using the
pattern `<server>.<schema>.<table>` (for this PoC:
`source.public.weather_readings`), and serialise using Apicurio Avro
converters. The worker and connector are configured for single-task
operation (`replicas: 1`, `tasksMax: 1`) with at-least-once delivery
behaviour in the current manifests.

### 8.3 MirrorMaker 2 Configuration

MM2 is configured to replicate topics matching a regex pattern from the
source cluster to the destination cluster (`topicsPattern: ".*"`). The
MirrorCheckpointConnector is present, but group offset sync is disabled
in the current PoC (`sync.group.offsets.enabled: "false"`).
Replication-related defaults are set to `-1` in MM2, inheriting cluster
defaults; in this single-node Minikube setup, effective replication is
single-replica.

### 8.4 Sink Connector Configuration

The JDBC sink connector (`io.debezium.connector.jdbc.JdbcSinkConnector`)
is configured to use upsert semantics keyed on the primary key field
extracted from the Debezium event envelope
(`insert.mode=upsert`, `primary.key.mode=record_value`). It uses Avro
deserialisation via Apicurio Registry and currently drops
delete/tombstone records (`delete.handling.mode=drop`,
`drop.tombstones=true`) rather than propagating deletes to the
destination database. This is a deliberate PoC simplification noted in
the challenge register (Section 4.1).

## 9. Results and Discussion

The PoC demonstrates the following outcomes:

### 9.1 Data Consistency

The operational scripts validate end-to-end propagation by consuming
mirrored messages on the sink side and verifying that rows appear in
`sink_db.public.weather_readings`. Duplicate rows are not observed at
the destination in the tested inserts, consistent with upsert semantics.
This result is consistent with the at-least-once plus idempotent-sink
pattern described in [3] and [8].

### 9.2 Replication Lag

End-to-end replication latency is observed as low in normal PoC runs,
with the source-to-sink flow remaining operational during standard test
execution. A formal p50/p95 latency benchmark under controlled load has
not yet been implemented (see Section 4.1, throughput challenge).

### 9.3 Schema Evolution

The pipeline uses a shared Apicurio Registry and supports schema-based
serialisation/deserialisation at both Connect clusters. No explicit
compatibility policy is currently configured; schema changes in the
tested PoC are applied manually.

### 9.4 Viability Assessment for City Weather Data Transfer

Against the Civitas Connect project goal and the use cases in Section 2,
the current PoC indicates that this architecture is a viable option for
efficient city weather-sensor data transfer within the tested scope.
The repository demonstrates stable end-to-end propagation for
`weather_readings`, practical idempotent-equivalent sink outcomes,
and script-driven reproducibility - satisfying R1-R6 as documented in
Section 3.

The challenge register in Section 4 identifies the gap between current
PoC viability and production-grade hardening: transaction-boundary
fidelity, delete propagation, offset-sync failover, and benchmarked
throughput remain open items.

### 9.5 Consistency and Efficiency Position

-   **Consistency posture:** at-least-once transport across the full
    path, with idempotent-equivalent sink outcomes through primary-key
    upsert [3].
-   **Efficiency posture:** operationally low lag is observed in normal
    PoC runs; no formal benchmarking has been conducted.
-   **Delivery gap:** exactly-once source support and transaction-aware
    sink buffering are not currently enabled, consistent with the
    at-least-once posture documented throughout.

## 10. Conclusion

This report presents the design rationale, motivating use cases,
challenge analysis, scientific background, and current implementation
state of a Civitas Connect-style, source-to-sink-only city
weather-data transfer PoC.

The motivating use cases (Section 2) - city operations dashboards,
cross-environment data sharing, disaster recovery, and platform
decoupling - establish a concrete justification for the
event-driven, CDC-based architecture. The challenge register
(Section 4) identifies delivery semantics, transaction boundary
preservation, schema governance, offset translation, and throughput
management as the key technical challenges for this class of pipeline,
and documents which are addressed (at-least-once with idempotent sink,
shared schema registry, checkpoint connector) and which remain for
future hardening (delete propagation, offset sync, throughput
benchmarking). The scientific background (Section 5) grounds each major
design decision in established literature [1], [2], [3], [4], [7],
[8], [9].

The implemented path validates end-to-end propagation, sink-side
idempotent-equivalent outcomes, and reproducible deployment workflows.
For detailed shortcomings, unresolved challenges, and future work
backlog, see `docs/implementation_shortcomings_and_background.md`.

## 11. References

[1] J. Kreps, N. Narkhede, and J. Rao, "Kafka: A distributed messaging
system for log processing," in *Proceedings of the NetDB Workshop at
VLDB*, 2011.

[2] J. Kreps, "The Log: What every software engineer should know about
real-time data's unifying abstraction," *LinkedIn Engineering Blog*,
2013.

[3] M. Kleppmann, *Designing Data-Intensive Applications*, ch. 11,
"Stream Processing." Sebastopol, CA, USA: O'Reilly Media, 2017.

[4] Apache Software Foundation, "KIP-382: MirrorMaker 2.0," *Apache
Kafka Wiki*, 2019.

[5] Apache Software Foundation, "KIP-656: MirrorMaker2 Exactly-Once
Semantics," *Apache Kafka Wiki*, 2021.

[6] Apache Software Foundation, "KIP-986: Cross-Cluster Replication
Improvements," *Apache Kafka Wiki*, 2024.

[7] Debezium Project, "Documentation: Log-based Change Data Capture,"
*debezium.io*, 2023.

[8] A. Margara et al., "A survey on distributed data stream
processing," *ACM Computing Surveys*, 2023.

[9] Confluent, "Kafka Connect Architecture and Exactly-Once,"
*docs.confluent.io*, 2024.
