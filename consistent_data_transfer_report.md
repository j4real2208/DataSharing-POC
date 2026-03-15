**Consistent Data Transfer Between Heterogeneous Data Stores**

Using Apache Kafka Connect and MirrorMaker 2

*A Proof-of-Concept Report*

\[Author Name\]

\[Institution / Course Name\]

\[Submission Date\]

Abstract

This report presents the design rationale and proof-of-concept (PoC)
implementation of an end-to-end pipeline for consistent and reliable
transfer between heterogeneous database systems. The architecture uses
Apache Kafka as the messaging backbone, Kafka Connect source connectors
with Change Data Capture (CDC) to ingest source-database changes,
Apache MirrorMaker 2 (MM2) to replicate topics across cluster
boundaries, and a Kafka Connect sink connector to materialise records in
an independent destination database. The report defines motivating
requirements, catalogues principal consistency challenges in distributed
multi-hop pipelines, and situates design choices within scientific and
engineering literature. The PoC demonstrates an at-least-once pipeline
with idempotent-equivalent destination outcomes (via primary-key
upsert) for practical workloads, while using Apicurio Registry for
shared schema resolution across source and sink connectors.

Table of Contents

1\. Introduction

2\. Motivation and Use Cases

2.1 Requirements

2.2 Database Migration with Zero Downtime

2.3 Cross-Environment Data Synchronisation

2.4 Disaster Recovery and Active-Passive Replication

2.5 Microservice Data Decoupling

2.6 Requirements-to-PoC Traceability

3\. Challenges

4\. Scientific and Technical Background

4.1 The Log as a Distributed Primitive

4.2 Change Data Capture (CDC)

4.3 Kafka Connect: Source and Sink Connectors

4.4 Cross-Cluster Replication with MirrorMaker 2

4.5 Delivery Semantics and Exactly-Once Guarantees

4.6 Schema Governance with Apicurio Registry

4.7 Literature Gap and PoC Contribution

5\. System Architecture

6\. Implementation (Proof-of-Concept)

7\. Results and Discussion

8\. Conclusion

8.1 Future Work Backlog

9\. References

1\. Introduction

Modern enterprise architectures rarely rely on a single, monolithic data
store. Operational databases, analytics warehouses, microservices, and
reporting systems are distributed across organisational and, in many
cases, geographical boundaries, each serving distinct operational
purposes and ownership domains. Maintaining data consistency across
these boundaries remains a foundational challenge in distributed systems
engineering.

Traditional synchronisation approaches --- periodic batch exports, ETL
pipelines, or direct database-to-database replication --- often suffer
from high latency, tight coupling, and fragility under failure.
Event-driven architectures centred on a persistent, replicated log
address many of these limitations by decoupling producers and consumers
while providing a durable, replayable record of change.

Apache Kafka has emerged as a dominant platform for event-driven
integration. Its design, introduced by Kreps et al. [1] and later
extended through Apache Kafka Improvement Proposals (KIPs), provides
high-throughput, fault-tolerant, and ordered message delivery. Kafka
Connect extends this platform with a standardised framework for source
and sink integration. MirrorMaker 2 (MM2), introduced in KIP-382 [6],
extends this model across Kafka clusters to support cross-data-centre
and cross-environment replication.

This report presents a PoC pipeline that composes these components into
an end-to-end transfer path: source-database changes are captured via
CDC, streamed to a source Kafka cluster, replicated to a destination
Kafka cluster via MM2, and materialised into a destination database via
a sink connector. Schema consistency across all hops is enforced through
Apicurio Registry.

2\. Motivation and Use Cases

This section defines the requirements motivating the PoC and maps them
to representative use cases.

2.1 Requirements

The PoC is designed around the following operational requirements:

-   **R1 - Continuous change propagation:** capture row-level changes
    from the source database and propagate them to a destination
    database with low latency.

-   **R2 - Decoupled cross-environment transfer:** move data across
    environment boundaries without direct database-to-database
    connectivity.

-   **R3 - Replay and recoverability:** retain a durable event log so
    consumers can recover from failure and replay from known offsets.

-   **R4 - Destination-side idempotent outcomes:** tolerate duplicate
    delivery in the transport path while converging to a correct final
    table state at the sink.

-   **R5 - Shared schema contract:** enforce compatible
    serialisation/deserialisation across source and sink connectors.

-   **R6 - Reproducible operations:** provide script-driven deployment
    and validation gates to ensure repeatable runs.

2.2 Database Migration with Zero Downtime

Organisations frequently need to migrate data from a legacy system to a
new database platform without incurring downtime. A CDC-based Kafka
pipeline allows the destination database to be kept in near-real-time
sync with the source, enabling a controlled cutover. The pipeline can
run continuously for days or weeks, and the switchover can happen at any
point with minimal data loss risk.

2.3 Cross-Environment Data Synchronisation

In many regulated industries, production data must be available in
isolated analytics or staging environments. Direct database connections
between environments violate security perimeters. A Kafka-mediated
pipeline that crosses environments via MM2 provides a controlled,
auditable channel that preserves data lineage without creating direct
network paths between environments.

2.4 Disaster Recovery and Active-Passive Replication

Regulatory and operational requirements often mandate that a recoverable
copy of operational data exists in a geographically separate location.
MirrorMaker 2 was designed explicitly for this purpose, enabling
active-passive and active-active replication topologies (KIP-382 [6]).

2.5 Microservice Data Decoupling

In a microservices architecture, multiple services may require access to
data that originates in a single authoritative database. Rather than
each service querying the source database directly, the data is
propagated through Kafka topics, allowing services to subscribe
independently without placing load on the origin system. Sink connectors
then materialise topic data into each service's local database.

2.6 Requirements-to-PoC Traceability

The current implementation satisfies these requirements to varying
degrees:

| Requirement | Implementation evidence in this repo | Status |
| --- | --- | --- |
| R1 | Debezium source connector captures `public.weather_readings` and publishes `source.public.weather_readings`; MM2 mirrors to sink topic namespace; JDBC sink writes to destination Postgres. | Implemented |
| R2 | Two-cluster topology (`minikube-a` and `minikube-b`) with MM2 bridging source Kafka to sink Kafka, avoiding direct source->sink DB coupling. | Implemented |
| R3 | Kafka topics provide retained log semantics; scripts include readiness checks and operational validation flows. | Implemented |
| R4 | JDBC sink uses upsert by primary key (`insert.mode=upsert`) to make replayed events converge to equivalent final row state. | Implemented |
| R5 | Both Connect clusters use Apicurio Avro converters and resolve shared registry on sink side. | Implemented |
| R6 | `scripts/deploy-source.sh` and `scripts/deploy-sink.sh` provide gated bring-up and verification flow. | Implemented |

3\. Challenges

The following challenges are central to achieving consistent data
transfer in distributed, multi-hop pipelines. For each challenge, this
report indicates whether the current PoC addresses it fully,
partially, or not yet.

| Challenge | Why it matters | PoC status | Current treatment in this report |
| --- | --- | --- | --- |
| Delivery semantics across hops | Determines duplicate/loss behavior under failures. | Partial | At-least-once path with sink upsert for idempotent-equivalent outcomes. |
| Transaction boundary preservation | Prevents intermediate inconsistent states at sink. | Partial | Row-level CDC events captured; no transaction-level sink buffering. |
| Schema evolution governance | Prevents producer/consumer breakage as schemas change. | Partial | Shared Apicurio used; explicit compatibility policy automation not yet codified. |
| Consumer offset translation across clusters | Enables cleaner failover and consumer resume semantics. | Partial | MM2 checkpointing present; group offset sync currently disabled. |
| Backpressure and throughput mismatch | Controls lag growth and recovery time under load. | Partial | Operationally observed, but no benchmark harness for systematic lag characterization. |
| Ordering guarantees | Protects per-entity correctness with parallel processing. | Partial | Design assumes key-based ordering; broad-scale partition stress tests not yet automated. |

3.1 Delivery Semantics Across Multiple Hops

Each stage of the pipeline introduces a potential failure boundary.
Achieving end-to-end exactly-once delivery requires coordinated
transactional guarantees at the source connector boundary (KIP-618 [7]),
within Kafka itself (idempotent producers and transactional APIs),
across clusters via MM2 (KIP-656 [8]), and at the sink connector boundary.
In practice, the weakest guarantee in the chain determines end-to-end
behaviour. In this PoC, the pipeline is characterised as at-least-once,
with destination upsert semantics mitigating duplicate effects.

3.2 Transaction Boundary Preservation

A source database may commit multi-row, multi-table transactions
atomically. CDC systems emit row-level change events, so one source
transaction is decomposed into multiple Kafka records. Without explicit
coordination, the sink may transiently observe intermediate,
inconsistent states. Debezium transaction metadata and the transactional
outbox pattern address this issue, but at additional implementation
complexity. The current PoC does not yet implement transaction-aware
sink application.

3.3 Schema Evolution

As source schemas evolve, Kafka message schemas must evolve accordingly.
Without a schema registry, producers and consumers become tightly
coupled through implicit assumptions. Apicurio Registry enforces
compatibility modes (BACKWARD, FORWARD, FULL) to reduce the risk of
silent breaking changes. The current PoC uses a shared registry but does
not yet automate explicit compatibility-policy enforcement.

3.4 Consumer Offset Translation Across Clusters

When MM2 replicates a topic from a source cluster to a destination
cluster, destination offsets differ from source offsets. Consumers that
resume after failover therefore require offset translation from source
to destination positions. MM2's MirrorCheckpointConnector provides this
mapping but must be explicitly enabled and correctly configured. In this
PoC, checkpointing is enabled, while consumer group offset sync remains
disabled.

3.5 Backpressure and Throughput Mismatch

The source connector may produce messages faster than the sink connector
or destination database can consume them. Kafka provides buffering, but
unbounded lag growth can increase recovery time and replay volume after
failures. Sink connectors must be sized and tuned to drain the topic at
sufficient throughput to maintain acceptable lag. Current scripts
validate functional propagation rather than throughput limits.

3.6 Ordering Guarantees

Kafka guarantees ordering only within a single partition. If the
pipeline uses multiple partitions for parallelism, events for the same
database row may be delivered out of order to the sink if partition
assignment is not key-aligned. Source connectors must partition by
primary key to preserve per-entity ordering.

4\. Scientific and Technical Background

4.1 The Log as a Distributed Primitive

The intellectual foundation of the entire architecture is the concept of
the distributed commit log. Kreps argues that the log --- an
append-only, totally ordered sequence of records --- is the unifying
abstraction underlying databases, distributed systems, and real-time
data integration. Every database already maintains a transaction log for
recovery purposes; Kafka externalises this concept as a first-class,
network-accessible, replicated service [2].

The seminal paper by Kreps, Narkhede, and Rao introduced Kafka as
a distributed messaging system designed specifically for log processing
at LinkedIn. Unlike traditional message brokers that delete messages
upon delivery, Kafka retains messages for a configurable period,
enabling consumers to replay history, restart from arbitrary offsets,
and decouple consumption rate from production rate. This property is
what makes the multi-hop pipeline in this PoC resilient: each stage can
fail and recover independently without data loss [1].

Implication for this PoC: using Kafka as the central durable log is what
allows independent recovery of Debezium, MM2, and JDBC sink components
without requiring tightly coupled restart procedures.

4.2 Change Data Capture (CDC)

Change Data Capture is the technique by which changes made to a database
are captured and propagated to downstream systems. Two principal
approaches exist in the literature:

**Query-based CDC** periodically polls the source database using
high-watermark queries (e.g., WHERE updated_at \> last_poll_time). This
approach is simple but incurs polling overhead, misses hard deletes, and
introduces latency proportional to the poll interval.

**Log-based CDC** reads directly from the database transaction log
(MySQL binary log, PostgreSQL Write-Ahead Log, Oracle redo log).
Debezium, the open-source CDC framework used in this PoC, implements
log-based CDC. Debezium\'s documentation confirms that this approach
captures all changes with millisecond latency, requires no changes to
the data model, captures deletions, and preserves the before- and
after-image of every changed row.

The scientific case for log-based CDC over query-based polling is
supported by performance studies showing that log reading places minimal
additional load on the source database, since the transaction log is
written regardless of whether CDC is active.

Implication for this PoC: log-based Debezium CDC is selected to minimize
source database intrusion while capturing inserts, updates, and deletes
from the same authoritative change stream.

4.3 Kafka Connect: Source and Sink Connectors

Kafka Connect is the integration framework built into the Apache Kafka
platform for scalable, fault-tolerant data movement between Kafka and
external systems. Its architecture separates concerns into three layers:
Workers (JVM processes that host connectors), Connectors (configuration
objects that define the integration), and Tasks (the execution units
that perform the actual data transfer).

Source connectors read from an external system and write to a Kafka
topic. Sink connectors read from a Kafka topic and write to an external
system. The Connect framework manages offset tracking for both types:
for source connectors, offsets represent the position in the source
(e.g., a WAL LSN or binlog position); for sink connectors, they
correspond to the Kafka topic offset of the last successfully written
record.

KIP-618 [7] extended the Connect
framework to support exactly-once semantics for source connectors by
wrapping the produce-and-commit-offset operation in a Kafka transaction.
Without KIP-618, a worker crash between producing a record to Kafka and
committing the source offset would cause the record to be re-read and
re-produced on restart, resulting in at-least-once delivery. With
KIP-618 enabled, the produce and offset commit are atomic, eliminating
this source of duplication.

Implication for this PoC: Kafka Connect provides the operational control
plane (worker/connector/task) for both source and sink data movement,
but the current manifests prioritize practical at-least-once operation
over full EOS tuning.

4.4 Cross-Cluster Replication with MirrorMaker 2

MirrorMaker 2 was introduced in Apache Kafka 2.4 via KIP-382 [6] as a
replacement for the original
MirrorMaker. MM2 is itself built on Kafka Connect, using three internal
connectors to achieve cross-cluster replication:

-   MirrorSourceConnector: consumes records from the source cluster and
    produces them to the destination cluster, preserving record headers
    and providing configurable topic renaming.

-   MirrorCheckpointConnector: periodically emits checkpoints containing
    the mapping between source consumer group offsets and their
    translated equivalents in the destination cluster, enabling
    consumers to resume from the correct position after failover.

-   MirrorHeartbeatConnector: emits periodic heartbeat records that
    allow monitoring tooling to measure end-to-end replication latency.

Because MM2 operates as a consumer on the source cluster and a producer
on the destination cluster, the two write operations --- the record
write to the destination topic and the offset commit on the source
cluster --- cannot be wrapped in a single atomic transaction that spans
both clusters. This is the fundamental cross-cluster consistency
challenge. KIP-656 [8] addresses this by
enabling exactly-once semantics within MM2 using Kafka's transactional
producer API on the destination cluster, committing consumer offsets as
part of the same transaction as the produced records.

Implication for this PoC: MM2 is used primarily for robust topic
replication between clusters; failover-grade offset translation is only
partially exercised in the current setup.

4.5 Delivery Semantics and Exactly-Once Guarantees

The distributed systems literature distinguishes three delivery
guarantees:

-   At-most-once: records may be lost but are never duplicated. Achieved
    by disabling retries and not committing offsets before processing.

-   At-least-once: records are never lost but may be delivered more than
    once. The standard guarantee for most Kafka configurations.

-   Exactly-once: each record is delivered and processed exactly once,
    even in the presence of failures. The strongest and most complex
    guarantee.

Kafka\'s exactly-once semantics (EOS) were introduced in version 0.11
through two mechanisms: idempotent producers (which assign sequence
numbers to records, allowing brokers to deduplicate retries) and
transactional producers (which allow a batch of records and an offset
commit to be applied atomically). Kleppmann provides a rigorous
treatment of the correctness conditions required for EOS in the broader
context of distributed data systems, noting that true end-to-end
exactly-once requires idempotency at every stage of the pipeline,
including the external sink system [5].

For this PoC, the pipeline is best characterised as at-least-once
across the multi-hop path, with idempotent writes (upsert by primary
key) at the destination database ensuring practical
idempotent-equivalent outcomes for replayed records.

Implication for this PoC: correctness is achieved through
at-least-once-plus-idempotent-sink design rather than strict end-to-end
exactly-once guarantees.

4.6 Schema Governance with Apicurio Registry

Schema compatibility is a cross-cutting concern that affects every hop
in the pipeline. Without a shared schema contract, a change to the
source database schema (e.g., adding a NOT NULL column) can silently
break deserialisation at the sink, causing consumer failures that may
not be detected immediately.

Apicurio Registry provides a centralised schema store that decouples the
schema definition from the message payload. Producers serialise records
using a schema fetched from the registry and embed only a compact schema
ID in the message. Consumers retrieve the schema by ID and use it for
deserialisation. The registry enforces configurable compatibility rules
--- BACKWARD compatibility ensures that new schemas can read data
written with old schemas, preventing breaking changes from being
registered.

The throughput benefit is significant: the Red Hat Apicurio Registry
User Guide notes that embedding only a schema ID rather than the full
schema in each message directly reduces message size and increases
achievable throughput for a given Kafka cluster configuration.

Implication for this PoC: a shared registry endpoint is used as the
cross-cluster schema contract between source and sink Connect workers.

4.7 Literature Gap and PoC Contribution

Existing literature and platform specifications provide strong treatment
of individual components (CDC, Kafka Connect, MM2, EOS, and schema
registry). However, comparatively less guidance exists on combining
these components into an operationally reproducible two-cluster pipeline
with explicit consistency trade-offs. This PoC contributes by
systematically documenting that integration path in a script-driven,
inspectable setup and by making its guarantee boundary explicit:
at-least-once delivery with idempotent-equivalent sink state, alongside
clearly stated gaps in transaction fidelity and failover semantics.

5\. System Architecture

The end-to-end pipeline consists of seven logical components arranged in
a linear topology:

1.  Source Database: The authoritative operational data store (e.g.,
    PostgreSQL or MySQL). All writes originate here.

2.  Kafka Connect Source Cluster (with Debezium CDC Connector): Monitors
    the source database transaction log and publishes row-level change
    events (insert, update, delete) as Avro-serialised messages to a
    Kafka topic in the source cluster. Apicurio Registry is used by the
    serialiser.

3.  Source Kafka Cluster: Stores the change event stream as a durable,
    partitioned, replicated log. Topics are partitioned by primary key
    to preserve per-entity ordering.

4.  MirrorMaker 2: Consumes from the source Kafka cluster and produces
    to the destination Kafka cluster. It is configured with broad topic
    replication (`topicsPattern: ".*"`) and a checkpoint connector; in
    the current PoC, consumer group offset sync is disabled
    (`sync.group.offsets.enabled=false`). Topic renaming conventions are
    applied to prevent naming collisions.

5.  Destination Kafka Cluster: Receives the replicated topic. Consumer
    groups reading from this cluster have access to translated offset
    checkpoints for failover support.

6.  Kafka Connect Sink Cluster (with JDBC Sink Connector): Consumes from
    the destination Kafka cluster and applies changes to the destination
    database using upsert semantics keyed by primary key.

7.  Destination Database: The target data store. Receives a consistent,
    eventually-synchronised copy of the source data.

Apicurio Registry is deployed as a shared service accessible to both the
source and destination Kafka Connect workers, ensuring that the same
schema definitions govern serialisation at the source and
deserialisation at the sink.

6\. Implementation (Proof-of-Concept)

6.1 Environment

The PoC is implemented on two Kubernetes clusters (two Minikube
profiles) and deployed primarily through gated shell scripts
(`scripts/deploy-source.sh` and `scripts/deploy-sink.sh`). The
deployment includes source and destination PostgreSQL clusters (Zalando
operator), source and destination Kafka clusters (Strimzi, KRaft), two
Kafka Connect clusters (Debezium source and Debezium JDBC sink), a
MirrorMaker 2 deployment, and an Apicurio Registry instance on the sink
side that is shared by both Connect clusters.

6.2 Source Connector Configuration

The Debezium PostgreSQL source connector is configured to use pgoutput
logical replication, publish change events to topics named using the
pattern \<server\>.\<schema\>.\<table\> (for this PoC:
`source.public.weather_readings`), and serialise using Apicurio Avro
converters. The worker and connector are configured for practical
single-task operation (`replicas: 1`, `tasksMax: 1`) with at-least-once
delivery behaviour in the current manifests.

6.3 MirrorMaker 2 Configuration

MM2 is configured to replicate topics matching a regex pattern from the
source cluster to the destination cluster (`topicsPattern: ".*"`). The
MirrorCheckpointConnector is present, but group offset sync is disabled
in the current PoC (`sync.group.offsets.enabled: "false"`).
Replication-related defaults are set to `-1` in MM2, inheriting cluster
defaults; in this single-node Minikube setup, effective replication is
single-replica.

6.4 Sink Connector Configuration

The JDBC sink connector is configured to use upsert semantics keyed on
the primary key field extracted from the Debezium event envelope
(`insert.mode=upsert`, `primary.key.mode=record_value`). It uses Avro
deserialisation via Apicurio Registry and currently drops delete/tombstone
records (`delete.handling.mode=drop`, `drop.tombstones=true`) rather
than propagating deletes to the destination database.

7\. Results and Discussion

The PoC demonstrates the following outcomes:

7.1 Data Consistency

The operational scripts validate end-to-end propagation by consuming
mirrored messages on the sink side and verifying that rows appear in
`sink_db.public.weather_readings`. Duplicate rows are not observed at
the destination in the tested inserts, consistent with upsert semantics.
The current repository scripts do not yet automate checksum comparison
or systematic failure-injection experiments.

7.2 Replication Lag

End-to-end replication latency is observable as low-latency in normal
PoC runs, but the current repository does not include an automated,
repeatable benchmark harness for publishing a formal latency
distribution (for example, p50/p95 across load levels).

7.3 Schema Evolution

The pipeline uses a shared Apicurio Registry and supports schema-based
serialisation/deserialisation at both Connect clusters. A dedicated,
scripted schema-evolution test (for example, nullable-column addition
plus compatibility assertion) is not yet codified in the current repo,
and compatibility policy is not explicitly set in the included manifest.

7.4 Challenges Not Fully Addressed

Transaction boundary preservation remains a partial implementation. The
PoC captures individual change events but does not yet implement
transactional buffering at the sink. As a result, the destination
database may briefly reflect intermediate states of multi-row source
transactions. Full transactional fidelity would require implementation
of the transactional outbox pattern at the source and a custom sink
connector capable of batching and committing by transaction ID.

8\. Conclusion

This report has presented a use-case-motivated, architecturally grounded
design for consistent end-to-end data transfer between heterogeneous
databases using Apache Kafka Connect and MirrorMaker 2. It identifies the
principal challenges --- delivery semantics, transaction boundary
preservation, schema evolution, and cross-cluster offset translation ---
and situates them within the relevant scientific and technical
literature.

The PoC validates the core CDC replication path and demonstrates
practical idempotent-equivalent outcomes at the sink boundary (via
upsert) without custom application code. The current implementation is
best characterised as at-least-once across the multi-hop pipeline, with
two priority gaps for future work: (1) transaction-boundary fidelity at
the sink boundary, and (2) explicit, automated validation of
failure-mode semantics and schema-compatibility policy enforcement.

Although implemented as a focused PoC, the architecture is not tied to a
single database technology and is applicable to broader enterprise data
integration scenarios, including migration, disaster recovery, and
microservice data decoupling.

8.1 Future Work Backlog

The following implementation backlog is prioritised to close the
remaining consistency and reproducibility gaps identified in this PoC:

1.  **P0 - Clarify and validate delivery semantics under failure**:
    add scripted failure-injection scenarios (for example, source/sink
    Connect restarts and MM2 restarts), then record replay/duplication
    outcomes as reproducible evidence.

2.  **P0 - Decide and implement delete propagation policy**: either
    enable delete propagation end-to-end (connector transform updates,
    sink handling, validation tests) or explicitly codify soft-delete
    semantics as an architectural constraint.

3.  **P1 - Improve cross-cluster failover readiness**: evaluate and,
    where appropriate, enable MM2 consumer group offset sync and add a
    documented failover runbook that exercises checkpoint translation.

4.  **P1 - Add schema governance automation**: define compatibility
    policy in Apicurio and add CI/scripted checks for non-breaking and
    breaking schema changes.

5.  **P2 - Extend validation depth**: add source-vs-sink checksum
    comparison and repeatable latency measurements (for example,
    p50/p95) under controlled load.

9\. References

References are listed in order of first citation using numeric style.
Grey literature (KIPs and official documentation) is included alongside
peer-reviewed sources, consistent with systems engineering reporting.

[1] J. Kreps, N. Narkhede, and J. Rao, "Kafka: A distributed messaging system for log processing," in *Proc. NetDB Workshop at VLDB*, 2011.

[2] J. Kreps, "The Log: What every software engineer should know about real-time data's unifying abstraction," LinkedIn Engineering Blog, 2013.

[3] G. Wang, J. Koshy, et al., "Building a replicated logging system with Apache Kafka," *Proc. VLDB Endowment*, 2015.

[4] M. Kleppmann and J. Kreps, "Kafka, Samza and the Unix philosophy of distributed data," *IEEE Data Eng. Bull.*, 2015.

[5] M. Kleppmann, *Designing Data-Intensive Applications*, ch. 11, "Stream Processing." O'Reilly Media, 2017.

[6] Apache Software Foundation, "KIP-382: MirrorMaker 2.0," Apache Kafka Wiki, 2019.

[7] Apache Software Foundation, "KIP-618: Exactly-Once Source Connectors," Apache Kafka Wiki, 2021.

[8] Apache Software Foundation, "KIP-656: MirrorMaker2 Exactly-Once Semantics," Apache Kafka Wiki, 2021.

[9] Apache Software Foundation, "KIP-986: Cross-Cluster Replication Improvements," Apache Kafka Wiki, 2024.

[10] Debezium Project, "Documentation: Log-based Change Data Capture," debezium.io, 2023.

[11] Red Hat, *Apicurio Registry User Guide v2.4*, docs.redhat.com, 2023.

[12] M. J. Sax et al., "Streams and Tables: Two sides of the same coin," in *BIRTE Workshop, VLDB*, 2018.

[13] A. Margara et al., "A survey on distributed data stream processing," *ACM Computing Surveys*, 2023.

[14] Decodable, "Aggregating CDC events on transactional boundaries," decodable.co, 2023.

[15] Confluent, "Kafka Connect Architecture and Exactly-Once," docs.confluent.io, 2024.

*Note: For KIPs and official documentation, versions accessible in March
2026 were used.*
