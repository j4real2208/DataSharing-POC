# POC Report: Efficient City Weather-Data Transfer via Civitas Connect

## Project Context

This proof of concept (PoC) evaluates whether a Civitas Connect-style
integration architecture is a viable option for efficient transfer of
city weather-sensor data across system boundaries.

This document consolidates implemented-state reporting and the companion
shortcomings/background analysis into a single file.

The tested data path is:

`source Postgres -> source Kafka (CDC) -> MirrorMaker 2 -> sink Kafka -> JDBC sink -> sink Postgres`

The representative dataset is `public.weather_readings`, modeled as city
weather-sensor readings. The operational scope is two isolated city
instances with unidirectional transfer only (`source city -> sink city`).

## Goal

Demonstrate that city sensor data can be transferred efficiently,
reliably, and repeatably using an event-driven architecture without
custom transfer application code.

\newpage

## Success Criteria

The PoC is considered viable if it demonstrates all of the following:

1. Continuous end-to-end propagation from source database to sink database.
2. Operationally low lag in normal runs.
3. Correct sink convergence under replay conditions (idempotent-equivalent outcomes).
4. Reproducible deployment and validation using repository scripts.

## Requirements and Use Cases

### Core Requirements

- **R1 - Efficient continuous transfer:** stream city weather-sensor
  changes from source to sink with low operational delay.
- **R2 - Reliable multi-hop delivery:** preserve data through
  source Kafka, cross-cluster replication, and sink materialization.
- **R3 - Recoverability and replay:** support restart/recovery behavior
  through durable log retention and offset-based consumption.
- **R4 - Practical sink correctness:** tolerate duplicate delivery while
  converging to correct final sink state.
- **R5 - Reproducible operations:** deploy and verify the pipeline using
  script-driven workflow.

### Motivating Use Cases

1. **City operations dashboards:** near-real-time weather readings for
   municipal monitoring and response.
2. **Cross-environment data sharing:** move sensor data into isolated
   analytics environments without direct database links.
3. **Disaster recovery readiness:** maintain a replicated city-data flow
   in an independent Kafka/database stack.
4. **Platform decoupling:** publish sensor changes once and enable
   multiple downstream consumers without loading the source database.

\newpage

## Challenge Register and PoC Coverage

| Challenge | Why it matters | PoC coverage |
| --- | --- | --- |
| Delivery semantics across multiple hops | Determines duplicate/loss behavior under failures. | **Addressed (partial):** pipeline is at-least-once end-to-end with sink upsert convergence. |
| Transaction boundary preservation | Prevents transient inconsistent sink states for multi-row commits. | **Partially addressed:** row-level CDC works; transaction-aware sink buffering is not implemented. |
| Cross-cluster offset translation | Supports cleaner failover and resume behavior. | **Partially addressed:** checkpointing exists; group offset sync is currently disabled. |
| Schema evolution governance | Prevents producer/consumer breakage over time. | **Partially addressed:** shared registry is in place; explicit policy automation is pending. |
| Throughput mismatch and backpressure | Affects lag growth and recovery time. | **Partially addressed:** functional flow validated; benchmark harness not yet implemented. |

This challenge profile documents what is currently solved in the PoC and
what remains for production hardening.

\newpage

## Scientific Background and Literature Context

This PoC is grounded in established distributed-systems and streaming
literature:

- Kafka as a distributed log foundation for decoupled, replayable data
  integration [1], [2].
- CDC as the practical mechanism for propagating source-database changes
  into streaming infrastructure [10].
- Kafka Connect as the operational framework for source and sink
  integration [15].
- MirrorMaker 2 for cross-cluster replication and failover-oriented
  offset handling [6], [8], [9].
- End-to-end delivery-semantics trade-offs and idempotent sink design as
  a practical correctness strategy [5], [13].

This background motivates the architectural choice made here: an
at-least-once, multi-hop streaming pipeline with idempotent-equivalent
sink outcomes as a pragmatic approach for city weather-sensor transfer.

\newpage

## Implemented Architecture

- **Capture:** Debezium PostgreSQL CDC connector on source side.
- **Transport backbone:** Kafka topics with durable log semantics.
- **Cross-cluster transfer:** MirrorMaker 2 from source Kafka to sink Kafka.
- **Materialization:** JDBC sink connector writing into sink Postgres.
- **Schema contract:** Shared Apicurio Registry across both Connect clusters.

\newpage

## Evidence of Viability in This Repository

- Source CDC emits weather-reading change events and MM2 mirrors them to
  sink-side topics.
- Sink connector applies upsert semantics keyed by primary key, so
  tested replayed records converge to the expected final sink state.
- Deployment and validation are script-driven (`scripts/deploy-source.sh`,
  `scripts/deploy-sink.sh`), improving reproducibility.
- The repository workflow verifies end-to-end arrival of data in
  `sink_db.public.weather_readings`.

## Consistency and Efficiency Position

Within current scope, this PoC supports the claim that a Civitas
Connect-style architecture is a viable option for efficient transfer of
city weather-sensor data.

- **Consistency posture:** At-least-once transport with idempotent-equivalent
  sink outcomes via upsert.
- **Efficiency posture:** Low-latency operational behavior is observed in
  normal runs, but not yet benchmarked with a formal p50/p95 harness.

## Current Boundaries

The PoC does not yet provide:

- formal throughput and latency benchmarking under controlled load,
- automated failure-injection evidence across all failure modes,
- transaction-boundary preservation at sink commit granularity,
- finalized delete-propagation policy for production semantics,
- bidirectional city-to-city synchronization (scope is source-to-sink-only).

\newpage

## Conclusion

The PoC demonstrates practical technical viability for transferring city
weather-sensor data through a Civitas Connect-oriented architecture.
It achieves reproducible end-to-end propagation and stable sink-state
convergence in the tested path, while clearly documenting the remaining
work required for production-grade performance and semantics validation.

## Recommended Next Steps

1. Add repeatable load tests and publish p50/p95 latency and throughput.
2. Add scripted failure-injection scenarios and capture replay outcomes.
3. Decide and implement delete semantics end-to-end.
4. Improve cross-cluster failover readiness with offset-sync runbooks.
5. Add automated schema-compatibility policy checks.

## References

[1] J. Kreps, N. Narkhede, and J. Rao, "Kafka: A distributed messaging system for log processing," in *Proceedings of the NetDB Workshop at VLDB*, 2011.

[2] J. Kreps, "The Log: What every software engineer should know about real-time data's unifying abstraction," *LinkedIn Engineering Blog*, 2013.

[5] M. Kleppmann, *Designing Data-Intensive Applications*, ch. 11, "Stream Processing." Sebastopol, CA, USA: O'Reilly Media, 2017.

[6] Apache Software Foundation, "KIP-382: MirrorMaker 2.0," *Apache Kafka Wiki*, 2019.

[8] Apache Software Foundation, "KIP-656: MirrorMaker2 Exactly-Once Semantics," *Apache Kafka Wiki*, 2021.

[9] Apache Software Foundation, "KIP-986: Cross-Cluster Replication Improvements," *Apache Kafka Wiki*, 2024.

[10] Debezium Project, "Documentation: Log-based Change Data Capture," *debezium.io*, 2023.

[13] A. Margara et al., "A survey on distributed data stream processing," *ACM Computing Surveys*, 2023.

[15] Confluent, "Kafka Connect Architecture and Exactly-Once," *docs.confluent.io*, 2024.
