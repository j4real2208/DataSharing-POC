# Implementation Shortcomings and Background

## Purpose

This companion document captures material intentionally removed from
`consistent_data_transfer_report.md` to keep the main report focused on
currently implemented PoC state. It consolidates challenge analysis,
scientific context, known limitations, and future work for the same
scope: two isolated city instances with unidirectional
`source city -> sink city` transfer.

## Challenge Register

| Challenge | Why it matters | Current PoC status |
| --- | --- | --- |
| Delivery semantics across hops | Determines duplicate/loss behavior under failures. | Addressed (partial): at-least-once path with sink upsert convergence. |
| Transaction boundary preservation | Prevents transient inconsistent sink states. | Partially addressed: row-level CDC only; no transaction-aware sink buffering. |
| Schema evolution governance | Prevents producer/consumer breakage over time. | Partially addressed: shared Apicurio registry is present; explicit policy automation is pending. |
| Consumer offset translation across clusters | Supports cleaner failover and consumer resume behavior. | Partially addressed: checkpointing exists; group offset sync is disabled. |
| Throughput mismatch and backpressure | Impacts lag growth and recovery time. | Partially addressed: functional flow validated; benchmark harness not implemented. |
| Ordering guarantees | Protects per-entity correctness under parallelism. | Partially addressed: key-based assumptions present; no broad partition stress validation. |

## Selected Scientific and Technical Background

- Kafka as a distributed, replayable log for decoupled integration [1], [2].
- CDC as the mechanism for propagating source-database changes [10].
- Kafka Connect as source/sink integration framework [15].
- MirrorMaker 2 for cross-cluster replication and offset translation [6], [8], [9].
- Delivery semantics and idempotent sink strategy in distributed pipelines [5], [13].

## Limitations and Scope Boundaries

The current PoC viability claim is intentionally bounded. It does not
currently provide:

- formal throughput and latency benchmarking under controlled load;
- comprehensive automated failure-injection evidence;
- transaction-boundary fidelity at sink commit granularity;
- finalized delete-propagation policy for production semantics;
- bidirectional city-to-city synchronization (scope is source-to-sink-only).

## Future Work Backlog

1. **P0 - Clarify and validate delivery semantics under failure:** add
   scripted failure-injection scenarios (for example, source/sink
   Connect restarts and MM2 restarts), then record replay/duplication
   outcomes as reproducible evidence.
2. **P0 - Decide and implement delete propagation policy:** either
   enable delete propagation end-to-end (connector transform updates,
   sink handling, validation tests) or explicitly codify soft-delete
   semantics as an architectural constraint.
3. **P1 - Improve cross-cluster failover readiness:** evaluate and,
   where appropriate, enable MM2 consumer group offset sync and add a
   documented failover runbook that exercises checkpoint translation.
4. **P1 - Add schema governance automation:** define compatibility
   policy in Apicurio and add CI/scripted checks for non-breaking and
   breaking schema changes.
5. **P2 - Extend validation depth:** add source-vs-sink checksum
   comparison and repeatable latency measurements (for example,
   p50/p95) under controlled load.

All backlog items should preserve the same operational boundary used in
`consistent_data_transfer_report.md`: one-way source-to-sink transfer
between isolated city instances.

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
