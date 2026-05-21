# Report-Implementation Alignment Matrix

This document summarises the highest-impact gaps between
`consistent_data_transfer_report.md` and the current repository state in
a decision-oriented format.

| Issue | Evidence (Current Repo) | Value of Incorporating Correction | Recommended Action | Priority |
| --- | --- | --- | --- | --- |
| Delivery semantics overstatement (EOS wording vs current config) | No `exactly.once.source.support` set in `poc/connect/kafka-connect-debezium.yaml`; MM2 config in `poc/kafka/mirrormaker2-source-to-sink.yaml` does not enable explicit EOS mode. | Prevents overclaiming guarantees in design reviews, audits, and DR planning. | Keep report language at at-least-once + idempotent-equivalent sink effect unless manifests/tests are upgraded. | P0 |
| Delete semantics mismatch | `poc/connect/jdbc-sink-connector.yaml` uses `transforms.unwrap.drop.tombstones: "true"` and `transforms.unwrap.delete.handling.mode: drop`. | Avoids incorrect assumption of full CRUD parity; clarifies stale-row risk. | Either implement delete propagation and tests, or explicitly document soft-delete/no-delete replication policy. | P0 |
| Offset translation / failover posture stronger than implementation | `poc/kafka/mirrormaker2-source-to-sink.yaml` includes checkpoint connector but sets `sync.group.offsets.enabled: "false"`. | Provides realistic failover readiness expectations and clearer DR scope. | Add a failover runbook; evaluate enabling group offset sync where appropriate. | P1 |
| Environment description drift | Deployments are Kubernetes-based (`scripts/deploy-source.sh`, `scripts/deploy-sink.sh`), not Docker Compose. | Restores reproducibility and reduces onboarding confusion. | Keep implementation section aligned to Minikube + Strimzi + Zalando operator flow. | P1 |
| Connector identity mismatch | Sink connector class is `io.debezium.connector.jdbc.JdbcSinkConnector` in `poc/connect/jdbc-sink-connector.yaml`. | Improves technical accuracy of connector behaviour/capability claims. | Keep report terminology as Debezium JDBC sink connector unless code changes. | P1 |
| Replication factor assumptions exceed PoC topology | Single-node nodepools in `poc/kafka/kafka-source-nodepool.yaml` and `poc/kafka/kafka-sink-nodepool.yaml`; cluster defaults in `poc/kafka/kafka-*.yaml` are effectively single-replica in PoC. | Correctly positions durability limits for this PoC and prevents HA misinterpretation. | State PoC durability limits explicitly; reserve RF=3 claims for multi-node testbed. | P1 |
| Schema compatibility enforcement not explicit | Shared registry exists (`poc/apicurio/apicurio-registry.yaml`) but explicit compatibility policy is not set in included manifests. | Distinguishes intended governance from configured governance; improves scientific rigor. | Add explicit compatibility policy setup and a schema-evolution test script. | P1 |
| Results claims exceed scripted verification depth | Current scripts (`scripts/deploy-sink.sh`, `scripts/verify.sh`) validate readiness, consumption, and sink row presence, but not checksum/failure benchmark suites. | Increases credibility by tying claims to reproducible methods. | Add checksum comparison, controlled failure-injection tests, and latency benchmark harness. | P2 |

## Priority recommendation snapshot

1. Close P0 items first (delivery semantics wording discipline and delete policy).
2. Address P1 items to improve failover rigor, technical precision, and governance clarity.
3. Implement P2 benchmarking to strengthen results in future report revisions.

