---
config:
  theme: neo
  look: neo
  layout: fixed
---
flowchart LR
subgraph StrimziA["Strimzi Operator"]
KA["Kafka Cluster KRaft"]
KCA["KafkaConnect Debezium Source"]
MM2["MirrorMaker2"]
end
subgraph PostgresOpA["Postgres Operator Zalando"]
PG_A["PostgreSQL Source DB"]
end
subgraph K8sA["Minikube Cluster A - Source City"]
StrimziA
PostgresOpA
end
subgraph StrimziB["Strimzi Operator"]
KB["Kafka Cluster KRaft"]
KCB["KafkaConnect JDBC Sink"]
end
subgraph PostgresOpB["Postgres Operator Zalando"]
PG_B["PostgreSQL Destination DB"]
end
subgraph Apicurio["Apicurio Registry"]
AR["Schema Registry"]
end
subgraph K8sB["Minikube Cluster B - Sink City"]
StrimziB
PostgresOpB
Apicurio
end
PG_A -- CDC --> KCA
KCA -- Avro Events --> KA
KB -- Consume --> KCB
KCB -- Upsert --> PG_B
KA -- Replicate --> MM2
MM2 -- Mirror --> KB
KCA --- AR
KCB --- AR
n1[" "]
n2[" "]
n3[" "]
n4[" "]
n5[" "]
n6[" "]
n7[" "]
n8[" "]
n9[" "]
n10[" "]

    n1@{ icon: "gcp:kuberun", pos: "b", h: 77}
    n2@{ icon: "gcp:kuberun", pos: "b", h: 77}
    n3@{ icon: "azure:azure-database-postgresql-server", pos: "b"}
    n4@{ icon: "azure:azure-database-postgresql-server", pos: "b"}
    n5@{ icon: "aws:arch-amazon-managed-streaming-for-apache-kafka", pos: "b"}
    n6@{ icon: "aws:arch-amazon-managed-streaming-for-apache-kafka", pos: "b"}
    n7@{ icon: "gcp:connectors", pos: "b"}
    n8@{ icon: "gcp:connectors", pos: "b"}
    n9@{ icon: "azure:virtual-clusters", pos: "b"}
    n10@{ icon: "azure:virtual-clusters", pos: "b"}