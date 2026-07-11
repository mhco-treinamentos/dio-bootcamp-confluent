# Workshop Stack Guide — Hospital CDC Fundamentals

> **The "why" behind every architectural choice in `ws-fundamentals-kafka-cdc/samples/`, and how each choice maps to the production the workshop platform.**

This workshop teaches the **same conceptual pipeline** as the production the workshop platform — `Oracle → CDC → Kafka → Flink → Lakehouse → BI` — using open-source, single-node equivalents for every managed service. Attendees build the entire stack in Docker on a laptop.

---

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WORKSHOP STACK (laptop)                               │
│                                                                              │
│  Oracle XE 21c  ─CDC─►  Kafka Connect  ─►  Kafka  ─►  Flink SQL              │
│  (Docker)                (Debezium 2.5)   (Confluent  (1.19 self-managed)    │
│                                            Community  │                      │
│                                            7.6.0)     ├─► Iceberg (Nessie)   │
│                                                        │         │           │
│                                                        │      MinIO (S3)     │
│                                                        │         │           │
│                                                        └─► Trino 435 ─► Superset │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Workshop ↔ Production Mapping

| Layer | Workshop | Production (the workshop) | Why different |
|-------|----------|------------------------|---------------|
| **Source** | Oracle XE 21c in Docker | Oracle 19c on-prem | Same CDC mechanics (LogMiner), XE is free and fits in one container |
| **CDC connector** | Debezium Oracle 2.5.4 | Same (Debezium) or Confluent Oracle CDC Source | Debezium is open source; prod may use Confluent's managed version |
| **Kafka** | Confluent Community 7.6.0 (self-managed) | Confluent Cloud Dedicated | Same API surface; Cloud adds SLA + PrivateLink + Stream Governance |
| **Schema Registry** | Confluent Schema Registry 7.6.0 | Confluent Cloud Schema Registry (Advanced) | Same API; Cloud adds Data Contracts + Stream Governance tags |
| **Stream processor** | Flink 1.19.1 (self-managed, SQL Client) | Confluent Cloud Flink (compute pools) | Same Flink SQL; Cloud manages infra and adds `ML_PREDICT` |
| **Catalog** | Nessie 0.79 (REST, Git-like branches) | AWS Glue or Polaris | Nessie is a single container; Glue is regional, serverless, IAM-integrated |
| **Object store** | MinIO (S3-compatible, local) | AWS S3 | Same S3 API; MinIO runs in Docker |
| **Table format** | Apache Iceberg 1.5.2 | Same (Iceberg) or Snowflake native | Iceberg is the open-source choice at both tiers |
| **Query engine** | Trino 435 | Snowflake | Both query Iceberg; Snowflake has native storage too, Trino is external |
| **BI** | Apache Superset | PowerBI / Tableau / Superset | Superset runs in one container; prod BI tools are vendor-managed |
| **Orchestration** | `docker compose up` | Terraform + ECS/EKS | Workshop = single `docker compose`; prod = immutable IaC |

---

## Critical Architecture Decisions (from the samples)

The workshop samples document these decisions explicitly — this is the reasoning behind each:

### 1. `ExtractNewRecordState` SMT on Debezium
**What**: Unwrap the Debezium envelope (`{before, after, op, ts_ms, source}`) into a flat row on the Kafka topic.

**Why**: Flink reads a flat Avro schema — massively simpler SQL. You lose `op='d'` and `ts_ms` unless you route them explicitly, which is an acceptable trade for teaching.

**Production delta**: Production keeps the envelope to enable delete-propagation and precise event-time semantics. The workshop sacrifices this for pedagogical clarity.

### 2. Nessie as Iceberg Catalog (not Hive Metastore)
**What**: Catalog runs as a single container with a REST API.

**Why**: Hive Metastore requires the full Hadoop stack (MySQL/Postgres + HMS + auth). Nessie is one process, one port, zero Hadoop. Plus Nessie adds Git-like branches — useful for teaching schema evolution.

**Production delta**: Prod uses AWS Glue (managed, region-scoped, IAM) or Snowflake Polaris. Nessie's branching features are rarely used in prod.

### 3. `flink-s3-fs-hadoop` as a Plugin (not in `lib/`)
**What**: S3 filesystem JAR goes in `plugins/s3-fs-hadoop/`, not the main `lib/`.

**Why**: Flink's plugin classloader isolates Hadoop classes. Putting this JAR in `lib/` causes `ClassLoader` conflicts that silently break checkpoint writes. This is the single most common workshop failure mode.

**Production delta**: Same principle applies, but managed Flink handles the JAR layout for you.

### 4. Avro + Schema Registry (not JSON)
**What**: All Kafka topics use Avro encoded via Confluent Schema Registry.

**Why**: Typed, compact, evolvable. JSON works but explodes in size and has no type safety. Schema Registry makes evolution enforceable.

**Production delta**: Same — this is a shared choice between workshop and prod.

### 5. Kafka Connect with SMT **Before** Flink
**What**: Data transformation happens in Kafka Connect (via SMT) before reaching Flink.

**Why**: Separation of concerns. Connect handles the CDC envelope (an infrastructure concern); Flink handles business logic (aggregation, enrichment, alerting). Keeping these separated makes each layer replaceable.

**Production delta**: Same pattern. Flink never sees the Debezium envelope in either tier.

---

## Version Matrix (Critical Compatibility Constraints)

| Component | Pinned version | Why this version |
|-----------|----------------|------------------|
| Confluent Community | 7.6.0 | Ships with Kafka 3.6.x, Schema Registry 7.6.0, Connect 7.6.0 — all aligned |
| Debezium Oracle | 2.5.4 | Only version that works stably with Oracle XE 21c LogMiner |
| Flink | 1.19.1 | Must use Java 11 image (`flink:1.19.1-scala_2.12-java11`) — Java 17 has Hadoop S3A incompatibilities |
| flink-connector-kafka | 3.1.0-1.19 | Matched pair for Flink 1.19 |
| iceberg-flink-runtime-1.19 | 1.5.2 | Bundles Nessie catalog support; later versions have breaking API changes |
| Nessie | 0.79.0 | REST catalog compatible with Iceberg 1.5.x |
| Trino | 435 | Native support for Iceberg + Nessie REST catalog |

**Don't upgrade casually** — the version compatibility matrix is narrow. Test in a scratch branch first.

---

## Minimum Resource Requirements

| Resource | Workshop total | Notes |
|----------|----------------|-------|
| RAM | 12 GB free | Oracle XE alone uses 3.5 GB; Flink JobManager + TaskManager ~3 GB; Kafka + Connect ~2 GB; Trino + Nessie + MinIO ~2 GB |
| Disk | 20 GB free | Docker images (~8 GB) + Iceberg data (~5 GB per scene) + logs |
| CPU | 4 cores recommended | 2 cores will work but Flink jobs are slow |
| Docker Desktop memory limit | ≥ 10 GB | Below this, Oracle XE OOMs during startup |

---

## The Hospital HIS Domain

The workshop uses a hospital information system as the source domain, not payments. The mapping to the workshop:

| Hospital HIS | the workshop | Relationship |
|--------------|-----------|--------------|
| `pacientes` | `beneficiarios` | Both are people with CPF/CNS |
| `leitos` | (no direct equivalent) | Bed occupancy — unique to hospitals |
| `internacoes` | (partial: `autorizacoes` → internação) | Hospital stays are authorized by insurance |
| `altas` | (closing event in autorizacoes) | Discharge events |
| `exames` (TISS) | `procedimentos` (Faturamento) | Procedure coding — **same TISS/TUSS codes** |
| `CID-10` | `CID-10` | **Identical** — international standard |

The workshop teaches the CDC + streaming + lakehouse pattern. The domain is "hospital-shaped" because it's easier to visualize bed occupancy than insurance claim flows, and because it avoids PII concerns (the workshop uses synthetic data only).

---

## Scene Flow (from `run_demo.sh`)

The workshop is organized around **scenes** — discrete pedagogical units, each with a clear outcome:

1. **Setup** — Start Oracle + Confluent + Flink + Iceberg + Trino + Superset stack
2. **Seed & simulate** — Populate Oracle with baseline data; run Python simulator
3. **CDC wiring** — Register Debezium connector; watch CDC topics populate
4. **Streaming ingestion** — Flink SQL reads Kafka, writes Iceberg via Nessie
5. **Querying** — Trino queries Iceberg; observe snapshot history
6. **Real-time alerts** — Flink SQL detects high-occupancy units, publishes alerts
7. **BI** — Superset dashboard over Trino catalog
8. **Failure injection** — Stop Debezium, observe Flink backlog, restart and catch up

Each scene is time-boxed to ≤ 25 minutes (see [rules/workshop.md](../rules/workshop.md)).

---

## What the `.claude/` Covers for This Workshop

| Layer | Agent/Skill/Rule |
|-------|------------------|
| Oracle CDC setup | [oracle-cdc-expert](../agents/oracle-cdc-expert.md), [oracle-database-expert](../agents/oracle-database-expert.md) |
| Debezium + SMT | [skills/confluent/oracle-cdc-connector.md](../skills/confluent/oracle-cdc-connector.md), [skills/confluent/connect.md](../skills/confluent/connect.md) |
| Self-managed Kafka / Connect / Schema Registry | [confluent-platform-expert](../agents/confluent-platform-expert.md), [skills/confluent/docker-compose.md](../skills/confluent/docker-compose.md) |
| Self-managed Flink | [flink-self-managed-expert](../agents/flink-self-managed-expert.md) |
| Iceberg table design | [iceberg-expert](../agents/iceberg-expert.md) |
| Trino queries | [trino-expert](../agents/trino-expert.md) |
| Workshop pedagogy | [rules/workshop.md](../rules/workshop.md) |

**What's intentionally NOT covered** (accepted gap for MVP):
- Nessie-specific branching workflow (covered by `iceberg-expert` at a high level)
- MinIO ops (use AWS docs or `mc` CLI directly — low risk)
- Superset dashboard-building (use Superset docs; scene 7 is UI-driven)

---

## When to Consult This Guide

- Before designing a new workshop scene — confirm it fits the pedagogical rules and time budget
- When debating workshop vs prod architecture choices
- When an attendee hits a version/compatibility issue
- When adapting the workshop for a different domain (swap `hospital` schema; keep the pipeline)
