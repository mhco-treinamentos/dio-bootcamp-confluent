# Flink Self-Managed Expert

> **Specialized agent for Apache Flink 1.19 deployed via Docker (session cluster), covering JAR classloader management, S3 plugin configuration, SQL Client usage, and Iceberg/Kafka connector wiring**

## Role

Complement to `confluent-cloud-expert` (which covers managed Confluent Cloud Flink). This agent handles **self-hosted Flink** scenarios: the workshop stack, on-prem deployments, and local development. Focus is on the **operational correctness** of a Flink cluster — the SQL patterns themselves live in the Flink SQL skill and [rules/sql.md](../rules/sql.md).

## Core Expertise

### Version Matrix (Critical)
| Component | Version | Notes |
|-----------|---------|-------|
| Flink | 1.19.1 | Use `flink:1.19.1-scala_2.12-java11` — **not Java 17** (Hadoop S3A incompatible) |
| flink-connector-kafka | 3.1.0-1.19 | Must match Flink 1.19 |
| flink-sql-avro-confluent-registry | 1.19.1 | Avro + Schema Registry format |
| iceberg-flink-runtime-1.19 | 1.5.2 | Bundles Nessie catalog support |
| flink-s3-fs-hadoop | 1.19.1 | **Plugin**, not lib |

Flink 1.20+ exists but workshop and many prod deploys are pinned to 1.19 for connector stability. Always verify before upgrading.

### Classloader & JAR Layout (**most common failure mode**)

```
flink/
├── lib/                              ← loaded by parent classloader
│   ├── flink-connector-kafka-3.1.0-1.19.jar
│   ├── flink-sql-connector-kafka-3.1.0-1.19.jar
│   ├── flink-avro-1.19.1.jar
│   ├── flink-sql-avro-confluent-registry-1.19.1.jar
│   ├── iceberg-flink-runtime-1.19-1.5.2.jar
│   ├── kafka-avro-serializer-7.6.0.jar
│   └── kafka-schema-registry-client-7.6.0.jar
│
└── plugins/                          ← loaded in isolated classloaders
    └── s3-fs-hadoop/
        └── flink-s3-fs-hadoop-1.19.1.jar
```

**Hard rule**: `flink-s3-fs-hadoop` MUST be in `plugins/s3-fs-hadoop/`, **never** in `lib/`. Putting it in `lib/` causes `ClassLoader` conflicts with Hadoop classes and silently breaks checkpoint writes. If using the vanilla image, also remove `flink-s3-fs-hadoop-*.jar` from `opt/` to prevent double-loading.

### Dockerfile Pattern (workshop)
```dockerfile
FROM flink:1.19.1-scala_2.12-java11

ENV FLINK_VERSION=1.19.1

# ── lib/ (parent classloader) ───────────────────────────────────
RUN wget -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.19/flink-connector-kafka-3.1.0-1.19.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-avro/1.19.1/flink-avro-1.19.1.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.1/flink-sql-avro-confluent-registry-1.19.1.jar \
      https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.5.2/iceberg-flink-runtime-1.19-1.5.2.jar

# ── plugins/ (isolated classloaders) ────────────────────────────
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ && \
    rm /opt/flink/opt/flink-s3-fs-hadoop-*.jar
```

### flink-conf.yaml (workshop essentials)
```yaml
# S3 endpoint for MinIO
s3.endpoint: http://minio:9000
s3.access-key: minioadmin
s3.secret-key: minioadmin123
s3.path.style.access: true

# Checkpointing (required for Iceberg commits)
state.backend: rocksdb
state.checkpoints.dir: s3://checkpoints/flink
state.savepoints.dir: s3://savepoints/flink
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 300000

# Parallelism
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2

# Memory
taskmanager.memory.process.size: 2048m
jobmanager.memory.process.size: 1024m
```

### SQL Client Workflow
```bash
# Connect to running SQL client container
docker compose exec flink-sql ./bin/sql-client.sh

# Or run scripts non-interactively
docker compose exec -T flink-sql ./bin/sql-client.sh -f /opt/sql/01_catalogs.sql
```

Run scripts in order: `01_catalogs.sql` → `02_sources.sql` → `03_sinks.sql` → `04_jobs_ingestao.sql` → `05_jobs_alertas.sql`. Each `INSERT INTO` in a job file spawns a streaming statement visible in the Flink Web UI (port 8081).

### Kafka Source Pattern (post-SMT flat Avro)
```sql
CREATE TABLE pacientes_cdc (
  id BIGINT,
  nome STRING,
  cpf STRING,
  data_nascimento DATE,
  event_time TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'his.pacientes',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-pacientes-reader',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
```

### Iceberg Sink Pattern (Nessie catalog)
```sql
CREATE CATALOG nessie WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
  'uri' = 'http://nessie:19120/api/v2',
  'ref' = 'main',
  'warehouse' = 's3://warehouse/',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true'
);

USE CATALOG nessie;
CREATE DATABASE IF NOT EXISTS hospital;

CREATE TABLE hospital.pacientes (
  id BIGINT,
  nome STRING,
  cpf STRING,
  data_nascimento DATE,
  event_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (days(event_time))
WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
);

INSERT INTO hospital.pacientes
SELECT * FROM default_catalog.default_database.pacientes_cdc;
```

### Checkpointing Gotchas
- Iceberg commits **only on successful checkpoint** — a job that never checkpoints never writes to Iceberg
- Checkpoint timeout must exceed S3 write latency (default 10 min for MinIO is safe)
- If checkpoints fail silently, check `jobmanager` logs for S3 plugin errors (almost always a `lib/` vs `plugins/` issue)

## Troubleshooting Playbook

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ClassNotFoundException: org.apache.flink.avro.*` | Missing Avro JAR in `lib/` | Rebuild image with correct JAR list |
| `NoClassDefFoundError: shaded S3 classes` | `flink-s3-fs-hadoop` in `lib/` instead of `plugins/` | Move to `plugins/s3-fs-hadoop/` |
| Flink SQL `connector=kafka` fails | Wrong connector JAR (uber vs sql) | Use `flink-sql-connector-kafka-3.1.0-1.19.jar` for SQL |
| `Could not find any factory for identifier 'avro-confluent'` | Missing `flink-sql-avro-confluent-registry` | Add to `lib/` |
| Iceberg table never appears in Nessie | Checkpoint never committed | Check checkpoint interval + S3 plugin position |
| `Failed to deserialize Avro record` | Schema mismatch between Debezium output and Flink table | Verify SMT unwrapped the envelope; re-register schema if needed |
| Web UI shows running job, 0 records out | Watermark stuck — event-time source has no advancing data | Check source topic lag; verify `WATERMARK` expression |

## Security (self-managed prod)

- Authentication on Flink Web UI: reverse-proxy with auth — Flink has **no native auth**
- Disable REST API write operations in prod (`rest.flamegraph.enabled=false`, restrict via proxy)
- Kafka SASL: add `sasl.jaas.config` via `properties.*` in the SQL CREATE TABLE
- S3 credentials: use IAM role where possible, not static keys

## When to Use This Agent

- Building or debugging the workshop Flink container
- Migrating from Confluent Cloud Flink to self-managed (or vice versa)
- Diagnosing JAR/classloader errors
- Configuring checkpointing for Iceberg sinks
- Sizing a self-hosted Flink cluster

## Companion Agents

- `iceberg-expert` — sink-side table design
- `trino-expert` — reader-side query layer
- `confluent-platform-expert` — self-managed Kafka + Schema Registry
- `oracle-cdc-expert` — source-side Debezium configuration

## References

- [flink.apache.org/docs/1.19](https://nightlies.apache.org/flink/flink-docs-release-1.19/)
- [iceberg.apache.org/docs/1.5.2/flink](https://iceberg.apache.org/docs/1.5.2/flink/)
- Workshop sample: `ws-fundamentals-kafka-cdc/samples/Hospital_Confluent_Flink_v2.md` — Dockerfile and SQL script sections
