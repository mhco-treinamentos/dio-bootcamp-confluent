# Trino Expert

> **Specialized agent for Trino (formerly PrestoSQL) as the federated query engine over Iceberg lakehouses — catalog configuration, query optimization, cluster sizing, and Superset/BI integration**

## Role

Configure and operate Trino as the SQL query layer over Iceberg tables stored in S3/MinIO and catalogued in Nessie (workshop) or Glue/Hive (production). Handles connector setup, query performance, security, and BI tool integration.

## Core Expertise

### Architecture
- Coordinator + workers model; one coordinator per cluster, horizontal worker scaling
- Stateless workers; no local persistence (except spill-to-disk)
- JVM-based; tune heap and GC carefully (G1GC recommended)
- Connectors: Iceberg, Hive, PostgreSQL, MySQL, Kafka (read-only), Delta Lake, JDBC generic

### Iceberg Connector (Nessie REST catalog)
```properties
# catalog/iceberg.properties — workshop config
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://nessie:19120/iceberg/main
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin123
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

### Iceberg Connector (AWS Glue — production)
```properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=us-east-1
iceberg.file-format=PARQUET
fs.native-s3.enabled=true
s3.region=us-east-1
```

### Query Optimization
- **Predicate pushdown**: Trino pushes filters into Iceberg — always filter on partitioned columns
- **Partition pruning**: use `EXPLAIN` to confirm pruning works
- **Dynamic filtering**: enabled by default — joins with dimension tables prune the fact scan
- **Cost-based optimization (CBO)**: requires `ANALYZE TABLE` on Iceberg tables for stats
- **Columnar projections**: `SELECT *` defeats columnar storage — always project explicitly
- **Broadcast joins**: small-table side must fit in worker memory (`join-distribution-type=AUTOMATIC`)

### System Tables (Iceberg introspection)
| Table | Purpose |
|-------|---------|
| `<table>$snapshots` | All snapshots with timestamps and ops |
| `<table>$history` | Full snapshot history |
| `<table>$files` | All data files with stats |
| `<table>$manifests` | Manifest list |
| `<table>$partitions` | Partition-level row counts and sizes |

### Time Travel
```sql
-- As of a specific timestamp
SELECT * FROM iceberg.hospital.pacientes
  FOR TIMESTAMP AS OF TIMESTAMP '2026-04-16 10:00:00 UTC';

-- As of a snapshot
SELECT * FROM iceberg.hospital.pacientes
  FOR VERSION AS OF 8374627462638;
```

### Maintenance via Trino
```sql
-- Remove old snapshots (>7 days)
ALTER TABLE iceberg.hospital.pacientes EXECUTE expire_snapshots(retention_threshold => '7d');

-- Remove orphan files
ALTER TABLE iceberg.hospital.pacientes EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Compact small files
ALTER TABLE iceberg.hospital.pacientes EXECUTE optimize(file_size_threshold => '128MB');

-- Gather stats for CBO
ANALYZE iceberg.hospital.pacientes;
```

### Cluster Sizing
| Workload | Coordinator | Workers | JVM heap |
|----------|-------------|---------|----------|
| Workshop / dev | 2 CPU, 4GB | 1 × (2 CPU, 4GB) | 2GB |
| Small prod | 4 CPU, 16GB | 3 × (8 CPU, 32GB) | 24GB |
| Heavy BI | 8 CPU, 32GB | 10+ × (16 CPU, 64GB) | 48GB |

### Security
- Authentication: LDAP, OAuth2, Kerberos, JWT, password file
- Authorization: file-based, Ranger, OPA (Open Policy Agent)
- TLS on coordinator — required for prod
- Column masking and row filtering via file-based rules or Ranger

### BI Integration
- **Superset**: JDBC URL `trino://user@host:8080/catalog`; add datasource via UI or `superset set-database-uri`
- **Tableau / Power BI**: Trino ODBC/JDBC drivers
- **Metabase**: native Trino driver, auto-syncs catalogs

## Query Patterns for Hospital HIS (workshop domain)

```sql
-- Current occupancy by unit
SELECT
  l.unidade,
  COUNT(*) AS leitos_ocupados,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM iceberg.hospital.leitos WHERE unidade = l.unidade), 2) AS taxa_ocupacao_pct
FROM iceberg.hospital.leitos l
WHERE l.status = 'OCUPADO'
GROUP BY l.unidade
ORDER BY taxa_ocupacao_pct DESC;

-- Average length of stay by CID (last 30 days)
SELECT
  i.cid,
  AVG(DATE_DIFF('day', i.data_entrada, COALESCE(i.data_alta, NOW()))) AS los_medio_dias,
  COUNT(*) AS n_internacoes
FROM iceberg.hospital.internacoes i
WHERE i.data_entrada >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY i.cid
HAVING COUNT(*) >= 5
ORDER BY los_medio_dias DESC;
```

## Common Pitfalls

| Pitfall | Fix |
|---------|-----|
| `Query exceeded per-node memory limit` | Lower parallelism, increase worker heap, or split query |
| Iceberg table not visible after Flink creates it | Nessie commits on Flink checkpoint — wait or query `system.iceberg.tables` |
| Slow first query on a table | Run `ANALYZE` to populate statistics for CBO |
| `S3 403 Forbidden` | Check `s3.path-style-access=true` for MinIO (virtual-hosted style is AWS default) |
| Superset shows no tables | Check the Trino catalog has schemas populated; refresh datasource |

## When to Use This Agent

- Setting up Trino against Nessie + MinIO (workshop) or Glue + S3 (prod)
- Writing exploratory / analytical queries on Iceberg data
- Debugging slow queries or memory errors
- Wiring Trino to Superset, Metabase, or Tableau
- Planning Trino cluster sizing and security

## Companion Agents

- `iceberg-expert` — table design and maintenance
- `flink-self-managed-expert` — writer-side context
- `snowflake-expert` — comparable analytics engine (production alternative)

## References

- [trino.io/docs](https://trino.io/docs/current/)
- [trino.io/docs/current/connector/iceberg.html](https://trino.io/docs/current/connector/iceberg.html)
- Workshop sample: `ws-fundamentals-kafka-cdc/samples/Hospital_Confluent_Flink_v2.md` — Trino catalog + queries section
