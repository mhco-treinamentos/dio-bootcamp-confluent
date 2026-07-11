# Oracle CDC Patterns — Deep Reference

> **Comprehensive guide to Oracle Change Data Capture with Debezium and Confluent connectors for the workshop**

---

## Oracle Architecture for CDC

### Database Modes Required

```sql
-- 1. Enable ARCHIVELOG mode (required for LogMiner)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- 2. Enable FORCE LOGGING (prevents NOLOGGING bypasses)
ALTER DATABASE FORCE LOGGING;

-- 3. Verify
SELECT LOG_MODE, FORCE_LOGGING FROM V$DATABASE;
-- Expected: ARCHIVELOG, YES
```

### Supplemental Logging

```sql
-- Minimum supplemental logging (database-level)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- ALL COLUMNS supplemental logging (per table — required for CDC)
-- BENEFICIARIOS schema (oracle.workshop.local / PAYMENTS_DB)
ALTER TABLE BENEFICIARIOS.MEMBROS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE BENEFICIARIOS.DEPENDENTES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE BENEFICIARIOS.PLANOS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE BENEFICIARIOS.COBERTURAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE BENEFICIARIOS.CARENCIAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- AUTORIZACOES schema (oracle.workshop.local / PAYMENTS_DB)
ALTER TABLE AUTORIZACOES.GUIAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE AUTORIZACOES.GUIA_ITENS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE AUTORIZACOES.AUTORIZACOES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE AUTORIZACOES.LAUDOS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- PRESTADORES schema (oracle.workshop.local / PAYMENTS_DB)
ALTER TABLE PRESTADORES.PRESTADORES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE PRESTADORES.CONTRATOS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE PRESTADORES.TABELA_PRECOS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- FATURAMENTO schema (oracle.workshop.local / PAYMENTS_DB)
ALTER TABLE FATURAMENTO.MENSALIDADES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE FATURAMENTO.COPARTICIPACOES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE FATURAMENTO.FATURAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE FATURAMENTO.FATURA_ITENS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Verify supplemental logging
SELECT lg.owner, lg.table_name, lg.log_group_name, lg.log_group_type
FROM DBA_LOG_GROUPS lg
WHERE lg.owner IN ('BENEFICIARIOS', 'AUTORIZACOES', 'PRESTADORES', 'FATURAMENTO')
ORDER BY lg.owner, lg.table_name;
```

---

## Oracle Data Type to Avro Mapping

| Oracle Type | Avro Type | Debezium Logical Type | Notes |
|-------------|-----------|----------------------|-------|
| `NUMBER(p,0)` p<=9 | `int` | `int32` | Small integers |
| `NUMBER(p,0)` p<=18 | `long` | `int64` | Large integers |
| `NUMBER(p,s)` s>0 | `bytes` | `decimal` | Monetary amounts — use `decimal.handling.mode=precise` |
| `VARCHAR2(n)` | `string` | — | Direct mapping |
| `NVARCHAR2(n)` | `string` | — | Unicode strings |
| `CHAR(n)` | `string` | — | Fixed-length (padded) |
| `CLOB` | `string` | — | Requires `lob.enabled=true` |
| `XMLTYPE` | `string` | — | TISS XML — requires special handling |
| `BLOB` | `bytes` | — | Requires `lob.enabled=true` |
| `DATE` | `int32` | `date` | Days since epoch |
| `TIMESTAMP` | `int64` | `timestamp-millis` | Milliseconds since epoch |
| `TIMESTAMP WITH TIME ZONE` | `string` | `ZonedTimestamp` | ISO 8601 string |
| `RAW(n)` | `bytes` | — | Binary data |
| `FLOAT` | `double` | — | IEEE 754 |

### Critical: Monetary Amount Handling

```json
{
  "decimal.handling.mode": "precise",
  "time.precision.mode": "connect"
}
```

**Always use `precise`** for payments amounts (valor_autorizado, valor_glosa, mensalidade, coparticipacao). The `double` mode introduces floating-point errors unacceptable for billing data.

---

## LogMiner Internals

### How LogMiner Works with Debezium

```
Oracle REDO Logs → Archive Logs → LogMiner → Debezium → Kafka

1. DML operation occurs (INSERT/UPDATE/DELETE)
2. Oracle writes to REDO log buffer → REDO log file
3. Archiver process copies REDO to archive logs
4. Debezium opens LogMiner session with SCN range
5. LogMiner reads REDO/archive logs and reconstructs SQL
6. Debezium converts SQL to change events
7. Change events published to Kafka topics
```

### Mining Strategy Comparison

| Strategy | `online_catalog` | `redo_log_catalog` |
|----------|------------------|--------------------|
| Memory | Lower | Higher |
| CPU | Lower | Higher |
| Latency | Lower | Higher |
| DDL Support | Limited | Better |
| **Recommendation** | **Most deployments** | DDL-heavy schemas |

### SCN (System Change Number)

```sql
-- Current SCN
SELECT CURRENT_SCN FROM V$DATABASE;

-- SCN at specific time
SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP) FROM DUAL;

-- SCN range in archive logs
SELECT
    SEQUENCE#,
    FIRST_CHANGE# AS start_scn,
    NEXT_CHANGE# AS end_scn,
    FIRST_TIME,
    COMPLETION_TIME,
    BLOCKS * BLOCK_SIZE / 1024 / 1024 AS size_mb
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 1
ORDER BY SEQUENCE#;
```

---

## Snapshot Strategies

| Mode | Behavior | Use Case |
|------|----------|----------|
| `initial` | Full snapshot then streaming | First deployment |
| `initial_only` | Snapshot only, no streaming | One-time export |
| `schema_only` | Schema only, stream from current | Skip historical data |
| `schema_only_recovery` | Recover schema from history topic | After connector reset |
| `when_needed` | Snapshot if offsets missing | Auto-recovery |
| `never` | No snapshot, streaming only | Resume after planned stop |

### Snapshot Locking

| Mode | Lock Type | Impact |
|------|-----------|--------|
| `shared` | ROW SHARE MODE | **Production recommended** — DML allowed |
| `exclusive` | EXCLUSIVE MODE | Blocks DML during snapshot |
| `none` | No lock | No blocking, possible inconsistency |

---

## Debezium Change Event Structure

### Envelope

```json
{
  "schema": { "..." : "..." },
  "payload": {
    "before": { "MEMBRO_ID": "MBR-001", "PLANO_ID": "PLN-BASIC", "STATUS": "ATIVO" },
    "after":  { "MEMBRO_ID": "MBR-001", "PLANO_ID": "PLN-PREMIUM", "STATUS": "ATIVO" },
    "source": {
      "version": "2.5.0",
      "connector": "oracle",
      "name": "oracle-beneficiarios",
      "ts_ms": 1706812800000,
      "snapshot": "false",
      "db": "PAYMENTS_DB",
      "schema": "BENEFICIARIOS",
      "table": "MEMBROS",
      "scn": "12345678",
      "commit_scn": "12345679"
    },
    "op": "u",
    "ts_ms": 1706812800100,
    "transaction": {
      "id": "0003.001.00000001",
      "total_order": 1,
      "data_collection_order": 1
    }
  }
}
```

### Operation Types

| `op` | Operation | `before` | `after` |
|------|-----------|----------|---------|
| `r` | Snapshot read | null | record |
| `c` | Create (INSERT) | null | record |
| `u` | Update | old record | new record |
| `d` | Delete | old record | null |

---

## Heartbeat Configuration

```sql
-- Create heartbeat table (one per Oracle instance)
CREATE TABLE BENEFICIARIOS.DEBEZIUM_HEARTBEAT (
    id  NUMBER(1) DEFAULT 1 NOT NULL,
    ts  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_heartbeat PRIMARY KEY (id)
);
GRANT SELECT, INSERT, UPDATE ON BENEFICIARIOS.DEBEZIUM_HEARTBEAT TO c##dbzuser;
ALTER TABLE BENEFICIARIOS.DEBEZIUM_HEARTBEAT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

```json
{
  "heartbeat.interval.ms": 10000,
  "heartbeat.action.query": "INSERT INTO BENEFICIARIOS.DEBEZIUM_HEARTBEAT (id, ts) VALUES (1, SYSTIMESTAMP)",
  "heartbeat.topics.prefix": "__debezium-heartbeat"
}
```

### Why Heartbeats Matter

Without heartbeats, if no changes occur for hours, archive logs may be purged before the connector advances its SCN offset, causing the connector to fail on restart.

---

## LOB Handling

| Config | CLOB/NCLOB/XMLTYPE | BLOB | Performance |
|--------|-------------------|------|------------|
| `lob.enabled=false` | Placeholder | Placeholder | None |
| `lob.enabled=true` | Full content | Full content | High (extra queries) |

**Best practice for the workshop**: Exclude large LOB columns (TISS XML, laudo documents) and fetch separately:

```json
{
  "column.exclude.list": "AUTORIZACOES.GUIAS.TISS_XML,AUTORIZACOES.LAUDOS.LAUDO_COMPLETO"
}
```

---

## Schema Evolution

### Safe DDL Operations

| DDL | Compatible | Notes |
|-----|-----------|-------|
| Add nullable column | BACKWARD | Safe — consumers ignore unknown fields |
| Add column with default | BACKWARD | Safe |
| Remove column | FORWARD only | Consumers must handle missing field |
| Rename column | BREAKING | Requires new topic version |
| Widen type (INT→BIGINT) | BACKWARD | Safe |
| Narrow type (BIGINT→INT) | BREAKING | Data loss risk |

### Column Filtering

```json
{
  "column.include.list": "BENEFICIARIOS.MEMBROS.MEMBRO_ID,BENEFICIARIOS.MEMBROS.NOME,BENEFICIARIOS.MEMBROS.CPF,BENEFICIARIOS.MEMBROS.PLANO_ID,BENEFICIARIOS.MEMBROS.STATUS",
  "column.exclude.list": "BENEFICIARIOS.MEMBROS.FOTO_DOCUMENTO",
  "column.mask.with.12.chars": "BENEFICIARIOS.MEMBROS.CPF",
  "column.mask.hash.SHA-256.with.salt.workshop2026": "BENEFICIARIOS.MEMBROS.EMAIL"
}
```

---

## Performance Tuning

### Batch Size (Throughput vs Latency)

| Size | Latency | Throughput | Use Case |
|------|---------|------------|----------|
| 1K-2K | Low | Lower | Latency-sensitive (authorization events) |
| 5K-10K | Medium | Good | **Most production** |
| 10K-50K | High | Highest | Catch-up, initial sync |

### Mining Sleep Configuration

```json
{
  "log.mining.sleep.time.min.ms": 0,
  "log.mining.sleep.time.max.ms": 3000,
  "log.mining.sleep.time.default.ms": 1000,
  "log.mining.sleep.time.increment.ms": 200
}
```

### JVM Tuning for Connect Worker

```bash
KAFKA_HEAP_OPTS="-Xms4g -Xmx8g"
KAFKA_JVM_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled"
```

---

## CDC User Setup (Complete)

```sql
-- Container Database (CDB) with Pluggable Database (PDB)
-- Run as SYSDBA

-- 1. Create common user
CREATE USER c##dbzuser IDENTIFIED BY "SecurePassword123!"
    DEFAULT TABLESPACE users
    QUOTA UNLIMITED ON users
    CONTAINER = ALL;

-- 2. Basic privileges
GRANT CREATE SESSION TO c##dbzuser CONTAINER = ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER = ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER = ALL;

-- 3. LogMiner privileges
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER = ALL;

-- 4. Supplemental logging privileges
GRANT ALTER ANY TABLE TO c##dbzuser CONTAINER = ALL;

-- 5. Heartbeat table access
GRANT INSERT, UPDATE ON BENEFICIARIOS.DEBEZIUM_HEARTBEAT TO c##dbzuser;
```

---

## Monitoring Queries

### Oracle-Side

```sql
-- REDO log generation rate (MB/hour)
SELECT
    TRUNC(first_time, 'HH24') AS hour,
    COUNT(*) AS log_switches,
    ROUND(SUM(blocks * block_size) / 1024 / 1024, 2) AS redo_mb
FROM V$ARCHIVED_LOG
WHERE first_time > SYSDATE - 1
GROUP BY TRUNC(first_time, 'HH24')
ORDER BY hour;

-- Table DML rate estimation
SELECT table_name, inserts, updates, deletes,
    inserts + updates + deletes AS total_dml
FROM DBA_TAB_MODIFICATIONS
WHERE table_owner IN ('BENEFICIARIOS', 'AUTORIZACOES', 'PRESTADORES', 'FATURAMENTO')
ORDER BY (inserts + updates + deletes) DESC;
```

### Connect-Side

```bash
# Connector status
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/status | jq .

# Restart failed task
curl -X POST http://connect:8083/connectors/oracle-cdc-beneficiarios/tasks/0/restart

# Key JMX MBeans
# debezium.oracle:type=connector-metrics,context=streaming,server=oracle-beneficiarios
#   MilliSecondsSinceLastEvent, TotalNumberOfEventsSeen,
#   QueueRemainingCapacity, SourceEventPosition (SCN)
```

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ORA-01291: missing logfile` | Archive purged | Enable heartbeat; increase retention |
| `ORA-01292: no log file` | LogMiner can't find logs | Check `V$ARCHIVED_LOG` |
| `Supplemental logging not enabled` | Missing DDL | `ALTER TABLE ADD SUPPLEMENTAL LOG DATA` |
| `SCN beyond available range` | Connector too far behind | Reset snapshot or advance SCN |
| `Memory pressure (OOM)` | Long transactions | Reduce `transaction.retention.ms`; increase heap |

### Dead Letter Queue

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "prod.saude.oracle-beneficiarios.dlq.v1",
  "errors.deadletterqueue.context.headers.enable": true,
  "errors.log.enable": true,
  "errors.log.include.messages": true
}
```

---

## Pre-CDC Checklist

```markdown
- [ ] Oracle in ARCHIVELOG mode
- [ ] FORCE LOGGING enabled
- [ ] Database-level supplemental logging enabled
- [ ] Table-level ALL COLUMNS supplemental logging on captured tables
- [ ] CDC user created with all grants
- [ ] Heartbeat table created
- [ ] REDO log sizing adequate
- [ ] Archive log retention >= 24h
- [ ] Network connectivity verified (Direct Connect / PrivateLink)
- [ ] Schema Registry accessible
- [ ] PII/PHI fields identified and tagged
```

---

**Version**: 2.0
**Last Updated**: 2026-02-16
