# Oracle CDC Connector Skill

## Skill: `/confluent-oracle-cdc`

Deploy and manage Oracle CDC Source connectors (Debezium) for streaming Oracle XE changes to a local Confluent Platform / Kafka cluster running in Docker Compose.

## Usage

```
/confluent-oracle-cdc <action> [options]
```

### Actions
- `deploy-debezium` - Deploy Debezium Oracle CDC connector (self-managed)
- `deploy-managed` - Deploy Confluent Oracle CDC Source (managed)
- `status` - Check connector status and lag
- `snapshot` - Trigger or monitor initial snapshot
- `troubleshoot` - Debug common Oracle CDC issues

---

## Deployment Option 1: Debezium Oracle (Self-Managed)

### Prerequisites

```bash
# 1. Oracle Database preparation (run by DBA)
# Enable ARCHIVELOG mode
# Enable supplemental logging
# Create CDC user with grants

# 2. Download Oracle JDBC driver (not included in Debezium)
# ojdbc8.jar must be placed in Connect plugin path

# 3. Install Debezium Oracle connector
confluent-hub install debezium/debezium-connector-oracle:2.5.0
```

### Connector Configuration

```json
{
  "name": "oracle-cdc-beneficiarios",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname": "oracle.workshop.local",
    "database.port": "1521",
    "database.user": "${ORACLE_CDC_USER}",
    "database.password": "${ORACLE_CDC_PASSWORD}",
    "database.dbname": "PAYMENTS_DB",
    "database.pdb.name": "XEPDB1",

    "topic.prefix": "oracle-beneficiarios",
    "schema.include.list": "BENEFICIARIOS",
    "table.include.list": "BENEFICIARIOS.MEMBROS,BENEFICIARIOS.DEPENDENTES,BENEFICIARIOS.PLANOS,BENEFICIARIOS.COBERTURAS,BENEFICIARIOS.CARENCIAS",
    "column.exclude.list": "BENEFICIARIOS.MEMBROS.INTERNAL_NOTES",

    "log.mining.strategy": "online_catalog",
    "log.mining.batch.size.default": 5000,
    "log.mining.sleep.time.default.ms": 1000,

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "none",

    "decimal.handling.mode": "precise",
    "time.precision.mode": "adaptive_time_microseconds",

    "database.history.kafka.bootstrap.servers": "${CONFLUENT_BOOTSTRAP}",
    "database.history.kafka.topic": "_debezium.oracle-beneficiarios.schema-history",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",

    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms,source.scn",

    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "oracle-beneficiarios\\.BENEFICIARIOS\\.(.*)",
    "transforms.route.replacement": "prod.saude.oracle-beneficiarios.$1.cdc.v1",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.saude.oracle-beneficiarios.dlq.v1",
    "errors.deadletterqueue.context.headers.enable": true,

    "heartbeat.interval.ms": 10000,
    "provide.transaction.metadata": true
  }
}
```

### Deploy via REST API

```bash
# Deploy connector
curl -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @oracle-cdc-beneficiarios.json

# Check status
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/status | jq

# Restart if failed
curl -X POST http://connect:8083/connectors/oracle-cdc-beneficiarios/restart
```

---

## Deployment Option 2: Confluent Oracle CDC Source (Managed)

### Confluent CLI

```bash
# Create managed connector
confluent connect cluster create \
  --cluster <cluster-id> \
  --config-file oracle-cdc-managed.json

# List connectors
confluent connect cluster list --cluster <cluster-id>

# Describe connector
confluent connect cluster describe <connector-id> --cluster <cluster-id>

# Pause connector
confluent connect cluster pause <connector-id> --cluster <cluster-id>

# Resume connector
confluent connect cluster resume <connector-id> --cluster <cluster-id>
```

### Managed Connector Configuration

```json
{
  "connector.class": "OracleCdcSource",
  "name": "oracle-cdc-beneficiarios-managed",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "<SA_ID>",

  "oracle.server": "oracle.workshop.local",
  "oracle.port": "1521",
  "oracle.sid": "PAYMENTS_DB",
  "oracle.username": "${ORACLE_CDC_USER}",
  "oracle.password": "${ORACLE_CDC_PASSWORD}",

  "table.inclusion.regex": "BENEFICIARIOS\\.(MEMBROS|DEPENDENTES|PLANOS|COBERTURAS|CARENCIAS)",
  "start.from": "snapshot",

  "output.data.format": "AVRO",
  "output.key.format": "AVRO",

  "db.timezone": "America/Sao_Paulo",
  "topic.prefix": "prod.saude.oracle-beneficiarios",

  "max.batch.size": "1000",
  "poll.interval.ms": "1000",

  "emit.tombstones.on.delete": "true",
  "tasks.max": "1"
}
```

---

## Multiple Connector Strategy (the workshop)

```
CONNECTOR PER ORACLE SCHEMA
═══════════════════════════════════════════
oracle-cdc-beneficiarios
  └── Source: BENEFICIARIOS schema
  └── Tables: MEMBROS, DEPENDENTES, PLANOS, COBERTURAS, CARENCIAS
  └── Topics: prod.saude.oracle-beneficiarios.*.cdc.v1

oracle-cdc-autorizacoes
  └── Source: AUTORIZACOES schema
  └── Tables: GUIAS, GUIA_ITEMS, AUTORIZACOES, LAUDOS
  └── Topics: prod.saude.oracle-autorizacoes.*.cdc.v1

oracle-cdc-prestadores
  └── Source: PRESTADORES schema
  └── Tables: PRESTADORES, CONTRATOS, TABELAS_NEGOCIADAS, CREDENCIAMENTOS
  └── Topics: prod.saude.oracle-prestadores.*.cdc.v1

oracle-cdc-faturamento
  └── Source: FATURAMENTO schema
  └── Tables: FATURAS, MENSALIDADES, COPARTICIPACOES, GLOSAS
  └── Topics: prod.saude.oracle-faturamento.*.cdc.v1
```

---

## Snapshot Management

### Initial Snapshot Strategies

| Strategy | Duration (100M rows) | Lock | Use Case |
|----------|---------------------|------|----------|
| `initial` | Hours | None | First deployment |
| `initial_only` | Hours | None | One-time migration |
| `schema_only` | Seconds | None | Table already in Kafka |
| `no_data` | Seconds | None | Resume from SCN |

### Monitor Snapshot Progress

```bash
# Check connector status during snapshot
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/status | jq '.tasks[0].trace'

# Check Connect logs for snapshot progress
docker logs connect 2>&1 | grep -i "snapshot"

# Monitor topic message count during snapshot
confluent kafka topic consume prod.saude.oracle-beneficiarios.MEMBROS.cdc.v1 \
  --cluster <cluster-id> \
  --from-beginning \
  --print-key \
  --value-format avro \
  --limit 5
```

---

## Troubleshooting

### Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Missing supplemental logging | Empty `before` field in CDC events | Enable ALL columns supplemental logging on table |
| Archive log gap | Connector FAILED, SCN not found | Ensure archive log retention covers connector downtime |
| ORA-01291 | Missing log file | Increase archive log retention, reduce connector downtime |
| Oracle user permissions | Access denied | Grant LOGMINING, SELECT on V$ views |
| LOB column errors | Serialization failures | Exclude LOB columns or configure LOB handling |
| Schema drift | Compatibility error in SR | Use BACKWARD compatibility, add fields as optional |
| High latency | Growing CDC lag | Increase `log.mining.batch.size.max`, check REDO log volume |
| Duplicate records | Re-delivery after restart | Enable idempotent consumers, use `exactly_once` in Flink |

### Diagnostic Commands

```bash
# Check Oracle CDC connector health
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/status | jq '{
  state: .connector.state,
  worker: .connector.worker_id,
  tasks: [.tasks[] | {id: .id, state: .state, trace: .trace}]
}'

# Check SCN position (CDC offset)
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/offsets | jq

# Check connector metrics
curl -s http://connect:8083/connectors/oracle-cdc-beneficiarios/tasks/0/status | jq

# Check DLQ messages
confluent kafka topic consume prod.saude.oracle-beneficiarios.dlq.v1 \
  --cluster <cluster-id> \
  --from-beginning \
  --print-key \
  --limit 10
```

---

## Best Practices

1. **One connector per Oracle schema**: Isolate failure domains
2. **Always configure DLQ**: Capture and analyze failed records
3. **Heartbeat enabled**: Prevent stale offsets on idle tables
4. **Snapshot during off-peak**: Schedule initial snapshot during low traffic
5. **Monitor SCN lag**: Alert when CDC lag exceeds threshold
6. **Archive log retention**: Minimum 24h, ideally 48h for maintenance windows
7. **Column exclusions**: Exclude unnecessary LOB/BLOB columns
8. **Schema evolution**: Always BACKWARD compatible changes
