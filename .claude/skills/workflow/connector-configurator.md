# Connector Configurator

## Skill: `/connector-configurator`

Configure Kafka Connect connectors with proper settings, Single Message Transforms (SMTs), error handling, and monitoring. Covers Oracle CDC source, JDBC sink, S3 sink, and Snowflake sink connectors for the workshop's Confluent Cloud platform.

## Usage

```
/connector-configurator <connector-type> <domain> [options]
```

### Connector Types
- `oracle-cdc` - Debezium Oracle CDC Source
- `confluent-oracle-cdc` - Confluent managed Oracle CDC Source
- `jdbc-sink` - JDBC Sink for data warehouse
- `s3-sink` - S3 Sink for data lake
- `snowflake-sink` - Snowflake Sink for semantic layer

### Options
- `--managed` - Use Confluent Cloud managed connector
- `--self-managed` - Use self-managed (Connect worker on ECS)
- `--env` - Environment: dev, staging, prod
- `--schema` - Oracle schema: BENEFICIARIOS, AUTORIZACOES, PRESTADORES, FATURAMENTO

---

## Oracle CDC Source (Debezium)

### Beneficiários Connector

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

    "schema.include.list": "BENEFICIARIOS",
    "table.include.list": "BENEFICIARIOS.MEMBROS,BENEFICIARIOS.DEPENDENTES,BENEFICIARIOS.PLANOS,BENEFICIARIOS.COBERTURAS,BENEFICIARIOS.CARENCIAS",

    "topic.prefix": "oracle-beneficiarios",
    "database.server.name": "oracle-beneficiarios",

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "none",

    "log.mining.strategy": "online_catalog",
    "log.mining.batch.size.min": 1000,
    "log.mining.batch.size.max": 10000,
    "log.mining.sleep.time.min.ms": 0,
    "log.mining.sleep.time.max.ms": 3000,
    "log.mining.sleep.time.default.ms": 1000,
    "log.mining.sleep.time.increment.ms": 200,

    "decimal.handling.mode": "precise",
    "time.precision.mode": "connect",

    "heartbeat.interval.ms": 10000,
    "heartbeat.action.query": "INSERT INTO BENEFICIARIOS.DEBEZIUM_HEARTBEAT (id, ts) VALUES (1, SYSTIMESTAMP)",

    "schema.history.internal.kafka.topic": "_schemas.oracle-beneficiarios.history",
    "schema.history.internal.kafka.bootstrap.servers": "${BOOTSTRAP_SERVERS}",

    "provide.transaction.metadata": true,
    "include.schema.changes": true,

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",

    "transforms": "unwrap,route,insertMeta",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms,source.scn",

    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "oracle-beneficiarios\\.BENEFICIARIOS\\.(.*)",
    "transforms.route.replacement": "prod.saude.oracle-beneficiarios.$1.cdc.v1",

    "transforms.insertMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.insertMeta.timestamp.field": "kafka_ingested_at",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.saude.oracle-beneficiarios.dlq.v1",
    "errors.deadletterqueue.context.headers.enable": true,
    "errors.log.enable": true,
    "errors.log.include.messages": true
  }
}
```

---

## Connector Per Oracle Schema Strategy

| Connector Name | Oracle Schema | Tables | Topic Prefix |
|---------------|--------------|--------|--------------|
| `oracle-cdc-beneficiarios` | BENEFICIARIOS | MEMBROS, DEPENDENTES, PLANOS, COBERTURAS, CARENCIAS | `prod.saude.oracle-beneficiarios` |
| `oracle-cdc-autorizacoes` | AUTORIZACOES | GUIAS, GUIA_ITEMS, AUTORIZACOES, LAUDOS | `prod.saude.oracle-autorizacoes` |
| `oracle-cdc-prestadores` | PRESTADORES | PRESTADORES, CONTRATOS, TABELAS_NEGOCIADAS, CREDENCIAMENTOS | `prod.saude.oracle-prestadores` |
| `oracle-cdc-faturamento` | FATURAMENTO | FATURAS, MENSALIDADES, COPARTICIPACOES, GLOSAS | `prod.saude.oracle-faturamento` |

---

## SMT Patterns

### Unwrap + Route + Mask + Metadata (Full Chain)

```json
"transforms": "unwrap,route,maskPii,insertMeta",

"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.add.fields": "op,source.ts_ms",

"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "oracle-beneficiarios\\.BENEFICIARIOS\\.(.*)",
"transforms.route.replacement": "prod.saude.oracle-beneficiarios.$1.cdc.v1",

"transforms.maskPii.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.maskPii.fields": "cpf,cns,email,telefone",
"transforms.maskPii.replacement": "[MASKED]",

"transforms.insertMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.insertMeta.timestamp.field": "kafka_ingested_at"
```

### Flatten Nested Structures

```json
"transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
"transforms.flatten.delimiter": "_"
```

### Filter by Operation Type

```json
"transforms": "filterDeletes",
"transforms.filterDeletes.type": "org.apache.kafka.connect.transforms.Filter",
"transforms.filterDeletes.predicate": "isDelete",
"predicates": "isDelete",
"predicates.isDelete.type": "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
"predicates.isDelete.name": "__debezium.op"
```

---

## S3 Sink Connector

### Data Lake Export

```json
{
  "name": "s3-sink-enriched-guias",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",

    "topics": "prod.saude.flink.guias.enriched.v1",

    "s3.region": "us-east-1",
    "s3.bucket.name": "workshop-data-lake-prod",
    "topics.dir": "raw/saude/guias",

    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "partition.duration.ms": "3600000",
    "locale": "pt_BR",
    "timezone": "America/Sao_Paulo",

    "flush.size": "10000",
    "rotate.interval.ms": "3600000",
    "rotate.schedule.interval.ms": "3600000",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",

    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "store.url": "https://s3.us-east-1.amazonaws.com",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.saude.s3-sink.dlq.v1",
    "errors.deadletterqueue.context.headers.enable": true
  }
}
```

---

## JDBC Sink Connector

### Data Warehouse Export

```json
{
  "name": "jdbc-sink-enriched-guias",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "topics": "prod.saude.flink.guias.enriched.v1",

    "connection.url": "${JDBC_URL}",
    "connection.user": "${JDBC_USER}",
    "connection.password": "${JDBC_PASSWORD}",

    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "guia_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "table.name.format": "enriched_guias",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",

    "batch.size": "3000",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.saude.jdbc-sink.dlq.v1",
    "errors.deadletterqueue.context.headers.enable": true
  }
}
```

---

## Error Handling Configuration

### Standard DLQ Setup

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "<topic-prefix>.dlq.v1",
"errors.deadletterqueue.topic.replication.factor": 3,
"errors.deadletterqueue.context.headers.enable": true,
"errors.log.enable": true,
"errors.log.include.messages": true,
"errors.retry.delay.max.ms": "60000",
"errors.retry.timeout": "300000"
```

---

## Health Monitoring

### REST API Commands

```bash
# Check connector status
curl -s http://connect:8083/connectors/<name>/status | jq '{
  connector: .connector.state,
  tasks: [.tasks[] | {id: .id, state: .state}]
}'

# Restart failed task
curl -X POST http://connect:8083/connectors/<name>/tasks/0/restart

# Pause / Resume
curl -X PUT http://connect:8083/connectors/<name>/pause
curl -X PUT http://connect:8083/connectors/<name>/resume

# All connectors health
for c in $(curl -s http://connect:8083/connectors | jq -r '.[]'); do
  echo "$c: $(curl -s http://connect:8083/connectors/$c/status | jq -r '.connector.state')"
done
```

### Confluent CLI (Managed Connectors)

```bash
confluent connect cluster list --cluster <cluster-id>
confluent connect cluster describe <connector-id> --cluster <cluster-id>
```

---

## Configuration Checklist

```markdown
For every connector:

Connection:
- [ ] Credentials via environment variables (never hardcoded)
- [ ] Network connectivity verified (PrivateLink for Cloud)
- [ ] Minimum required privileges for database user

Data Quality:
- [ ] Key + value converters set to Avro
- [ ] Schema Registry URL and credentials configured
- [ ] decimal.handling.mode = precise (payments financial data)

Error Handling:
- [ ] DLQ topic configured with context headers
- [ ] Error tolerance set appropriately
- [ ] Error logging enabled

Topic Naming:
- [ ] SMTs route to the workshop naming convention
- [ ] DLQ follows naming convention

Performance:
- [ ] Batch size tuned for throughput
- [ ] tasks.max set (1 for CDC, higher for sinks)

Compliance:
- [ ] PII/PHI fields masked at connector level if needed
- [ ] Heartbeat enabled for CDC connectors
- [ ] Transaction metadata enabled for audit
```

---

**Version**: 1.0
**Last Updated**: 2026-02-12
