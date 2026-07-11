# Kafka Connect Connectors Skill

## Skill: `/confluent-connect`

Deploy and manage Kafka Connect connectors for data integration.

## Usage

```
/confluent-connect <action> [options]
```

### Actions
- `list` - List installed connector plugins
- `status` - Check connector status
- `create <type>` - Create a new connector
- `delete <name>` - Delete a connector
- `pause <name>` - Pause a connector
- `resume <name>` - Resume a connector

### Connector Types
- `jdbc-source` - JDBC Source Connector (database to Kafka)
- `jdbc-sink` - JDBC Sink Connector (Kafka to database)
- `debezium-postgres` - Debezium PostgreSQL CDC
- `debezium-mysql` - Debezium MySQL CDC
- `elasticsearch-sink` - Elasticsearch Sink
- `s3-sink` - AWS S3 Sink
- `file-source` - File Source (testing)
- `file-sink` - File Sink (testing)

---

## Connector Configurations

### JDBC Source Connector (PostgreSQL)

```json
{
  "name": "jdbc-source-postgres",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/mydb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topic.prefix": "postgres-",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "table.whitelist": "customers,orders,products",
    "poll.interval.ms": "1000",
    "batch.max.rows": "1000",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "createKey,extractInt",
    "transforms.createKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.createKey.fields": "id",
    "transforms.extractInt.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractInt.field": "id"
  }
}
```

### JDBC Source with Timestamp+Incrementing Mode

```json
{
  "name": "jdbc-source-timestamp",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/mydb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topic.prefix": "db-",
    "mode": "timestamp+incrementing",
    "incrementing.column.name": "id",
    "timestamp.column.name": "updated_at",
    "table.whitelist": "membros,guias",
    "poll.interval.ms": "5000",
    "validate.non.null": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

### JDBC Sink Connector

```json
{
  "name": "jdbc-sink-postgres",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/analytics",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topics": "processed-customers,processed-orders",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

### Debezium PostgreSQL CDC Connector

```json
{
  "name": "debezium-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "mydb",
    "database.server.name": "dbserver1",
    "topic.prefix": "cdc",
    "table.include.list": "public.customers,public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}
```

### Elasticsearch Sink Connector

```json
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "connection.url": "http://elasticsearch:9200",
    "topics": "customers,products",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "true",
    "behavior.on.null.values": "delete",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "extractKey",
    "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractKey.field": "id"
  }
}
```

### S3 Sink Connector

```json
{
  "name": "s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "s3.bucket.name": "my-kafka-data",
    "s3.region": "us-east-1",
    "topics": "guias,membros",
    "flush.size": "1000",
    "rotate.interval.ms": "60000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "partition.duration.ms": "3600000",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "locale": "en-US",
    "timezone": "UTC",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

### File Stream Source (Testing)

```json
{
  "name": "file-source-test",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "file": "/data/input.txt",
    "topic": "file-input-topic",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

---

## API Commands

### List Connectors

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq

# List connector plugins
curl -s http://localhost:8083/connector-plugins | jq
```

### Create Connector

```bash
# Create connector from JSON file
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json

# Create connector inline
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-connector",
    "config": {
      "connector.class": "...",
      ...
    }
  }'
```

### Manage Connectors

```bash
# Get connector status
curl -s http://localhost:8083/connectors/my-connector/status | jq

# Get connector config
curl -s http://localhost:8083/connectors/my-connector/config | jq

# Update connector config
curl -X PUT http://localhost:8083/connectors/my-connector/config \
  -H "Content-Type: application/json" \
  -d @updated-config.json

# Pause connector
curl -X PUT http://localhost:8083/connectors/my-connector/pause

# Resume connector
curl -X PUT http://localhost:8083/connectors/my-connector/resume

# Restart connector
curl -X POST http://localhost:8083/connectors/my-connector/restart

# Delete connector
curl -X DELETE http://localhost:8083/connectors/my-connector
```

### Task Management

```bash
# List tasks
curl -s http://localhost:8083/connectors/my-connector/tasks | jq

# Restart specific task
curl -X POST http://localhost:8083/connectors/my-connector/tasks/0/restart
```

---

## Common Transforms (SMT)

### Extract Field

```json
"transforms": "extractField",
"transforms.extractField.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
"transforms.extractField.field": "data"
```

### Rename Topic

```json
"transforms": "renameTopic",
"transforms.renameTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.renameTopic.regex": "(.*)source(.*)",
"transforms.renameTopic.replacement": "$1dest$2"
```

### Add Timestamp

```json
"transforms": "insertTimestamp",
"transforms.insertTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.insertTimestamp.timestamp.field": "processed_at"
```

### Flatten Nested JSON

```json
"transforms": "flatten",
"transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
"transforms.flatten.delimiter": "_"
```

### Filter Records

```json
"transforms": "filterNull",
"transforms.filterNull.type": "org.apache.kafka.connect.transforms.Filter",
"transforms.filterNull.predicate": "isNullValue",
"predicates": "isNullValue",
"predicates.isNullValue.type": "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"
```

---

## Troubleshooting

### Check Logs

```bash
docker logs connect -f --tail 100
```

### Common Issues

1. **Connector FAILED state**: Check task status and logs
2. **Schema incompatibility**: Verify schema registry URL
3. **Connection refused**: Check network connectivity
4. **No data flowing**: Verify source table/topic has data
