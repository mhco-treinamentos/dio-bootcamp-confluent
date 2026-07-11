# Kafka Topics Management Skill

## Skill: `/confluent-topics`

Create and manage Kafka topics with proper configuration.

## Usage

```
/confluent-topics <action> [options]
```

### Actions
- `list` - List all topics
- `create <name>` - Create a new topic
- `describe <name>` - Describe topic configuration
- `delete <name>` - Delete a topic
- `alter <name>` - Modify topic configuration
- `produce` - Send test messages
- `consume` - Read messages

---

## Topic Commands (Docker)

### List Topics

```bash
# List all topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# List with details
docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe

# Filter topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list | grep "membros"
```

### Create Topic

```bash
# Basic topic creation
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic membros \
  --partitions 3 \
  --replication-factor 1

# With configuration
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic guias \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy

# Compacted topic (for state/CDC)
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic membro-state \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --config segment.ms=100

# Log compacted with delete (for CDC tombstones)
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic cdc-events \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=compact,delete \
  --config retention.ms=604800000
```

### Describe Topic

```bash
# Describe topic
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --describe \
  --topic membros

# Describe all topics with under-replicated partitions
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --describe \
  --under-replicated-partitions

# Describe topics with specific config
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name membros
```

### Alter Topic

```bash
# Add partitions (cannot reduce)
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --alter \
  --topic membros \
  --partitions 6

# Change configuration
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name membros \
  --add-config retention.ms=259200000

# Remove configuration (revert to default)
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name membros \
  --delete-config retention.ms
```

### Delete Topic

```bash
# Delete single topic
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --delete \
  --topic membros

# Delete multiple topics (use with caution)
for topic in topic1 topic2 topic3; do
  docker exec broker kafka-topics --bootstrap-server localhost:9092 \
    --delete \
    --topic $topic
done
```

---

## Producer/Consumer Commands

### Console Producer

```bash
# Simple producer
docker exec -it broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic membros

# With key
docker exec -it broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --property "parse.key=true" \
  --property "key.separator=:"

# Example: key:value format
# membro-001:{"nome":"João Silva","cpf":"123.456.789-00"}
```

### Console Consumer

```bash
# Consume from beginning
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --from-beginning

# Consume with keys
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "

# Consume specific partition
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --partition 0 \
  --offset earliest

# Consume N messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --from-beginning \
  --max-messages 10

# Consumer group
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic membros \
  --group my-consumer-group
```

### Avro Producer/Consumer (with Schema Registry)

```bash
# Avro producer
docker exec -it schema-registry kafka-avro-console-producer \
  --bootstrap-server broker:29092 \
  --topic membros \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema='{"type":"record","name":"Membro","fields":[{"name":"id","type":"string"},{"name":"nome","type":"string"}]}'

# Avro consumer
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic membros \
  --from-beginning \
  --property schema.registry.url=http://localhost:8081
```

---

## Consumer Group Management

```bash
# List consumer groups
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe consumer group
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe \
  --group my-consumer-group

# Show all groups with lag
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe \
  --all-groups

# Reset offsets (careful!)
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --topic membros \
  --reset-offsets \
  --to-earliest \
  --execute

# Reset to specific offset
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --topic membros \
  --reset-offsets \
  --to-offset 100 \
  --execute

# Delete consumer group
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --delete \
  --group my-consumer-group
```

---

## Topic Configuration Reference

### Retention Settings

| Config | Description | Default |
|--------|-------------|---------|
| `retention.ms` | Time to retain messages | 604800000 (7 days) |
| `retention.bytes` | Max size per partition | -1 (unlimited) |
| `cleanup.policy` | delete, compact, or both | delete |
| `segment.bytes` | Segment file size | 1073741824 (1GB) |
| `segment.ms` | Time before segment rolls | 604800000 (7 days) |

### Performance Settings

| Config | Description | Default |
|--------|-------------|---------|
| `compression.type` | none, gzip, snappy, lz4, zstd | producer |
| `max.message.bytes` | Max message size | 1048588 |
| `min.insync.replicas` | Min replicas for acks=all | 1 |

### Compaction Settings

| Config | Description | Default |
|--------|-------------|---------|
| `min.cleanable.dirty.ratio` | Min dirty ratio to trigger | 0.5 |
| `min.compaction.lag.ms` | Min time before compaction | 0 |
| `max.compaction.lag.ms` | Max time before compaction | infinite |
| `delete.retention.ms` | Tombstone retention | 86400000 (1 day) |

---

## REST Proxy Commands

```bash
# List topics via REST
curl -s http://localhost:8082/topics | jq

# Get topic details
curl -s http://localhost:8082/topics/membros | jq

# Produce message via REST
curl -X POST http://localhost:8082/topics/membros \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{
    "records": [
      {"key": "membro-001", "value": {"nome": "João Silva", "cpf": "123.456.789-00"}},
      {"key": "membro-002", "value": {"nome": "Maria Santos", "cpf": "987.654.321-00"}}
    ]
  }'

# Create consumer instance
curl -X POST http://localhost:8082/consumers/my-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{
    "name": "my-consumer",
    "format": "json",
    "auto.offset.reset": "earliest"
  }'

# Subscribe to topic
curl -X POST http://localhost:8082/consumers/my-group/instances/my-consumer/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics": ["membros"]}'

# Consume messages
curl -s http://localhost:8082/consumers/my-group/instances/my-consumer/records \
  -H "Accept: application/vnd.kafka.json.v2+json" | jq
```

---

## Best Practices

### Partitioning Strategy

- **By key**: Use consistent hashing for related data
- **By time**: Time-based partitioning for time-series
- **Round-robin**: For load balancing (no key)

### Retention Strategy

| Use Case | Cleanup Policy | Retention |
|----------|---------------|-----------|
| Events/Logs | delete | 7-30 days |
| CDC/State | compact | Forever |
| Audit | delete | 1-7 years |
| Temp/Staging | delete | 1-24 hours |

### Naming Conventions

```
<env>.<domain>.<source_system>.<entity>.<event_type>.v<version>

Examples:
- prod.saude.oracle-beneficiarios.membros.cdc.v1
- prod.saude.flink.guias.enriched.v1
- prod.saude.oracle-autorizacoes.guias.cdc.v1
```
