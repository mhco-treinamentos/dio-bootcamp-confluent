# Confluent Platform Troubleshooting Skill

## Skill: `/confluent-troubleshoot`

Debug and resolve common issues with Confluent Platform.

## Usage

```
/confluent-troubleshoot <issue> [options]
```

### Common Issues
- `connectivity` - Network and connection issues
- `performance` - Slow consumers/producers
- `connector` - Kafka Connect problems
- `schema` - Schema Registry issues
- `rebalance` - Consumer group rebalancing
- `disk` - Storage and retention issues

---

## Diagnostic Commands

### Check Service Health

```bash
# Check all containers
docker-compose ps

# Check container logs
docker logs broker --tail 100
docker logs connect --tail 100
docker logs schema-registry --tail 100

# Follow logs in real-time
docker logs -f connect

# Check resource usage
docker stats
```

### Verify Connectivity

```bash
# Test Zookeeper
docker exec zookeeper echo ruok | nc localhost 2181

# Test Kafka broker
docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092

# Test Schema Registry
curl -s http://localhost:8081/subjects | jq

# Test Kafka Connect
curl -s http://localhost:8083/connectors | jq

# Test from inside container
docker exec broker nc -zv schema-registry 8081
docker exec connect nc -zv broker 29092
```

---

## Common Problems & Solutions

### 1. Broker Not Starting

**Symptoms:**
- Broker container keeps restarting
- "Unable to connect to Zookeeper" errors

**Diagnosis:**
```bash
docker logs broker 2>&1 | grep -i error
docker exec broker cat /var/log/kafka/server.log | tail -50
```

**Solutions:**

```bash
# Check Zookeeper is healthy
docker exec zookeeper zkServer.sh status

# Verify network connectivity
docker network inspect confluent-network

# Check disk space
docker exec broker df -h /var/lib/kafka/data

# Clear corrupt data (development only!)
docker-compose down
docker volume rm $(docker volume ls -q | grep broker)
docker-compose up -d
```

### 2. Connect Container Failing

**Symptoms:**
- Connect keeps restarting
- "Worker failed to start" errors

**Diagnosis:**
```bash
docker logs connect 2>&1 | grep -i "error\|exception" | tail -20
docker exec connect curl -s localhost:8083/
```

**Solutions:**

```bash
# Wait for Kafka to be fully ready
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Check Connect internal topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe \
  --topic _connect-configs

# Recreate Connect internal topics (if corrupted)
docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic _connect-configs
docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic _connect-offsets
docker exec broker kafka-topics --bootstrap-server localhost:9092 --delete --topic _connect-status
docker-compose restart connect
```

### 3. Connector in FAILED State

**Symptoms:**
- Connector shows FAILED status
- Tasks are failing

**Diagnosis:**
```bash
# Check connector status
curl -s http://localhost:8083/connectors/my-connector/status | jq

# Check task status
curl -s http://localhost:8083/connectors/my-connector/tasks/0/status | jq

# Get detailed error trace
curl -s http://localhost:8083/connectors/my-connector/status | jq '.tasks[].trace'
```

**Solutions:**

```bash
# Restart the connector
curl -X POST http://localhost:8083/connectors/my-connector/restart

# Restart specific task
curl -X POST http://localhost:8083/connectors/my-connector/tasks/0/restart

# Delete and recreate
curl -X DELETE http://localhost:8083/connectors/my-connector
# Fix config and recreate...

# Check connector plugins installed
curl -s http://localhost:8083/connector-plugins | jq '.[].class'
```

### 4. Schema Compatibility Errors

**Symptoms:**
- 409 Conflict when registering schema
- "Schema being registered is incompatible"

**Diagnosis:**
```bash
# Check current compatibility level
curl -s http://localhost:8081/config | jq

# Check subject compatibility
curl -s http://localhost:8081/config/my-topic-value | jq

# Test compatibility
curl -X POST http://localhost:8081/compatibility/subjects/my-topic-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'
```

**Solutions:**

```bash
# Temporarily disable compatibility (development only!)
curl -X PUT http://localhost:8081/config/my-topic-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "NONE"}'

# Register schema
curl -X POST http://localhost:8081/subjects/my-topic-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'

# Re-enable compatibility
curl -X PUT http://localhost:8081/config/my-topic-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

### 5. Consumer Lag / Slow Processing

**Symptoms:**
- Consumer lag increasing
- Messages piling up

**Diagnosis:**
```bash
# Check consumer group lag
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group

# Check partition distribution
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic

# Monitor lag over time
watch -n 5 'docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-group 2>/dev/null | grep my-topic'
```

**Solutions:**

```bash
# Scale consumers (add more instances in consumer group)

# Increase partitions (cannot decrease!)
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic my-topic --partitions 12

# Optimize consumer configuration
# - Increase fetch.min.bytes
# - Increase fetch.max.wait.ms
# - Increase max.poll.records
```

### 6. Consumer Group Rebalancing Issues

**Symptoms:**
- Frequent rebalances
- "Member failed to heartbeat" errors

**Diagnosis:**
```bash
# Check group state
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group --state

# Watch for rebalances
docker logs broker 2>&1 | grep -i rebalance
```

**Solutions:**

```bash
# Increase session timeout
# Consumer config: session.timeout.ms=45000

# Increase heartbeat interval
# Consumer config: heartbeat.interval.ms=15000

# Use static group membership
# Consumer config: group.instance.id=consumer-1

# Increase max.poll.interval.ms if processing is slow
# Consumer config: max.poll.interval.ms=600000
```

### 7. Disk Space Issues

**Symptoms:**
- "Log directory not found" errors
- Topics being deleted unexpectedly

**Diagnosis:**
```bash
# Check disk usage
docker exec broker df -h /var/lib/kafka/data
docker exec broker du -sh /var/lib/kafka/data/*

# Check topic sizes
docker exec broker kafka-log-dirs --bootstrap-server localhost:9092 \
  --describe --topic-list my-topic
```

**Solutions:**

```bash
# Reduce retention
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=86400000  # 1 day

# Increase disk (Docker volume)
# Modify docker-compose.yml to use larger volume

# Delete old topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic old-topic

# Force log cleanup
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=1000

# Wait for cleanup, then restore
docker exec broker kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=604800000
```

---

## Performance Tuning

### Producer Optimization

```properties
# Batch messages
batch.size=32768
linger.ms=20

# Compression
compression.type=snappy

# Increase buffer
buffer.memory=67108864

# Acknowledgments (tradeoff: latency vs durability)
acks=1  # or acks=all for durability
```

### Consumer Optimization

```properties
# Larger fetches
fetch.min.bytes=50000
fetch.max.wait.ms=500
max.poll.records=500

# Parallel processing
# Use multiple partitions and consumers
```

### Broker Optimization

```properties
# Increase file descriptors
ulimit -n 100000

# Tune JVM
KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"

# Network threads
num.network.threads=8
num.io.threads=16
```

---

## Useful Monitoring Queries

### JMX Metrics (if enabled)

```bash
# Bytes in/out per second
kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec

# Request latency
kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce
kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer

# Under-replicated partitions
kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
```

### ksqlDB Monitoring

```sql
-- Show running queries
SHOW QUERIES;

-- Describe query
EXPLAIN <query_id>;

-- Check consumer lag
SELECT * FROM KSQL_PROCESSING_LOG EMIT CHANGES;
```

---

## Emergency Procedures

### Complete Reset (Development Only!)

```bash
# Stop everything
docker-compose down

# Remove all volumes
docker volume rm $(docker volume ls -q | grep -E "zookeeper|broker|connect")

# Start fresh
docker-compose up -d
```

### Recover from Broker Failure

```bash
# 1. Check which broker is down
docker-compose ps

# 2. Check for under-replicated partitions
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions

# 3. Restart the failed broker
docker-compose restart broker

# 4. Monitor replication
watch -n 5 'docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe --under-replicated-partitions'
```

### Export/Backup Topics

```bash
# Export to file
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --property print.key=true \
  --timeout-ms 10000 > backup.txt

# Import from file
cat backup.txt | docker exec -i broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-topic-restored \
  --property parse.key=true
```
