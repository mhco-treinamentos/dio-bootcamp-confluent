# Performance Analyzer

## Skill: `/performance-analyzer`

Analyze and optimize performance for Kafka producers, consumers, Flink SQL jobs, and CDC connectors on the workshop's Confluent Cloud platform.

## Usage

```
/performance-analyzer <component> [options]
```

### Components
- `producer` - Kafka producer throughput and latency
- `consumer` - Consumer lag and processing time
- `connector` - CDC connector performance and lag
- `flink` - Flink SQL CFU usage and state size
- `cluster` - Cluster-level metrics and capacity
- `end-to-end` - Full pipeline latency analysis

---

## Producer Performance

### Key Metrics

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Throughput (msgs/s) | Per SLA | <50% of expected |
| Latency p99 (ms) | <500 | >2000 |
| Batch size (avg) | >80% of configured | <50% of configured |
| Compression ratio | >2:1 (lz4) | <1.5:1 |
| Error rate | 0% | >0.1% |

### Optimization Checklist

```markdown
Batching:
- [ ] linger.ms = 10 (Cloud) or 5 (local) — allows batch filling
- [ ] batch.size = 32768 (Cloud) or 16384 (local)
- [ ] compression.type = lz4 (best throughput/ratio)

Reliability:
- [ ] acks = all (required for idempotence)
- [ ] enable.idempotence = true
- [ ] max.in.flight.requests.per.connection = 5

Connection:
- [ ] Connection pooling if multiple producers
- [ ] DNS caching configured for Cloud endpoints
- [ ] Retry backoff configured (retry.backoff.ms = 100)
```

### Confluent Cloud Producer Config (Optimized)

```python
optimized_producer = {
    "bootstrap.servers": "<BOOTSTRAP>",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "<API_KEY>",
    "sasl.password": "<API_SECRET>",
    "client.id": "workshop-{service}-producer",
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "linger.ms": 10,
    "batch.size": 32768,
    "compression.type": "lz4",
    "retries": 2147483647,
    "delivery.timeout.ms": 120000,
    "request.timeout.ms": 30000,
}
```

---

## Consumer Performance

### Key Metrics

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Consumer lag (records) | <1000 | >10000 |
| Processing rate (msgs/s) | >= production rate | <50% of production |
| Commit interval (ms) | 5000 | N/A |
| Rebalance frequency | Rare | >1/hour |

### Lag Monitoring

```bash
# Check consumer group lag
confluent kafka consumer-group lag describe <group-id> \
  --cluster <cluster-id>

# List all consumer groups
confluent kafka consumer-group list --cluster <cluster-id>
```

---

## CDC Connector Performance

### Key Metrics

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| CDC lag (seconds) | <60 | >300 |
| Events/second | Matches DML rate | <50% of expected |
| Snapshot progress | 100% | Stuck for >1h |
| LogMiner session time | <30s per batch | >60s |
| Queue saturation | <80% | >90% |
| DLQ message count | 0 | >0 |

### Optimization Parameters

```json
{
  "log.mining.batch.size.min": 1000,
  "log.mining.batch.size.max": 10000,
  "log.mining.sleep.time.min.ms": 0,
  "log.mining.sleep.time.max.ms": 3000,
  "log.mining.sleep.time.default.ms": 1000,
  "log.mining.sleep.time.increment.ms": 200,
  "max.queue.size": 8192,
  "max.batch.size": 2048,
  "poll.interval.ms": 1000
}
```

### Oracle-Side Performance Queries

```sql
-- REDO log generation rate
SELECT
    TRUNC(first_time, 'HH24') AS hour,
    COUNT(*) AS log_switches,
    ROUND(SUM(blocks * block_size) / 1024 / 1024, 2) AS redo_mb
FROM V$ARCHIVED_LOG
WHERE first_time > SYSDATE - 1
GROUP BY TRUNC(first_time, 'HH24')
ORDER BY hour;

-- Active LogMiner sessions
SELECT sid, serial#, username, status, program
FROM V$SESSION
WHERE program LIKE '%LogMiner%' OR module LIKE '%Debezium%';

-- Long-running open transactions (memory pressure risk)
SELECT
    t.xidusn || '.' || t.xidslot || '.' || t.xidsqn AS xid,
    t.start_time,
    ROUND((SYSDATE - TO_DATE(t.start_time, 'MM/DD/YY HH24:MI:SS')) * 24 * 60, 2) AS mins
FROM V$TRANSACTION t
WHERE (SYSDATE - TO_DATE(t.start_time, 'MM/DD/YY HH24:MI:SS')) * 24 * 60 > 30;
```

---

## Flink SQL Performance

### CFU Usage Guide

| Job Type | CFUs | Key Factor |
|----------|------|------------|
| Simple projection | 2-4 | Message rate |
| 2-table join | 4-8 | State size |
| 3+ table join | 8-16 | Table size |
| Tumbling window | 4-8 | Key cardinality |
| CEP pattern | 6-12 | Pattern complexity |
| Deduplication | 4-8 | Unique key count |

### Optimization Tips

```sql
-- 1. Limit scan range
/*+ OPTIONS('scan.startup.mode' = 'latest-offset') */

-- 2. Filter early
WHERE CREATED_AT >= CURRENT_TIMESTAMP - INTERVAL '30' DAY

-- 3. Smaller windows = less state
TUMBLE(event_time, INTERVAL '5' MINUTE)  -- preferred over 1 HOUR

-- 4. Monitor via Confluent Cloud Flink dashboard
```

---

## Cluster-Level Performance

### Capacity Planning

| Cluster Type | Max Throughput | Max Partitions | Use Case |
|-------------|---------------|----------------|----------|
| Basic | 100 MB/s | 2,048 | Dev/test |
| Standard | 100 MB/s | 2,048 | Staging |
| Dedicated (CKU 1) | 50 MB/s | 4,500 | Small prod |
| Dedicated (CKU 2) | 100 MB/s | 9,000 | Medium prod |
| Dedicated (CKU 4+) | 200+ MB/s | 18,000+ | Large prod |

### Monitoring Commands

```bash
# Cluster metrics
confluent kafka cluster describe <cluster-id> -o json

# Topic throughput
confluent kafka topic describe <topic> --cluster <cluster-id>
```

---

## End-to-End Latency Analysis

```
Oracle COMMIT -> CDC Capture -> Kafka Produce -> Flink Process -> Consumer

Breakdown:
|- Oracle -> CDC:     10-500ms  (LogMiner mining interval)
|- CDC -> Kafka:      5-50ms    (producer batching + network)
|- Kafka -> Flink:    1-10ms    (consumer fetch)
|- Flink Processing:  1-100ms   (depends on job complexity)
|- Flink -> Sink:     5-50ms    (producer batching)

Total Target: <1 second (p95)
Total Budget: <5 seconds (p99)
```

---

## Best Practices

1. **Monitor CDC lag** as the primary health indicator for the pipeline
2. **Set alerts** before problems become visible to consumers
3. **Right-size Flink CFUs** — start small, scale based on metrics
4. **Use lz4 compression** everywhere for best throughput
5. **Avoid over-partitioning** — more partitions = more overhead
6. **Profile Oracle REDO rate** before sizing the CDC connector
7. **Test with production-like volume** in staging before go-live

---

**Version**: 1.0
**Last Updated**: 2026-02-12
