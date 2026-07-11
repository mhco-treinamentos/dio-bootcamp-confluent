# Topic Designer

## Skill: `/topic-designer`

Design Kafka topics with proper naming conventions, partitioning strategies, retention policies, and configurations following the workshop's Confluent Cloud standards.

## Usage

```
/topic-designer <domain> <entity> [options]
```

### Options
- `--event-type` - Event type: cdc, enriched, aggregated, alert, command (default: cdc)
- `--env` - Environment: dev, staging, prod (default: prod)
- `--source-system` - Source system: oracle-beneficiarios, oracle-autorizacoes, oracle-prestadores, oracle-faturamento, flink
- `--throughput` - Expected messages/sec: low (<100), medium (100-1000), high (>1000)
- `--retention` - Retention: 7d, 30d, 90d, 365d, infinite (default: 30d)

---

## Naming Convention

```
{env}.{domain}.{source_system}.{entity}.{event_type}.v{version}
```

### Examples

| Use Case | Topic Name |
|----------|------------|
| Beneficiarios CDC | `prod.saude.oracle-beneficiarios.membros.cdc.v1` |
| Autorizacoes CDC | `prod.saude.oracle-autorizacoes.guias.cdc.v1` |
| Prestadores CDC | `prod.saude.oracle-prestadores.prestadores.cdc.v1` |
| Faturamento CDC | `prod.saude.oracle-faturamento.faturas.cdc.v1` |
| Flink Enrichment | `prod.saude.flink.guias.enriched.v1` |
| Flink Aggregation | `prod.saude.flink.hourly_guia_volumes.aggregated.v1` |
| Glosa Alert | `prod.saude.flink.glosa_alerts.detected.v1` |
| DLQ | `prod.saude.oracle-beneficiarios.dlq.v1` |
| Schema History | `_schemas.oracle-beneficiarios.history` |

---

## Partitioning Strategy

### Partition Count Guidelines

| Throughput | Partition Count | Notes |
|-----------|----------------|-------|
| Low (<100 msg/s) | 6 | Minimum for HA |
| Medium (100-1K msg/s) | 12 | Good balance |
| High (1K-10K msg/s) | 24 | Higher parallelism |
| Very High (>10K msg/s) | 48+ | Scale with consumers |

### Partition Key Selection

| Domain | Recommended Key | Reasoning |
|--------|----------------|-----------|
| Guias/Autorizacoes | `membro_id` | Co-locate membro guias |
| Membros | `membro_id` | Natural entity key |
| Planos | `membro_id` | Co-locate with membro |
| Prestadores | `prestador_id` | Natural entity key |
| Faturamento | `prestador_id` | Co-locate by prestador |
| Glosa Alerts | `prestador_id` | Co-locate with faturamento |

---

## Retention Policies

| Topic Type | Retention | Compaction | Reasoning |
|------------|-----------|------------|-----------|
| CDC events | 30 days | No | Replayable from source |
| Enriched streams | 30 days | No | Derived, can be recomputed |
| Aggregated metrics | 90 days | No | Historical analytics |
| Glosa alerts | 365 days | No | ANS compliance requirement |
| Membro state | Infinite | Yes (compact) | Latest state needed |
| Prestador state | Infinite | Yes (compact) | Latest state needed |
| DLQ | 90 days | No | Troubleshooting window |
| Schema history | Infinite | No | Required by connectors |

---

## Topic Configuration Templates

### CDC Topic (Oracle Source)

```bash
confluent kafka topic create prod.saude.oracle-autorizacoes.guias.cdc.v1 \
  --partitions 24 \
  --config "cleanup.policy=delete" \
  --config "retention.ms=2592000000" \
  --config "min.insync.replicas=2" \
  --config "compression.type=lz4" \
  --config "max.message.bytes=1048576" \
  --config "segment.ms=3600000" \
  --cluster <cluster-id>
```

### Compacted Topic (Entity State)

```bash
confluent kafka topic create prod.saude.oracle-beneficiarios.membros.state.v1 \
  --partitions 12 \
  --config "cleanup.policy=compact" \
  --config "min.compaction.lag.ms=3600000" \
  --config "delete.retention.ms=86400000" \
  --config "min.insync.replicas=2" \
  --config "compression.type=lz4" \
  --cluster <cluster-id>
```

### Enriched Stream Topic (Flink Output)

```bash
confluent kafka topic create prod.saude.flink.guias.enriched.v1 \
  --partitions 24 \
  --config "cleanup.policy=delete" \
  --config "retention.ms=2592000000" \
  --config "min.insync.replicas=2" \
  --config "compression.type=lz4" \
  --cluster <cluster-id>
```

### DLQ Topic

```bash
confluent kafka topic create prod.saude.oracle-autorizacoes.dlq.v1 \
  --partitions 6 \
  --config "cleanup.policy=delete" \
  --config "retention.ms=7776000000" \
  --config "min.insync.replicas=2" \
  --cluster <cluster-id>
```

---

## Terraform Topic Resources

### Single Topic

```hcl
resource "confluent_kafka_topic" "guias_cdc" {
  kafka_cluster {
    id = confluent_kafka_cluster.prod.id
  }
  topic_name       = "prod.saude.oracle-autorizacoes.guias.cdc.v1"
  partitions_count = 24
  rest_endpoint    = confluent_kafka_cluster.prod.rest_endpoint

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000"
    "min.insync.replicas" = "2"
    "compression.type"    = "lz4"
    "max.message.bytes"   = "1048576"
  }

  credentials {
    key    = confluent_api_key.admin.id
    secret = confluent_api_key.admin.secret
  }
}
```

### Multiple Topics with for_each

```hcl
locals {
  beneficiarios_topics = {
    "membros"      = { partitions = 12, retention_ms = "-1", cleanup = "compact" }
    "dependentes"  = { partitions = 12, retention_ms = "-1", cleanup = "compact" }
    "planos"       = { partitions = 12, retention_ms = "-1", cleanup = "compact" }
    "coberturas"   = { partitions = 6, retention_ms = "-1", cleanup = "compact" }
    "carencias"    = { partitions = 6, retention_ms = "2592000000", cleanup = "delete" }
  }
}

resource "confluent_kafka_topic" "oracle_beneficiarios_cdc" {
  for_each = local.beneficiarios_topics

  kafka_cluster {
    id = confluent_kafka_cluster.prod.id
  }
  topic_name       = "prod.saude.oracle-beneficiarios.${each.key}.cdc.v1"
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.prod.rest_endpoint

  config = {
    "cleanup.policy"      = each.value.cleanup
    "retention.ms"        = each.value.retention_ms
    "min.insync.replicas" = "2"
    "compression.type"    = "lz4"
  }

  credentials {
    key    = confluent_api_key.admin.id
    secret = confluent_api_key.admin.secret
  }
}
```

---

## Design Checklist

```markdown
For every new topic, verify:

Naming:
- [ ] Follows {env}.{domain}.{source_system}.{entity}.{event_type}.v{version}
- [ ] Uses lowercase with dots as separators
- [ ] Entity name matches source table (snake_case)
- [ ] Version included (start with v1)

Partitioning:
- [ ] Partition count matches expected throughput
- [ ] Partition key chosen for data locality
- [ ] No hot partitions (uniform key distribution)

Configuration:
- [ ] Retention matches data lifecycle policy
- [ ] Compaction enabled only for entity state topics
- [ ] Compression set (lz4 for most, snappy for dev)
- [ ] min.insync.replicas = 2 for production

Schema:
- [ ] Avro schema registered in Schema Registry
- [ ] BACKWARD compatibility set
- [ ] PII fields tagged with confluent:tags ["PII", "LGPD"]
- [ ] PHI fields tagged with confluent:tags ["PHI", "SAUDE", "ANS"]
- [ ] Key schema defined (for compacted topics)

Security:
- [ ] ACLs configured for producer service account
- [ ] ACLs configured for consumer service accounts
- [ ] DLQ topic created alongside main topic
```

---

## Best Practices

1. **Always start with v1** in topic names; create v2 only on breaking schema changes
2. **Use 6 partitions minimum** even for low-throughput topics to allow future scaling
3. **Set min.insync.replicas=2** on all production topics (Confluent Cloud Dedicated)
4. **Use lz4 compression** for best throughput/compression tradeoff
5. **Create DLQ topics** alongside every CDC and connector topic
6. **Use compaction** only for entity-state topics where latest value per key matters
7. **Match partition keys** between related topics to enable co-partitioned joins in Flink
8. **Never increase partitions** on compacted topics — plan partition count upfront
9. **Use Terraform** to manage all topic configurations as code
10. **Document topic ownership** in data contracts

---

**Version**: 1.0
**Last Updated**: 2026-02-12
