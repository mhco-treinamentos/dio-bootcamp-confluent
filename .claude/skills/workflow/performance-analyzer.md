# Performance Analyzer

## Skill: `/performance-analyzer`

Analisa e otimiza performance de producers, consumers, conectores CDC e statements Flink no Confluent Cloud. Fonte de métricas: **Metrics API** (`api.telemetry.confluent.cloud`) — não há JMX local em serviço gerenciado. Base do módulo 7 (observabilidade, custos).

## Usage

```
/performance-analyzer <component> [options]
```

### Components
- `producer` — Throughput e latência de producers
- `consumer` — Lag e taxa de processamento
- `connector` — Performance do CDC gerenciado
- `flink` — CFUs e estado dos statements
- `cluster` — Métricas de cluster e capacidade
- `end-to-end` — Latência do pipeline completo

---

## Metrics API (fonte única de métricas)

Autenticação: API key de **cloud resource management** (não a key do cluster):

```bash
confluent api-key create --resource cloud --description "metrics api"
export CLOUD_API_KEY=... CLOUD_API_SECRET=...
```

### Métricas usadas nos labs

| Métrica | O que mede |
|---------|------------|
| `io.confluent.kafka.server/received_bytes` | Ingress por topic |
| `io.confluent.kafka.server/sent_bytes` | Egress por topic |
| `io.confluent.kafka.server/received_records` | Msgs/s produzidas |
| `io.confluent.kafka.server/consumer_lag_offsets` | **Consumer lag (métrica-chave)** |
| `io.confluent.kafka.server/retained_bytes` | Storage por topic (custos!) |
| `io.confluent.flink/num_records_in` / `num_records_out` | Throughput de statement |
| `io.confluent.flink/compute_pool_utilization` | Uso de CFU do pool |

### Query padrão (consumer lag por grupo)

```bash
curl -s https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -u "$CLOUD_API_KEY:$CLOUD_API_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{
    "aggregations": [{"metric": "io.confluent.kafka.server/consumer_lag_offsets"}],
    "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "<cluster-id>"},
    "group_by": ["metric.consumer_group_id"],
    "granularity": "PT1M",
    "intervals": ["PT1H/now"]
  }' | jq '.data[] | {group: .["metric.consumer_group_id"], lag: .value}'
```

> Prometheus/Grafana (módulo 7): a Metrics API expõe formato scrape em
> `https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=<cluster-id>`
> — plugável direto como scrape target com basic auth.

---

## Producer

### Métricas-alvo

| Métrica | Alvo | Alerta |
|---------|------|--------|
| Throughput (msgs/s) | Conforme lab | <50% do esperado |
| Latência p99 (ms) | <500 | >2000 |
| Compression ratio | >2:1 (lz4/snappy) | <1.5:1 |
| Taxa de erro (delivery callback) | 0% | >0.1% |

### Config otimizada (perfil cloud — rules.md)

```python
import os

optimized_producer = {
    "bootstrap.servers": os.environ["CC_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CC_API_KEY"],
    "sasl.password": os.environ["CC_API_SECRET"],
    "client.id": "bootcamp-gen-payments",
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "linger.ms": 10,
    "batch.size": 32768,
    "compression.type": "lz4",
    "delivery.timeout.ms": 120000,
}
```

Client-side: usar `statistics.interval.ms` + callback de stats do confluent-kafka para latência/batch médios (substitui o JMX do client).

---

## Consumer

| Métrica | Alvo | Alerta |
|---------|------|--------|
| Lag (records) | <1000 | >10000 |
| Taxa de processamento | ≥ taxa de produção | <50% |
| Frequência de rebalance | Raro | >1/h |

```bash
# Visão rápida via CLI
confluent kafka consumer group lag describe <group>
```

Lag crescendo → mais consumers (até nº de partições), `max.poll.records` maior, processamento assíncrono; rebalances → `session.timeout.ms=45000`, `group.instance.id` (static membership).

---

## Conector CDC (managed)

| Métrica | Alvo | Alerta |
|---------|------|--------|
| Lag CDC (commit → topic) | <60s | >300s |
| DLQ count | 0 | >0 |
| Status tasks | RUNNING | FAILED |

```bash
confluent connect cluster describe <lcc-id>
confluent kafka topic consume dev.payments.pg-payments.dlq.v1 --from-beginning --print-key
```

### Postgres fonte (lado do banco)

```sql
-- Lag de replicação do slot Debezium (bytes retidos de WAL)
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

-- Transações longas seguram o WAL e atrasam o CDC
SELECT pid, state, now() - xact_start AS duration, query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL AND now() - xact_start > interval '5 minutes';
```

---

## Flink (managed)

| Statement | CFUs | Fator |
|-----------|------|-------|
| Projeção/filtro | 1-2 | Taxa de msgs |
| Join 2 tabelas | 2-4 | Estado |
| Join 3+ / CEP | 4-8 | Estado/complexidade |
| Janela tumbling | 2-4 | Cardinalidade da chave |

```bash
confluent flink statement list
confluent flink statement describe <name>       # status + scaling
confluent flink statement exception list <name>
```

```sql
-- Otimizações nos labs
-- 1. Filtrar cedo (reduz estado)
-- 2. Janelas menores (5 min > 1 h para demo)
-- 3. Co-particionar topics de join (mesma chave/partições)
```

Utilização do pool: métrica `io.confluent.flink/compute_pool_utilization` na Metrics API; pool no teto de CFU → statements enfileiram.

---

## Cluster e custos

| Tipo | Limites | Uso no bootcamp |
|------|---------|-----------------|
| Basic | ~250 MB/s in, sem custo fixo | **Todos os labs** |
| Standard | + SLA/features | Não usar (créditos) |
| Dedicated/Freight | CKUs dedicados | Só teoria — módulo 7 |

```bash
# Storage retido por topic (driver de custo silencioso)
curl -s https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -u "$CLOUD_API_KEY:$CLOUD_API_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{"aggregations":[{"metric":"io.confluent.kafka.server/retained_bytes"}],
       "filter":{"field":"resource.kafka.id","op":"EQ","value":"<cluster-id>"},
       "group_by":["metric.topic"],"granularity":"PT1H","intervals":["PT6H/now"]}' | jq
```

---

## Latência end-to-end (desafio final)

```
Postgres COMMIT -> Debezium managed -> Kafka -> Flink SQL -> Tableflow/consumer

|- Postgres -> conector:  100-1000ms (poll da replicação lógica)
|- Conector -> Kafka:     10-100ms   (batch + rede)
|- Kafka -> Flink:        10-100ms   (fetch)
|- Flink processing:      10-500ms   (join/janela)
|- Flink -> topic saída:  10-100ms

Alvo p95: <2s produzir→detectar fraude (medir com kafka_ingested_at do SMT)
```

---

## Best practices

1. **Consumer lag é a métrica nº 1** — alertar antes do usuário perceber
2. **Metrics API para tudo** — sem JMX em serviço gerenciado; client-side usar stats do librdkafka
3. **Medir com o campo `kafka_ingested_at`** (SMT InsertField) para lag end-to-end
4. **Dimensionar CFU de baixo para cima** — começar no mínimo e escalar por métrica
5. **`retained_bytes` no radar** — retenção esquecida come créditos em silêncio
6. **Exportar para Prometheus/Grafana no módulo 7** via endpoint `/export`

---

**Última atualização**: 2026-07-11
