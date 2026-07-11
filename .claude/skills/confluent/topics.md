# Kafka Topics Management Skill

## Skill: `/confluent-topics`

Criar e gerenciar topics no **Confluent Cloud** via CLI `confluent` (v4). Fallback local (Docker) apenas onde indicado — ver [local-dev-fallback.md](local-dev-fallback.md).

## Usage

```
/confluent-topics <action> [options]
```

### Actions
- `list` — Listar topics
- `create <name>` — Criar topic
- `describe <name>` — Configuração do topic
- `update <name>` — Alterar configuração
- `delete <name>` — Deletar topic
- `produce` / `consume` — Mensagens de teste
- `consumer-groups` — Lag e offsets

---

## Contexto (uma vez por sessão)

```bash
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>
# A partir daqui os comandos não precisam de --cluster/--environment
```

## Comandos de topic (Confluent Cloud)

### Listar

```bash
confluent kafka topic list
confluent kafka topic list -o json | jq -r '.[].name' | grep "payments"
```

### Criar

```bash
# Topic de eventos gerados (naming de rules.md; RF=3 é fixo na cloud)
confluent kafka topic create dev.payments.gen.transactions.created.v1 \
  --partitions 6 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

# Topic compactado (estado de entidade / snapshot)
confluent kafka topic create lab.payments.gen.accounts.snapshot.v1 \
  --partitions 6 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=60000

# Compact+delete (CDC com tombstones e janela de retenção)
confluent kafka topic create dev.payments.pg-payments.transactions.cdc.v1 \
  --partitions 6 \
  --config cleanup.policy=compact,delete \
  --config retention.ms=604800000
```

> Conceitos-âncora ao criar/explicar topics: **RF=3**, **min.insync.replicas=2**, **acks=all** (defaults do Confluent Cloud — cite-os nas aulas mesmo quando não configuráveis no plano Basic).

### Describe / Update / Delete

```bash
confluent kafka topic describe dev.payments.gen.transactions.created.v1

# Alterar configuração (retention de 7d para 3d)
confluent kafka topic update dev.payments.gen.transactions.created.v1 \
  --config retention.ms=259200000

# Adicionar partições (nunca reduz; cuidado com ordem por chave!)
confluent kafka topic update dev.payments.gen.transactions.created.v1 --partitions 12

confluent kafka topic delete dev.payments.gen.transactions.created.v1 --force
```

---

## Produce / Consume (CLI)

```bash
# Producer simples (JSON como string)
confluent kafka topic produce dev.payments.gen.transactions.created.v1

# Com chave (ordem por account_id)
confluent kafka topic produce dev.payments.gen.transactions.created.v1 \
  --parse-key --delimiter ":"
# formato: acc-001:{"transaction_id":"tx-1","amount":"80.00"}

# Producer Avro integrado ao Schema Registry
confluent kafka topic produce dev.payments.gen.transactions.created.v1 \
  --value-format avro \
  --schema ./shared/schemas/payments/transaction_created.avsc

# Consumer
confluent kafka topic consume dev.payments.gen.transactions.created.v1 --from-beginning
confluent kafka topic consume dev.payments.gen.transactions.created.v1 \
  --from-beginning --print-key --value-format avro
confluent kafka topic consume dev.payments.gen.transactions.created.v1 --group bootcamp-lab02
```

## Consumer groups

```bash
confluent kafka consumer group list
confluent kafka consumer group describe bootcamp-lab02

# Lag por partição (base do lab de observabilidade — módulo 7)
confluent kafka consumer group lag describe bootcamp-lab02
```

> Reset de offsets não está na CLI cloud — usar consumer com `auto.offset.reset` ou a UI (Consumers → grupo → offsets). No fallback local: `kafka-consumer-groups --reset-offsets`.

---

## Fallback local (só topics + SR)

Mesmos conceitos, ferramentas do broker (ver [local-dev-fallback.md](local-dev-fallback.md)):

```bash
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create --topic dev.payments.gen.transactions.created.v1 --partitions 6 --replication-factor 1
docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group bootcamp-lab02
```

---

## Referência de configuração

### Retenção

| Config | Descrição | Default |
|--------|-----------|---------|
| `retention.ms` | Tempo de retenção | 604800000 (7 dias) |
| `retention.bytes` | Tamanho máximo por partição | -1 (ilimitado) |
| `cleanup.policy` | delete, compact ou ambos | delete |

### Performance / confiabilidade

| Config | Descrição | Cloud |
|--------|-----------|-------|
| `compression.type` | none, gzip, snappy, lz4, zstd | producer |
| `max.message.bytes` | Tamanho máximo da mensagem | 2 MB (Basic) |
| `min.insync.replicas` | Réplicas mínimas para acks=all | 2 (fixo) |
| replication factor | Cópias por partição | 3 (fixo) |

### Compactação

| Config | Descrição | Default |
|--------|-----------|---------|
| `min.compaction.lag.ms` | Tempo mínimo antes de compactar | 0 |
| `max.compaction.lag.ms` | Tempo máximo até compactar | infinito |
| `delete.retention.ms` | Retenção de tombstones | 86400000 (1 dia) |

---

## Best practices

### Naming (rules.md)

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}
stage = dev | lab (não há prod no bootcamp)

dev.payments.pg-payments.transactions.cdc.v1
dev.payments.flink.fraud.detected.v1
lab.payments.gen.accounts.snapshot.v1
```

### Estratégia de retenção

| Caso de uso | Cleanup policy | Retenção |
|-------------|----------------|----------|
| Eventos gerados / labs | delete | 1–7 dias (créditos!) |
| CDC / estado | compact | — |
| Saídas Flink (enriched/fraud) | delete | 7 dias |
| DLQ | delete | 7–30 dias |

### Particionamento

- **Chave = entidade de ordenação** (`account_id` para transações — ordem por conta)
- 3–6 partições nos labs (setup rápido); dimensionamento real é conteúdo do módulo 7
- Nunca aumentar partições de topic compactado ou com dependência de ordem por chave

---

**Última atualização**: 2026-07-11
