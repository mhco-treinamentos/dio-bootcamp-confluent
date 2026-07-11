# Topic Designer

## Skill: `/topic-designer`

Design de topics Kafka com naming, particionamento, retenção e configuração seguindo os padrões do bootcamp no **Confluent Cloud** ([rules.md](../../rules.md)).

## Usage

```
/topic-designer <domain> <entity> [options]
```

### Options
- `--event-type` — Tipo: cdc, enriched, aggregated, detected, created, snapshot (default: created)
- `--stage` — `dev` | `lab` (**não há prod no bootcamp**; default: dev)
- `--source-system` — `pg-payments` (CDC), `flink` (derivados), `gen` (sintéticos)
- `--throughput` — Mensagens/s esperadas: low (<100), medium (100-1000), high (>1000)
- `--retention` — 1d, 7d, 30d (default: 7d — labs curtos gastam menos créditos)

---

## Naming Convention (rules.md)

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}
```

### Exemplos

| Caso de uso | Topic |
|-------------|-------|
| CDC accounts | `dev.payments.pg-payments.accounts.cdc.v1` |
| CDC cards | `dev.payments.pg-payments.cards.cdc.v1` |
| CDC transactions | `dev.payments.pg-payments.transactions.cdc.v1` |
| Flink enrichment | `dev.payments.flink.transactions.enriched.v1` |
| Flink fraud | `dev.payments.flink.fraud.detected.v1` |
| Flink aggregation | `dev.payments.flink.balance.aggregated.v1` |
| Gerador sintético | `dev.payments.gen.transactions.created.v1` |
| Snapshot de lab | `lab.payments.gen.accounts.snapshot.v1` |
| DLQ | `dev.payments.pg-payments.dlq.v1` |

---

## Particionamento

### Contagem de partições (labs)

| Throughput | Partições | Notas |
|-----------|-----------|-------|
| Low (<100 msg/s) — maioria dos labs | 3 | Setup rápido, menos custo |
| Medium (100–1K msg/s) | 6 | Default do desafio final |
| High (>1K msg/s) — stress test | 12 | Só quando o lab exige |

> Dimensionamento de produção (CKUs, partições por cluster) é conteúdo do módulo 7 — nos labs, menos é mais.

### Chave de partição

| Entidade | Chave | Razão |
|----------|-------|-------|
| transactions | `account_id` | Ordem por conta (idempotência/velocity por conta) |
| cards | `account_id` | Co-particionar com transactions p/ join no Flink |
| accounts | `account_id` | Chave natural |
| fraud.detected | `card_id` | Alertas agrupados por cartão |
| balance.aggregated | `account_id` | Estado por conta |

---

## Retenção

| Tipo de topic | Retenção | Compaction | Razão |
|---------------|----------|------------|-------|
| Eventos gerados (labs) | 1–7 dias | Não | Créditos; dado é regenerável (seed) |
| CDC | 7 dias | Não (ou compact+delete) | Replayable da fonte |
| Derivados Flink | 7 dias | Não | Recomputáveis |
| Estado de entidade (snapshot) | — | compact | Último valor por chave |
| DLQ | 7–30 dias | Não | Janela de troubleshooting |

> Retenção também é **compliance** (Live 3): reter só o necessário é princípio LGPD.

---

## Templates de criação (CLI)

### Topic CDC

```bash
confluent kafka topic create dev.payments.pg-payments.transactions.cdc.v1 \
  --partitions 6 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=lz4
```

### Topic compactado (estado)

```bash
confluent kafka topic create lab.payments.gen.accounts.snapshot.v1 \
  --partitions 3 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=3600000 \
  --config delete.retention.ms=86400000
```

### Saída do Flink

```bash
confluent kafka topic create dev.payments.flink.transactions.enriched.v1 \
  --partitions 6 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000
```

### DLQ

```bash
confluent kafka topic create dev.payments.pg-payments.dlq.v1 \
  --partitions 3 \
  --config cleanup.policy=delete \
  --config retention.ms=2592000000
```

> **RF=3** e **min.insync.replicas=2** são defaults do Confluent Cloud — citar como conceitos-âncora, sem precisar configurar.

---

## Terraform (IaC opcional dos labs)

```hcl
locals {
  cdc_topics = {
    "accounts"     = { partitions = 6, retention_ms = "604800000" }
    "cards"        = { partitions = 6, retention_ms = "604800000" }
    "transactions" = { partitions = 6, retention_ms = "604800000" }
  }
}

resource "confluent_kafka_topic" "pg_payments_cdc" {
  for_each = local.cdc_topics

  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  topic_name       = "dev.payments.pg-payments.${each.key}.cdc.v1"
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint

  config = {
    "cleanup.policy"   = "delete"
    "retention.ms"     = each.value.retention_ms
    "compression.type" = "lz4"
  }

  credentials {
    key    = confluent_api_key.topic_manager.id
    secret = confluent_api_key.topic_manager.secret
  }
}
```

---

## Design Checklist

```markdown
Para todo topic novo:

Naming:
- [ ] Segue {stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}
- [ ] stage é dev ou lab (nunca prod)
- [ ] Lowercase, pontos como separador, entity em snake_case
- [ ] Versão incluída (começa em v1; v2 só em breaking change)

Particionamento:
- [ ] 3 (labs) ou 6 (desafio final) partições
- [ ] Chave escolhida por localidade (account_id/card_id)
- [ ] Topics que fazem join no Flink co-particionados (mesma chave + nº de partições)

Configuração:
- [ ] Retenção mínima suficiente (créditos + LGPD)
- [ ] Compaction só para estado de entidade
- [ ] lz4 como compressão default

Schema:
- [ ] Avro registrado no SR (subject <topic>-value), namespace com.bootcamp.payments
- [ ] Compatibilidade BACKWARD
- [ ] Campos sensíveis com confluent:tags ["PII","LGPD"] / ["PCI"]

Segurança:
- [ ] ACLs por service account do lab (least privilege, por prefixo)
- [ ] DLQ criada junto quando há conector
- [ ] Topic entra no teardown.sh do lab
```

---

## Best practices

1. **Sempre v1** no nome; v2 apenas em breaking change de schema
2. **Co-particionar** topics que fazem join no Flink (mesma chave e nº de partições)
3. **Nunca aumentar partições** de topic compactado ou com dependência de ordem
4. **Criar DLQ junto** de todo topic alimentado por conector
5. **Documentar dono e propósito** no data contract ([data-contract-designer.md](data-contract-designer.md))
6. **Deletar topics de lab no teardown** — storage também consome créditos

---

**Última atualização**: 2026-07-11
