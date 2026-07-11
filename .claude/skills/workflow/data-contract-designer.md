# Data Contract Designer

## Skill: `/data-contract-designer`

Cria data contracts (YAML/Avro) definindo schema, expectativas de qualidade, SLAs e ownership para topics do bootcamp no Confluent Cloud. Garante alinhamento producer–consumer e alimenta o Stream Governance (Curso 2 — Data Contracts; Live 3 — governança).

## Usage

```
/data-contract-designer <domain> <entity> [options]
```

### Options
- `--format` — Formato: yaml, avro, json-schema (default: yaml)
- `--type` — Tipo de evento: cdc, enriched, aggregated, detected, created

---

## Estrutura do contrato

### Template completo (YAML) — transactions CDC

```yaml
apiVersion: v1
kind: DataContract
metadata:
  name: payments-transactions-cdc
  version: "1.0.0"
  owner:
    team: bootcamp-payments
    contact: mateus@example.com
  domain: payments
  tags:
    - cdc
    - payments
    - lgpd
    - pci

topic:
  name: dev.payments.pg-payments.transactions.cdc.v1
  partitions: 6
  replication_factor: 3          # default Confluent Cloud
  retention_ms: 604800000        # 7 dias
  cleanup_policy: delete
  compression: lz4

schema:
  format: avro
  compatibility: BACKWARD        # default do bootcamp — nunca NONE
  registry_subject: dev.payments.pg-payments.transactions.cdc.v1-value
  namespace: com.bootcamp.payments
  fields:
    - name: transaction_id
      type: string
      description: "Unique transaction identifier"
      pii: false
      required: true
      primary_key: true

    - name: account_id
      type: string
      description: "Account identifier (FK to accounts)"
      pii: false
      required: true
      foreign_key: dev.payments.pg-payments.accounts.cdc.v1

    - name: card_id
      type: string
      description: "Card identifier (FK to cards)"
      pii: false
      required: false
      foreign_key: dev.payments.pg-payments.cards.cdc.v1

    - name: amount
      type: decimal
      precision: 12
      scale: 2
      description: "Transaction amount in BRL"
      pii: false
      required: true
      constraints:
        - "amount > 0"

    - name: mcc
      type: string
      description: "Merchant category code (ISO 18245)"
      pii: false
      required: false

    - name: uf
      type: string
      description: "Transaction state (UF)"
      pii: false
      required: true

    - name: cpf
      type: string
      description: "Customer CPF (11 digits)"
      pii: true
      required: true
      tags:
        - confluent:PII
        - confluent:LGPD

    - name: card_pan_masked
      type: string
      description: "Masked PAN (BIN + last4) — NEVER cleartext"
      pii: true
      required: false
      tags:
        - confluent:PCI

    - name: created_at
      type: timestamp-millis
      description: "Transaction creation timestamp"
      pii: false
      required: true

quality:
  expectations:
    completeness:
      - field: transaction_id
        threshold: 100%
      - field: amount
        threshold: 100%
      - field: account_id
        threshold: 100%

    validity:
      - rule: "amount > 0"
        threshold: 99.9%
      - rule: "LENGTH(cpf) = 11"
        threshold: 100%
      - rule: "card_pan_masked NOT LIKE '%[0-9]{16}%'"   # nunca PAN em claro
        threshold: 100%

    timeliness:
      max_latency_seconds: 60
      description: "CDC events must arrive within 60s of Postgres commit"

    uniqueness:
      - field: transaction_id
        scope: topic
        threshold: 99.99%

sla:
  availability: 99.9%
  throughput:
    average_msgs_per_sec: 50     # escala de lab
    peak_msgs_per_sec: 500
  latency:
    p95_ms: 500
    p99_ms: 2000

consumers:
  - name: flink-enrichment
    service_account: sa-lab-modulo-04
    access: read
    purpose: "Enrich transactions with account/card data"

  - name: fraud-detection
    service_account: sa-lab-modulo-04
    access: read
    purpose: "MATCH_RECOGNIZE fraud rules"

  - name: tableflow
    service_account: sa-lab-modulo-05
    access: read
    purpose: "Materialize enriched output as Iceberg (downstream)"

lineage:
  source:
    system: Postgres 16 (lab source DB)
    database: payments
    schema: public
    table: transactions
    connector: pg-payments-cdc-source

  downstream:
    - topic: dev.payments.flink.transactions.enriched.v1
      job: tx-enrichment
    - topic: dev.payments.flink.fraud.detected.v1
      job: fraud-velocity
    - table: iceberg://dev.payments.flink.transactions.enriched.v1
      via: tableflow
```

---

## Avro com tags PII/PCI

```json
{
  "type": "record",
  "name": "TransactionCdc",
  "namespace": "com.bootcamp.payments",
  "doc": "Transaction CDC event (Debezium Postgres, unwrapped)",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "card_id", "type": ["null", "string"], "default": null},
    {
      "name": "amount",
      "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2},
      "doc": "Amount in BRL"
    },
    {"name": "mcc", "type": ["null", "string"], "default": null},
    {"name": "uf", "type": "string"},
    {"name": "cpf", "type": "string", "confluent:tags": ["PII", "LGPD"]},
    {"name": "card_pan_masked", "type": ["null", "string"], "default": null, "confluent:tags": ["PCI"]},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

> O hook `validate-avro-pii.sh` **bloqueia** `.avsc` com campos sensíveis sem `confluent:tags` — ver [rules/schemas.md](../../rules/schemas.md).

---

## Templates por tipo de evento

### CDC
- Schema: espelha a tabela Postgres (+ metadados Debezium se `after.state.only=false`)
- Qualidade: 100% completude nas PKs, integridade referencial nas FKs
- SLA: <60s do commit no Postgres

### Enriched (saída Flink)
- Schema: desnormalizado com campos do join
- Qualidade: herda a fonte + completude do join
- SLA: <5s adicionais sobre o CDC

### Aggregated
- Schema: chaves de agregação + métricas + timestamps de janela
- Qualidade: completude da janela, sem gaps de eventos atrasados
- SLA: janela de 5min → resultado em até 6min

### Detected (alertas de fraude)
- Schema: regra disparada (`fraud_rule`), referência da entidade, evidências
- Qualidade: tolerância zero a falso-negativo em regras críticas
- SLA: <30s da detecção ao alerta

---

## Integração com Stream Governance (Live 3)

```bash
# Tags PII no Schema Registry (inventário LGPD automático)
confluent schema-registry subject update \
  "dev.payments.pg-payments.transactions.cdc.v1-value" \
  --compatibility backward

# Tags de negócio/PII e lineage são visíveis na UI:
# Environment → Stream Governance → Stream Catalog / Stream Lineage
```

Data contracts alimentam o Stream Governance com:
- Lineage automático (connector → topic → Flink → Tableflow)
- Descoberta de schema e documentação
- Inventário de campos PII/PCI em todos os topics
- Base para anonimização on-the-fly no Flink (Live 3)

---

## Best practices

1. **Contrato antes do topic** — design-first
2. **Versionar contratos** junto com as versões de schema (Git, no lab correspondente)
3. **Tags `confluent:tags` em todo campo PII/PCI** — o hook bloqueia sem elas
4. **Compatibilidade BACKWARD sempre** — evolução sem quebrar consumers (Curso 2, Aula 3)
5. **Listar todos os consumers** com service account e propósito
6. **Lineage da fonte ao lakehouse** (Postgres → Kafka → Flink → Iceberg)
7. **PAN mascarado por contrato** — nunca PAN em claro em nenhum topic

---

**Última atualização**: 2026-07-11
