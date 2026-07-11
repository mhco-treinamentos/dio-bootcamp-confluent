# Data Contract Designer

## Skill: `/data-contract-designer`

Create data contracts (YAML/Avro) defining schema, quality expectations, SLAs, and ownership for Kafka topics on the workshop's Confluent Cloud platform. Ensures producer-consumer alignment and governance.

## Usage

```
/data-contract-designer <domain> <entity> [options]
```

### Options
- `--format` - Contract format: yaml, avro, json-schema (default: yaml)
- `--domain` - Domain: saude, beneficiarios, autorizacoes, prestadores, faturamento
- `--type` - Event type: cdc, enriched, aggregated, alert

---

## Data Contract Structure

### Full Contract Template (YAML)

```yaml
apiVersion: v1
kind: DataContract
metadata:
  name: autorizacoes-guias-cdc
  version: "1.0.0"
  owner:
    team: data-platform
    contact: data-platform@example.com
  domain: autorizacoes
  tags:
    - cdc
    - saude
    - lgpd
    - ans

topic:
  name: prod.saude.oracle-autorizacoes.guias.cdc.v1
  partitions: 24
  replication_factor: 3
  retention_ms: 2592000000  # 30 days
  cleanup_policy: delete
  compression: lz4

schema:
  format: avro
  compatibility: BACKWARD
  registry_subject: prod.saude.oracle-autorizacoes.guias.cdc.v1-value
  fields:
    - name: guia_id
      type: string
      description: "Unique guia TISS identifier"
      pii: false
      required: true
      primary_key: true

    - name: membro_id
      type: string
      description: "Membro identifier (FK to membros)"
      pii: false
      required: true
      foreign_key: prod.saude.oracle-beneficiarios.membros.cdc.v1

    - name: prestador_id
      type: string
      description: "Prestador identifier (FK to prestadores)"
      pii: false
      required: true
      foreign_key: prod.saude.oracle-prestadores.prestadores.cdc.v1

    - name: valor_autorizado
      type: decimal
      precision: 15
      scale: 2
      description: "Authorized amount in BRL"
      pii: false
      required: true
      constraints:
        - "valor_autorizado > 0"

    - name: tipo_guia
      type: enum
      symbols: [CONSULTA, SP_SADT, INTERNACAO, ODONTOLOGICA, HONORARIOS]
      description: "TISS guide type"
      pii: false
      required: true

    - name: status
      type: enum
      symbols: [SOLICITADA, EM_ANALISE, AUTORIZADA, NEGADA, EXECUTADA, FATURADA, PAGA, GLOSADA]
      description: "Guia lifecycle status"
      pii: false
      required: true

    - name: cid_10
      type: string
      description: "ICD-10 diagnosis code"
      phi: true
      required: false
      tags:
        - confluent:PHI
        - confluent:SAUDE
        - confluent:ANS

    - name: data_solicitacao
      type: timestamp-millis
      description: "Guia request timestamp"
      pii: false
      required: true

quality:
  expectations:
    completeness:
      - field: guia_id
        threshold: 100%
      - field: valor_autorizado
        threshold: 100%
      - field: membro_id
        threshold: 100%

    validity:
      - rule: "valor_autorizado > 0"
        threshold: 99.9%
      - rule: "tipo_guia IN ('CONSULTA','SP_SADT','INTERNACAO','ODONTOLOGICA','HONORARIOS')"
        threshold: 100%
      - rule: "status IN ('SOLICITADA','EM_ANALISE','AUTORIZADA','NEGADA','EXECUTADA','FATURADA','PAGA','GLOSADA')"
        threshold: 100%

    timeliness:
      max_latency_seconds: 60
      description: "CDC events must arrive within 60s of Oracle commit"

    uniqueness:
      - field: guia_id
        scope: topic
        threshold: 99.99%

sla:
  availability: 99.9%
  throughput:
    average_msgs_per_sec: 200
    peak_msgs_per_sec: 2000
  latency:
    p50_ms: 100
    p95_ms: 500
    p99_ms: 2000

consumers:
  - name: flink-enrichment
    service_account: sa-workshop-flink-prod
    access: read
    purpose: "Enrich guias with membro/prestador data"

  - name: snowflake-ingest
    service_account: sa-workshop-snowflake-ingest
    access: read
    purpose: "Ingest to Snowflake semantic layer via Snowpipe Streaming"

  - name: glosa-detection
    service_account: sa-workshop-consumer-glosa
    access: read
    purpose: "Real-time glosa pattern detection"

lineage:
  source:
    system: Oracle Autorizações
    database: PAYMENTS_DB
    schema: AUTORIZACOES
    table: GUIAS
    connector: oracle-cdc-autorizacoes

  downstream:
    - topic: prod.saude.flink.guias.enriched.v1
      job: flink-guia-enrichment
    - topic: prod.saude.flink.glosa_alerts.detected.v1
      job: flink-glosa-detection
    - database: snowflake://PAYMENTS_RAW.SAUDE.GUIAS
      connector: snowpipe-streaming
```

---

## Avro Schema with PII/PHI Tags

```json
{
  "type": "record",
  "name": "Guia",
  "namespace": "com.workshop.payments.autorizacoes",
  "doc": "Guia TISS CDC event",
  "fields": [
    {
      "name": "guia_id",
      "type": "string",
      "doc": "Unique guia identifier"
    },
    {
      "name": "membro_id",
      "type": "string",
      "doc": "Membro identifier"
    },
    {
      "name": "prestador_id",
      "type": "string",
      "doc": "Prestador identifier"
    },
    {
      "name": "valor_autorizado",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 15,
        "scale": 2
      },
      "doc": "Authorized amount in BRL"
    },
    {
      "name": "tipo_guia",
      "type": {
        "type": "enum",
        "name": "TipoGuia",
        "symbols": ["CONSULTA", "SP_SADT", "INTERNACAO", "ODONTOLOGICA", "HONORARIOS"]
      }
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "StatusGuia",
        "symbols": ["SOLICITADA", "EM_ANALISE", "AUTORIZADA", "NEGADA", "EXECUTADA", "FATURADA", "PAGA", "GLOSADA"]
      }
    },
    {
      "name": "cid_10",
      "type": ["null", "string"],
      "default": null,
      "doc": "ICD-10 diagnosis code",
      "confluent:tags": ["PHI", "SAUDE", "ANS"]
    },
    {
      "name": "data_solicitacao",
      "type": { "type": "long", "logicalType": "timestamp-millis" }
    }
  ]
}
```

---

## Contract Templates by Domain

### CDC Entity Contract

For raw CDC events from Oracle:
- Schema: matches Oracle table structure
- Quality: 100% completeness on PKs, referential integrity on FKs
- SLA: <60s latency from Oracle commit

### Enriched Event Contract

For Flink-processed enriched streams:
- Schema: denormalized with joined fields
- Quality: inherits source quality + join completeness
- SLA: <5s additional latency on top of CDC

### Aggregated Metric Contract

For windowed aggregation outputs:
- Schema: aggregation keys + metric columns + window timestamps
- Quality: window completeness, no late-event gaps
- SLA: depends on window size (5min window → results within 6min)

### Alert Contract

For glosa/anomaly alerts:
- Schema: alert type, severity, entity reference, evidence fields
- Quality: zero false-negative tolerance on critical alerts
- SLA: <30s detection-to-alert latency

---

## Governance Integration

### Schema Registry Tags

```bash
# Tag PHI fields in Schema Registry
confluent schema-registry subject update \
  "prod.saude.oracle-autorizacoes.guias.cdc.v1-value" \
  --tag "PHI" --field "cid_10"

confluent schema-registry subject update \
  "prod.saude.oracle-autorizacoes.guias.cdc.v1-value" \
  --tag "ANS" --field "cid_10"
```

### Stream Governance Catalog

Data contracts integrate with Confluent Stream Governance for:
- Automatic lineage tracking
- Schema discovery and documentation
- PII/PHI field inventory across all topics
- Data quality monitoring dashboards

---

## Best Practices

1. **Create contracts before topics** — design-first approach
2. **Version contracts** alongside schema versions
3. **Tag all PII/PHI fields** with `confluent:tags` in Avro schemas
4. **Define SLAs** for every production topic
5. **List all consumers** with their purpose and access level
6. **Track lineage** from source to all downstream destinations
7. **Review contracts quarterly** for accuracy and relevance
8. **Store contracts in Git** alongside Terraform topic definitions

---

**Version**: 1.0
**Last Updated**: 2026-02-12
