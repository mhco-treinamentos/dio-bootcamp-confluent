# Flink SQL Job Builder

## Skill: `/flink-job-builder`

Build and deploy Flink SQL jobs on Confluent Cloud for real-time stream processing. Includes patterns for CDC enrichment, windowed aggregation, CEP glosa/fraud detection, PII/PHI tokenization, deduplication, and data quality monitoring.

## Usage

```
/flink-job-builder <pattern> [options]
```

### Patterns
- `cdc-enrichment` - Join CDC streams with reference data
- `windowed-aggregation` - Tumble/Hop/Session window aggregations
- `cep-glosa-detection` - MATCH_RECOGNIZE for glosa/fraud patterns
- `pii-tokenization` - Hash/mask PII and PHI fields for downstream
- `deduplication` - Deduplicate CDC events on re-delivery
- `data-quality` - Real-time data quality monitoring

---

## Compute Pool Management

```bash
# Create compute pool
confluent flink compute-pool create "workshop-flink" \
  --cloud aws \
  --region us-east-1 \
  --max-cfu 20 \
  --environment <env-id>

# Submit SQL statement
confluent flink statement create \
  --sql "$(cat job.sql)" \
  --compute-pool <pool-id> \
  --environment <env-id>

# List running statements
confluent flink statement list --environment <env-id> --compute-pool <pool-id>
```

---

## Pattern 1: CDC Stream Enrichment

```sql
-- Enrich guias TISS with membro and prestador details
-- CFU Estimate: 6-10 (3-way join with state)

CREATE TABLE enriched_guias
WITH (
  'changelog.mode' = 'append',
  'kafka.partitions' = '24',
  'value.format' = 'avro-confluent'
)
AS SELECT
  g.GUIA_ID,
  g.NUMERO_GUIA,
  g.TIPO_GUIA,
  g.VALOR_AUTORIZADO,
  g.STATUS AS guia_status,
  g.DATA_SOLICITACAO AS guia_timestamp,
  m.MEMBRO_ID,
  m.NOME AS membro_nome,
  m.PLANO_ID,
  m.TIPO_PLANO,
  p.NOME AS prestador_nome,
  p.ESPECIALIDADE,
  p.CRM,
  p.MUNICIPIO AS prestador_municipio,
  CURRENT_TIMESTAMP AS enriched_at
FROM `prod.saude.oracle-autorizacoes.GUIAS.cdc.v1` g
JOIN `prod.saude.oracle-beneficiarios.MEMBROS.cdc.v1` m
  ON g.MEMBRO_ID = m.MEMBRO_ID
JOIN `prod.saude.oracle-prestadores.PRESTADORES.cdc.v1` p
  ON g.PRESTADOR_ID = p.PRESTADOR_ID;
```

---

## Pattern 2: Windowed Aggregation

### Tumbling Window — Hourly Guia Summary

```sql
-- CFU Estimate: 4-8 (window state, key cardinality)

CREATE TABLE hourly_guia_summary
WITH (
  'kafka.partitions' = '12',
  'value.format' = 'avro-confluent'
)
AS SELECT
  PRESTADOR_ID,
  TIPO_GUIA,
  TUMBLE_START(DATA_SOLICITACAO, INTERVAL '1' HOUR) AS window_start,
  TUMBLE_END(DATA_SOLICITACAO, INTERVAL '1' HOUR) AS window_end,
  COUNT(*) AS guia_count,
  SUM(VALOR_AUTORIZADO) AS total_autorizado,
  AVG(VALOR_AUTORIZADO) AS avg_autorizado,
  MAX(VALOR_AUTORIZADO) AS max_autorizado,
  COUNT(CASE WHEN TIPO_GUIA = 'INTERNACAO' THEN 1 END) AS internacao_count,
  COUNT(CASE WHEN TIPO_GUIA = 'SP_SADT' THEN 1 END) AS sadt_count
FROM `prod.saude.oracle-autorizacoes.GUIAS.cdc.v1`
GROUP BY
  PRESTADOR_ID,
  TIPO_GUIA,
  TUMBLE(DATA_SOLICITACAO, INTERVAL '1' HOUR);
```

### Session Window — Activity-Based Grouping

```sql
CREATE TABLE session_activity
WITH ('kafka.partitions' = '12', 'value.format' = 'avro-confluent')
AS SELECT
  MEMBRO_ID,
  SESSION_START(DATA_SOLICITACAO, INTERVAL '30' MINUTE) AS session_start,
  SESSION_END(DATA_SOLICITACAO, INTERVAL '30' MINUTE) AS session_end,
  COUNT(*) AS guia_count,
  SUM(VALOR_AUTORIZADO) AS total_autorizado
FROM `prod.saude.oracle-autorizacoes.GUIAS.cdc.v1`
GROUP BY MEMBRO_ID, SESSION(DATA_SOLICITACAO, INTERVAL '30' MINUTE);
```

---

## Pattern 3: CEP Glosa/Fraud Detection

```sql
-- Detect 5+ glosas for same prestador within 30 days (potential billing abuse)
-- CFU Estimate: 6-12 (CEP pattern complexity)

CREATE TABLE glosa_alerts
WITH ('kafka.partitions' = '6', 'value.format' = 'avro-confluent')
AS SELECT *
FROM `prod.saude.oracle-faturamento.GLOSAS.cdc.v1`
MATCH_RECOGNIZE (
  PARTITION BY PRESTADOR_ID
  ORDER BY DATA_GLOSA
  MEASURES
    FIRST(A.GUIA_ID) AS first_guia_id,
    LAST(A.GUIA_ID) AS last_guia_id,
    COUNT(A.GUIA_ID) AS glosa_count,
    SUM(A.VALOR_GLOSADO) AS total_glosado,
    FIRST(A.DATA_GLOSA) AS window_start,
    LAST(A.DATA_GLOSA) AS window_end,
    'REPEATED_GLOSA_PRESTADOR' AS alert_type,
    CURRENT_TIMESTAMP AS detected_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A{5,} WITHIN INTERVAL '30' DAY)
  DEFINE
    A AS A.VALOR_GLOSADO > 500.00 AND A.MOTIVO_GLOSA = 'PROCEDIMENTO_NAO_COBERTO'
) AS glosa_pattern;
```

---

## Pattern 4: PII/PHI Tokenization

```sql
-- SHA-256 tokenization for analytics consumers
-- CFU Estimate: 2-4

CREATE TABLE membros_tokenized
WITH ('kafka.partitions' = '6', 'value.format' = 'avro-confluent')
AS SELECT
  MEMBRO_ID,
  SHA2(CAST(CPF AS VARCHAR), 256) AS cpf_token,
  SHA2(CAST(CNS AS VARCHAR), 256) AS cns_token,
  SHA2(CAST(EMAIL AS VARCHAR), 256) AS email_token,
  SHA2(CAST(TELEFONE AS VARCHAR), 256) AS telefone_token,
  TIPO_PLANO,
  STATUS_MEMBRO,
  DATA_ADESAO,
  FAIXA_ETARIA,
  UF,
  CIDADE,
  CURRENT_TIMESTAMP AS tokenized_at
FROM `prod.saude.oracle-beneficiarios.MEMBROS.cdc.v1`;
```

---

## Pattern 5: Deduplication

```sql
-- Deduplicate CDC events on re-delivery
-- CFU Estimate: 4-6

CREATE TABLE guias_deduped
WITH ('kafka.partitions' = '24', 'value.format' = 'avro-confluent')
AS SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY GUIA_ID ORDER BY `__source.ts_ms` DESC) AS rn
  FROM `prod.saude.oracle-autorizacoes.GUIAS.cdc.v1`
) WHERE rn = 1;
```

---

## Pattern 6: Data Quality Monitoring

```sql
-- Real-time quality metrics every 5 minutes
-- CFU Estimate: 2-4

CREATE TABLE data_quality_metrics
WITH ('kafka.partitions' = '3', 'value.format' = 'avro-confluent')
AS SELECT
  'GUIAS' AS entity,
  TUMBLE_START(DATA_SOLICITACAO, INTERVAL '5' MINUTE) AS window_start,
  COUNT(*) AS total_records,
  COUNT(CASE WHEN VALOR_AUTORIZADO IS NULL THEN 1 END) AS null_valores,
  COUNT(CASE WHEN VALOR_AUTORIZADO <= 0 THEN 1 END) AS invalid_valores,
  COUNT(DISTINCT GUIA_ID) AS unique_ids,
  COUNT(*) - COUNT(DISTINCT GUIA_ID) AS duplicate_count,
  CAST((1.0 - (
    CAST(COUNT(CASE WHEN VALOR_AUTORIZADO IS NULL OR VALOR_AUTORIZADO <= 0 THEN 1 END) AS DOUBLE) /
    GREATEST(CAST(COUNT(*) AS DOUBLE), 1.0)
  )) * 100 AS DECIMAL(5,2)) AS quality_score_pct
FROM `prod.saude.oracle-autorizacoes.GUIAS.cdc.v1`
GROUP BY TUMBLE(DATA_SOLICITACAO, INTERVAL '5' MINUTE);
```

---

## CFU Sizing Guide

| Job Type | CFUs | Notes |
|----------|------|-------|
| Simple filter/projection | 2-4 | PII tokenization |
| Stream-table join (2 tables) | 4-8 | State for tables |
| Stream-table join (3+ tables) | 8-16 | State grows with tables |
| Tumbling window | 4-8 | Key cardinality dependent |
| Session window | 6-12 | Variable state |
| CEP (MATCH_RECOGNIZE) | 6-12 | Pattern complexity |
| Deduplication | 4-8 | State per unique key |
| Quality monitoring | 2-4 | Windowed counters |

---

## Best Practices

1. **Test in dev** before deploying to production
2. **Start with minimal CFUs** and scale up based on metrics
3. **Use `latest-offset`** scan mode for new jobs
4. **Apply filters early** to reduce state size
5. **Use `avro-confluent` format** for all sink tables
6. **Document every job** with CFU estimate and owner
7. **Tokenize PII/PHI** in separate jobs for clean analytics streams
8. **Monitor quality** with dedicated monitoring jobs

---

**Version**: 1.0
**Last Updated**: 2026-02-12
