# Flink SQL Job Builder

## Skill: `/flink-job-builder`

Constrói **statements Flink SQL no Confluent Cloud** (Flink totalmente gerenciado: compute pools + statements — **sem JARs, sem SQL Client self-managed, sem gerenciamento de checkpoint**). Padrões do módulo 4 e do desafio final: enriquecimento de CDC, agregações em janela, detecção de fraude com `MATCH_RECOGNIZE`, tokenização de PII e deduplicação.

## Usage

```
/flink-job-builder <pattern> [options]
```

### Patterns
- `cdc-enrichment` — Join de streams CDC com dados de referência
- `windowed-aggregation` — Janelas tumbling/hopping/session
- `fraud-detection` — `MATCH_RECOGNIZE` para padrões anti-fraude ([rules.md](../../rules.md))
- `pii-tokenization` — Hash/mascaramento de PII (LGPD — Live 3)
- `deduplication` — Deduplicar eventos CDC re-entregues
- `data-quality` — Monitoramento de qualidade em tempo real

---

## Compute pool e statements (gerenciado)

```bash
# Compute pool (ou use o plugin quickstart — ver skills/confluent/cloud-setup.md)
confluent flink compute-pool create bootcamp-flink \
  --cloud aws --region us-east-1 --max-cfu 10

# Shell SQL interativo (desenvolvimento nos labs)
confluent flink shell --compute-pool <pool-id>

# Statement como deploy (versionado no lab)
confluent flink statement create fraud-velocity \
  --sql "$(cat labs/modulo-04-flink/fraud_velocity.sql)" \
  --compute-pool <pool-id> \
  --database <cluster-name>

# Operação
confluent flink statement list
confluent flink statement describe fraud-velocity
confluent flink statement exception list fraud-velocity
confluent flink statement delete fraud-velocity     # TEARDOWN: statement RUNNING cobra 24/7
```

> No Flink gerenciado, cada **topic com schema é uma tabela** automaticamente (catálogo = environment, database = cluster). Checkpoints/estado são gerenciados — o aluno só escreve SQL.

---

## Pattern 1: CDC Stream Enrichment

```sql
-- Enriquece transações (CDC) com dados de conta e cartão
-- CFU estimate: 4-8 (join com estado)

CREATE TABLE `dev.payments.flink.transactions.enriched.v1` (
  PRIMARY KEY (transaction_id) NOT ENFORCED
) DISTRIBUTED INTO 6 BUCKETS
WITH ('value.format' = 'avro-registry')
AS SELECT
  t.transaction_id,
  t.amount,
  t.mcc,
  t.uf AS transaction_uf,
  t.created_at,
  a.account_id,
  a.account_tier,
  a.uf AS account_uf,
  c.card_id,
  c.card_brand,
  CURRENT_TIMESTAMP AS enriched_at
FROM `dev.payments.pg-payments.transactions.cdc.v1` t
JOIN `dev.payments.pg-payments.accounts.cdc.v1` a
  ON t.account_id = a.account_id
JOIN `dev.payments.pg-payments.cards.cdc.v1` c
  ON t.card_id = c.card_id;
```

## Pattern 2: Windowed Aggregation

```sql
-- Tumbling: volume por conta a cada 1 minuto (base do saldo agregado)
-- CFU estimate: 2-4

CREATE TABLE `dev.payments.flink.balance.aggregated.v1`
AS SELECT
  account_id,
  window_start,
  window_end,
  COUNT(*) AS tx_count,
  SUM(amount) AS total_amount,
  MAX(amount) AS max_amount
FROM TABLE(
  TUMBLE(TABLE `dev.payments.pg-payments.transactions.cdc.v1`,
         DESCRIPTOR(created_at), INTERVAL '1' MINUTE))
GROUP BY account_id, window_start, window_end;
```

> Janelas no Flink do Confluent Cloud usam **TVFs** (`TUMBLE`/`HOP`/`SESSION` como table functions), não o `GROUP BY TUMBLE(...)` legado. Session windows: `SESSION(TABLE ..., DESCRIPTOR(created_at), INTERVAL '30' MINUTE)`.

## Pattern 3: Fraud Detection (MATCH_RECOGNIZE)

```sql
-- RULE_VELOCITY: 3+ transações no mesmo cartão em <60s (rules.md)
-- CFU estimate: 4-8

CREATE TABLE `dev.payments.flink.fraud.detected.v1`
AS SELECT *
FROM `dev.payments.pg-payments.transactions.cdc.v1`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    FIRST(A.transaction_id) AS first_tx_id,
    LAST(A.transaction_id) AS last_tx_id,
    COUNT(A.transaction_id) AS tx_count,
    SUM(A.amount) AS total_amount,
    FIRST(A.created_at) AS window_start,
    LAST(A.created_at) AS window_end,
    'RULE_VELOCITY' AS fraud_rule
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A{3,}) WITHIN INTERVAL '60' SECOND
  DEFINE
    A AS TRUE
) AS velocity_pattern;
```

Demais regras do bootcamp (`RULE_GEO_IMPOSSIBLE`, `RULE_AMOUNT_OUTLIER`, `RULE_NIGHT_OWL`) seguem o mesmo esqueleto — tabela em [rules.md](../../rules.md). Classificação por modelo (`ML_PREDICT`) é o módulo 6.

## Pattern 4: PII Tokenization (LGPD — Live 3)

```sql
-- Anonimização on-the-fly antes do consumo analítico
-- CFU estimate: 1-2

CREATE TABLE `dev.payments.flink.accounts.tokenized.v1`
AS SELECT
  account_id,
  SHA2(CAST(cpf AS STRING), 256) AS cpf_token,
  SHA2(CAST(email AS STRING), 256) AS email_token,
  account_tier,
  uf,
  created_at
FROM `dev.payments.pg-payments.accounts.cdc.v1`;
```

## Pattern 5: Deduplication

```sql
-- Deduplicar CDC re-entregue (at-least-once na prática — módulo 1)
-- CFU estimate: 2-4

CREATE TABLE `dev.payments.flink.transactions.deduped.v1`
AS SELECT transaction_id, account_id, card_id, amount, mcc, uf, created_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY $rowtime DESC) AS rn
  FROM `dev.payments.pg-payments.transactions.cdc.v1`
) WHERE rn = 1;
```

## Pattern 6: Data Quality Monitoring

```sql
-- Métricas de qualidade a cada 5 minutos
-- CFU estimate: 1-2

CREATE TABLE `dev.payments.flink.quality.metrics.v1`
AS SELECT
  'transactions' AS entity,
  window_start,
  COUNT(*) AS total_records,
  COUNT(*) FILTER (WHERE amount IS NULL) AS null_amounts,
  COUNT(*) FILTER (WHERE amount <= 0) AS invalid_amounts,
  COUNT(DISTINCT transaction_id) AS unique_ids
FROM TABLE(
  TUMBLE(TABLE `dev.payments.pg-payments.transactions.cdc.v1`,
         DESCRIPTOR(created_at), INTERVAL '5' MINUTE))
GROUP BY window_start;
```

---

## CFU Sizing (labs)

| Tipo de statement | CFUs | Fator |
|-------------------|------|-------|
| Projeção/filtro/tokenização | 1-2 | Taxa de mensagens |
| Join 2 tabelas | 2-4 | Estado das tabelas |
| Join 3+ tabelas | 4-8 | Estado cresce por tabela |
| Janela tumbling | 2-4 | Cardinalidade da chave |
| Session window / CEP | 4-8 | Estado variável / complexidade |
| Deduplicação | 2-4 | Chaves únicas |

`--max-cfu 10` no pool cobre todos os labs; o pool escala a zero quando ocioso.

---

## Best practices

1. **Desenvolver no `confluent flink shell`**, versionar o `.sql` no lab e fazer deploy via `statement create`
2. **Filtrar cedo** para reduzir estado; janelas menores = menos estado
3. **`value.format = avro-registry`** nas tabelas de saída — pré-requisito do Tableflow (módulo 5)
4. **Nomear statements** (`fraud-velocity`, `tx-enrichment`) — facilita observabilidade e teardown
5. **Documentar CFU estimate** em comentário no `.sql`
6. **Deletar statements ao fim do lab** — RUNNING cobra continuamente (créditos!)
7. **Watermarks**: fontes de lab precisam de eventos contínuos para a janela fechar — usar os geradores

---

**Última atualização**: 2026-07-11
