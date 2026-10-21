-- =============================================================================
-- 03 · Enriquecimento + órfãos — temporal join com o estado da conta NO MOMENTO
-- Trata: pill #7 (orphan records) — poison-pill-patterns.md
--
-- Temporal join: cada transação casa com a versão da conta VIGENTE no
-- event_time dela (não a versão atual) — o padrão canônico de lookup em CDC.
-- Órfãos (account_id que não existe) não casam: LEFT JOIN os mantém vivos e
-- o segundo statement os desvia — integridade referencial como fluxo, não
-- como constraint.
--
-- Pré-requisito: tabela CDC de accounts com PRIMARY KEY e watermark (o topic
-- Debezium do módulo 3 atende — ver 00_watermarks.sql).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 3a. Enriquecidos: transação + tier/limite da conta no momento do evento
-- ---------------------------------------------------------------------------
CREATE TABLE `dev.payments.flink.transactions.enriched.v1` AS
SELECT
  t.transaction_id,
  t.event_time,
  t.account_id,
  t.card_id,
  t.merchant_id,
  t.channel,
  t.amount,
  t.mcc,
  t.uf_transacao,
  t.status,
  t.fraud_flag,
  t.fraud_rule,
  a.tier               AS account_tier,
  a.overdraft_limit    AS account_overdraft_limit,
  a.status             AS account_status
FROM `dev.payments.flink.transactions.clean.v1` AS t
JOIN `dev.payments.pg-payments.accounts.cdc.v1` FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id;

-- ---------------------------------------------------------------------------
-- 3b. Órfãos → DLQ com motivo (o join de cima os descartaria em silêncio;
--     LEFT + IS NULL os torna visíveis e reprocessáveis)
-- ---------------------------------------------------------------------------
CREATE TABLE `dev.payments.flink.transactions.orphans.v1` AS
SELECT
  t.*,
  'ACCOUNT_NOT_FOUND' AS dlq_reason,
  CURRENT_TIMESTAMP   AS quarantined_at
FROM `dev.payments.flink.transactions.clean.v1` AS t
LEFT JOIN `dev.payments.pg-payments.accounts.cdc.v1`
  FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id
WHERE a.account_id IS NULL;

-- Ciclo do órfão (mesmo desenho do DLT do Connect): desviado → diagnosticado
-- → conta criada/corrigida na fonte → reprocessado do topic de órfãos.
