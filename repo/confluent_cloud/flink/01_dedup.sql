-- =============================================================================
-- 01 · Deduplicação — a releitura vira inofensiva
-- Trata: pill #1 (duplicate) e near-duplicate — poison-pill-patterns.md
--
-- Duplicata exata: mesmo transaction_id, mesmo payload (retry de producer,
-- rebalance de consumer, restart de conector CDC). Near-duplicate: mesmo
-- transaction_id, event_time ±1s — por isso a dedup é POR CHAVE DE NEGÓCIO
-- (transaction_id), nunca por hash do payload.
-- =============================================================================

-- State TTL OBRIGATÓRIO: dedup guarda 1 linha por transaction_id — sem TTL,
-- o state cresce para sempre. 1h cobre o retry mais atrasado do lab.
SET 'sql.state-ttl' = '1 HOURS';

CREATE TABLE `dev.payments.flink.transactions.deduped.v1` AS
WITH ranked AS (
  SELECT
    t.*,
    -- Primeira ocorrência vence; $rowtime desempata (chegada física)
    ROW_NUMBER() OVER (
      PARTITION BY t.transaction_id
      ORDER BY t.$rowtime ASC
    ) AS occurrence
  FROM `dev.payments.gen.transactions.created.v1` AS t
)
SELECT
  r.event_id,
  r.event_time,
  r.transaction_id,
  r.account_id,
  r.card_id,
  r.merchant_id,
  r.channel,
  r.amount,
  r.mcc,
  r.uf_transacao,
  r.status,
  r.decline_reason,
  r.fraud_flag,
  r.fraud_rule,
  r.created_at,
  r.authorized_at,
  r.settled_at
FROM ranked AS r
WHERE r.occurrence = 1;

-- Validação no lab (dedup funcionando = contagens divergem pela taxa injetada):
-- SELECT COUNT(*) FROM `dev.payments.gen.transactions.created.v1`;
-- SELECT COUNT(*) FROM `dev.payments.flink.transactions.deduped.v1`;
