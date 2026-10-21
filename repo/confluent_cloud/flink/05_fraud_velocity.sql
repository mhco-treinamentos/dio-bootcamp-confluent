-- =============================================================================
-- 05 · Fraude determinística — MATCH_RECOGNIZE sobre o fluxo limpo
-- Consome: bursts injetados pelo TransactionGenerator (RULE_VELOCITY:
-- 3+ transações no mesmo cartão em <60s) — o ground truth do gerador
-- (fraud_flag) valida o detector: o CEP deve achar o que foi plantado.
--
-- Regras determinísticas → MATCH_RECOGNIZE (aqui);
-- classificação probabilística → ML_PREDICT (módulo 6), aplicada SÓ ao
-- subconjunto sinalizado (custo por chamada).
-- =============================================================================

CREATE TABLE `dev.payments.flink.fraud.detected.v1` AS
SELECT *
FROM `dev.payments.flink.transactions.clean.v1`
MATCH_RECOGNIZE (
  PARTITION BY card_id                 -- sempre na chave de negócio
  ORDER BY event_time
  MEASURES
    FIRST(T.event_time)         AS first_txn_time,
    LAST(T.event_time)          AS last_txn_time,
    COUNT(T.transaction_id)     AS txn_count,
    SUM(T.amount)               AS total_amount,
    'RULE_VELOCITY'             AS rule
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW       -- evita matches sobrepostos duplicando alertas
  PATTERN (T{3,}) WITHIN INTERVAL '60' SECOND   -- SEMPRE com WITHIN: limita o state
  DEFINE
    T AS T.card_id IS NOT NULL AND T.amount > 0
);

-- Validação no lab, fechando o ciclo com o gerador:
--   detectados ≈ transações com fraud_rule = 'RULE_VELOCITY' no ground truth
-- SELECT COUNT(*) FROM `dev.payments.flink.fraud.detected.v1`;
-- SELECT COUNT(DISTINCT card_id)
-- FROM `dev.payments.flink.transactions.clean.v1`
-- WHERE fraud_rule = 'RULE_VELOCITY';
