-- =============================================================================
-- 02 · Quality gate — o padrão DLT implementado em Flink
-- Trata: pill #5 (null), #6 (malformed semântico), #8 (extreme), #9 (timezone)
--
-- O MESMO padrão da Aula 4 do Curso 3 (DLQ do Connect), agora na camada de
-- processamento: o fluxo bom segue, o veneno é DESVIADO com diagnóstico —
-- nunca descartado em silêncio, nunca derrubando o pipeline.
-- Dois statements, mesma fonte: split declarativo.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 2a. Fluxo bom → transactions.clean (só o que passa em TODAS as regras)
-- ---------------------------------------------------------------------------
CREATE TABLE `dev.payments.flink.transactions.clean.v1` AS
SELECT t.*
FROM `dev.payments.flink.transactions.deduped.v1` AS t
WHERE t.amount > 0
  AND t.amount <= 100000.00                        -- teto do domínio (extreme values)
  AND t.mcc <> ''
  AND CHAR_LENGTH(t.mcc) = 4
  AND REGEXP(t.mcc, '^[0-9]{4}$')                  -- mcc "INVA"/"99" barrado
  AND t.uf_transacao IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
    'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
  )                                                -- uf "XX"/"ZZ"/"BR" barrada
  AND t.event_time <= CURRENT_TIMESTAMP + INTERVAL '5' MINUTE;
  -- ^ evento "do futuro" = sintoma clássico de timezone errada na fonte (+3h)

-- ---------------------------------------------------------------------------
-- 2b. Veneno → DLQ do Flink, com o PORQUÊ viajando junto (DLT sem contexto
--     é cemitério — Curso 3, Aula 4). A mensagem original vai intacta.
-- ---------------------------------------------------------------------------
CREATE TABLE `dev.payments.flink.transactions.dlq.v1` AS
SELECT
  t.*,
  -- Diagnóstico: qual regra barrou (primeira que falhou)
  CASE
    WHEN t.amount <= 0 THEN 'AMOUNT_NOT_POSITIVE'
    WHEN t.amount > 100000.00 THEN 'AMOUNT_ABOVE_DOMAIN_MAX'
    WHEN NOT REGEXP(t.mcc, '^[0-9]{4}$') THEN 'MCC_MALFORMED'
    WHEN t.uf_transacao NOT IN (
      'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
      'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    ) THEN 'UF_INVALID'
    WHEN t.event_time > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
      THEN 'EVENT_TIME_IN_FUTURE_TZ_SUSPECT'
    ELSE 'UNKNOWN'
  END AS dlq_reason,
  CURRENT_TIMESTAMP AS quarantined_at
FROM `dev.payments.flink.transactions.deduped.v1` AS t
WHERE NOT (
  t.amount > 0
  AND t.amount <= 100000.00
  AND t.mcc <> ''
  AND CHAR_LENGTH(t.mcc) = 4
  AND REGEXP(t.mcc, '^[0-9]{4}$')
  AND t.uf_transacao IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
    'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
  )
  AND t.event_time <= CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
);

-- Validação no lab: total = clean + dlq (nada perdido, nada duplicado)
-- SELECT
--   (SELECT COUNT(*) FROM `dev.payments.flink.transactions.clean.v1`) AS clean,
--   (SELECT COUNT(*) FROM `dev.payments.flink.transactions.dlq.v1`)  AS quarantined;
-- E o DLQ crescendo é SINTOMA: alerta de profundidade no módulo 7.
