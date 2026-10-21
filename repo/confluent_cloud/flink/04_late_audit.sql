-- =============================================================================
-- 04 · Auditoria de late data — medir o que a watermark deixou de fora
-- Trata: pill #2 (late-arriving) — poison-pill-patterns.md
--
-- Flink SQL não tem side-output de late events: o tratamento é (1) escolher o
-- delay certo na watermark (00_watermarks.sql) e (2) MEDIR a lateness real
-- para saber se o delay escolhido cabe. Este statement é o medidor.
-- =============================================================================

-- Lateness observada: diferença entre a chegada física ($rowtime) e o tempo
-- de negócio (event_time). O pill LATE_ARRIVING (1–72h) aparece aqui como
-- cauda longa — e prova por que 30s de watermark não salva evento de 3 dias.
CREATE TABLE `dev.payments.flink.transactions.lateness.v1` AS
SELECT
  window_start,
  window_end,
  COUNT(*) AS txn_count,
  -- p50/p95/max de atraso em segundos: o insumo para calibrar a watermark
  APPROX_PERCENTILE(TIMESTAMPDIFF(SECOND, event_time, $rowtime), 0.5) AS lateness_p50_s,
  APPROX_PERCENTILE(TIMESTAMPDIFF(SECOND, event_time, $rowtime), 0.95) AS lateness_p95_s,
  MAX(TIMESTAMPDIFF(SECOND, event_time, $rowtime)) AS lateness_max_s,
  -- Quantos ficariam fora com o delay atual (30s)
  SUM(CASE WHEN TIMESTAMPDIFF(SECOND, event_time, $rowtime) > 30 THEN 1 ELSE 0 END)
    AS would_be_dropped_at_30s
FROM TABLE(
  TUMBLE(
    TABLE `dev.payments.gen.transactions.created.v1`,
    DESCRIPTOR($rowtime),
    INTERVAL '5' MINUTE
  )
)
GROUP BY window_start, window_end;

-- Decisão de arquitetura que o lab extrai daqui:
--   lateness_p95 < 30s  → watermark de 30s segura o caso comum
--   cauda de horas/dias → não é problema de watermark: é REPROCESSO
--     (batch de reconciliação, ou upsert idempotente no destino — Curso 3, Aula 3)
