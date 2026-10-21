-- =============================================================================
-- 00 · Watermarks — o pré-requisito de TODO tratamento event-time
-- Trata: pill #2 (late-arriving) e #3 (out-of-order) — poison-pill-patterns.md
--
-- Confluent Cloud dá watermark default sobre $rowtime (tempo de ingestão).
-- Para lógica de negócio, a watermark vai SEMPRE no campo de evento:
-- out-of-order dentro do delay é reordenado; atrasado além dele é descartado
-- (por isso o 04_late_audit.sql mede o que ficou de fora).
-- =============================================================================

-- Stream do gerador (fonte: producers + TransactionGenerator)
ALTER TABLE `dev.payments.gen.transactions.created.v1`
MODIFY WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND;

-- Trade-off ensinado no lab (escolher pelo caso de uso):
--   INTERVAL '5'  SECOND → anti-fraude bloqueante (perde mais late, decide rápido)
--   INTERVAL '30' SECOND → balanceado (default dos labs)
--   INTERVAL '5'  MINUTE → analítico (janela fecha tarde, quase nada é late)

-- Topic CDC (Debezium): a watermark vai no ts_ms do envelope (commit na fonte),
-- NUNCA no $rowtime — o tempo de ingestão de um evento CDC não é tempo de negócio.
-- ALTER TABLE `dev.payments.pg-payments.transactions.cdc.v1`
-- MODIFY WATERMARK FOR ts_ms AS ts_ms - INTERVAL '30' SECOND;
