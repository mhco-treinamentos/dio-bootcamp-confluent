-- =============================================================================
-- Bootcamp Confluent Cloud 2026 — Teardown do dataset Neon
--
-- ⚠️ DESTRUTIVO: apaga TODO o dataset do bootcamp (schema payments inteiro).
-- Rode SOMENTE ao encerrar o bootcamp ou para recriar do zero (Replay rápido:
-- 01_ddl.sql → 02_seed.sql → 03_cdc_setup.sql).
--
-- Ordem importa: primeiro delete o CONECTOR no Confluent Cloud (senão o slot
-- órfão segura WAL e enche o storage do Neon):
--   confluent connect cluster delete <connector-id> --cluster <cluster-id>
-- =============================================================================

-- 1. Replication slot (se o conector não o removeu ao ser deletado)
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'bootcamp_cdc_slot' AND NOT active;

-- 2. Publication
DROP PUBLICATION IF EXISTS bootcamp_cdc_pub;

-- 3. Dataset inteiro (remove também os grants do cdc_user sobre as tabelas)
DROP SCHEMA IF EXISTS payments CASCADE;

-- 4. Role do conector (depois do schema, para não sobrar grant pendente)
DROP ROLE IF EXISTS cdc_user;

-- Verificação pós-teardown (tudo deve voltar vazio):
SELECT slot_name FROM pg_replication_slots;
SELECT pubname FROM pg_publication;
SELECT table_name FROM information_schema.tables WHERE table_schema = 'payments';
