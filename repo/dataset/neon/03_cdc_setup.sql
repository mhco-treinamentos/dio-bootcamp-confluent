-- =============================================================================
-- Bootcamp Confluent Cloud 2026 — Setup de CDC no Neon (Curso 3)
--
-- PRÉ-REQUISITO (fora deste script): habilitar Logical Replication no projeto
-- Neon (Project settings → Beta/Logical Replication). É o equivalente
-- gerenciado do wal_level=logical — sem isso o conector da Aula 2 não conecta.
--
-- Rode DEPOIS do 01_ddl.sql (e idealmente do 02_seed.sql, para o snapshot
-- inicial ter o que capturar).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Role dedicada do conector (least privilege — princípio da identidade de
--    aplicação do Curso 2). NUNCA usar o owner do banco no conector.
--    Troque a senha e exporte como PG_CDC_PASSWORD (nunca commitar).
-- -----------------------------------------------------------------------------
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'troque-esta-senha';

GRANT USAGE ON SCHEMA payments TO cdc_user;
GRANT SELECT ON payments.accounts, payments.cards, payments.transactions TO cdc_user;
-- O conector também cria o replication slot na primeira execução (slot.name).

-- -----------------------------------------------------------------------------
-- 2. REPLICA IDENTITY FULL — sem isso, eventos UPDATE/DELETE trazem só a PK no
--    campo `before` do envelope Debezium. FULL grava a linha inteira no WAL
--    (custo aceitável nos labs; é o que torna a demo before/after possível).
-- -----------------------------------------------------------------------------
ALTER TABLE payments.accounts     REPLICA IDENTITY FULL;
ALTER TABLE payments.cards        REPLICA IDENTITY FULL;
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;

-- -----------------------------------------------------------------------------
-- 3. Publication apenas com as tabelas capturadas (nunca FOR ALL TABLES).
--    Para capturar customers/merchants em labs futuros:
--    ALTER PUBLICATION bootcamp_cdc_pub ADD TABLE payments.customers;
-- -----------------------------------------------------------------------------
CREATE PUBLICATION bootcamp_cdc_pub
    FOR TABLE payments.accounts, payments.cards, payments.transactions;

-- -----------------------------------------------------------------------------
-- 4. Verificação
-- -----------------------------------------------------------------------------
SELECT * FROM pg_publication_tables WHERE pubname = 'bootcamp_cdc_pub';
SHOW wal_level;  -- esperado: logical (após o toggle do projeto Neon)
