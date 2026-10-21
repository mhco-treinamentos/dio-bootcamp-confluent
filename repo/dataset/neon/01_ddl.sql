-- =============================================================================
-- Bootcamp Confluent Cloud 2026 — DDL do "banco legado" (Neon Postgres)
-- Domínio: payments (dados 100% sintéticos)
--
-- Execute no SQL Editor do Neon (projeto bootcamp-dio) ou via psql:
--   psql "postgresql://$PG_USER:$PG_PASSWORD@$PG_HOST/$PG_DATABASE" -f 01_ddl.sql
--
-- Toda tabela tem PRIMARY KEY — a key do evento CDC (chave → partição → ordem).
-- Money é NUMERIC(15,2) — com decimal.handling.mode=precise no conector,
-- vira bytes+decimal no Avro (nunca float — ver .claude/rules/schemas.md).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS payments;

-- -----------------------------------------------------------------------------
-- customers — clientes pessoa física (entidade master)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.customers (
    customer_id     UUID PRIMARY KEY,
    cpf             VARCHAR(11)  NOT NULL UNIQUE,          -- PII/LGPD: sem pontuação
    nome            VARCHAR(200) NOT NULL,                 -- PII/LGPD
    data_nascimento DATE         NOT NULL,                 -- PII/LGPD
    email           VARCHAR(200),                          -- PII/LGPD
    telefone        VARCHAR(20),                           -- PII/LGPD: +55XXXXXXXXXXX
    logradouro      VARCHAR(200),
    numero          VARCHAR(10),
    complemento     VARCHAR(100) DEFAULT '',
    bairro          VARCHAR(100),
    cidade          VARCHAR(100),
    uf              CHAR(2),
    cep             VARCHAR(8),
    pais            CHAR(2)      NOT NULL DEFAULT 'BR',
    status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- accounts — contas corrente/pagamento (entidade master)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.accounts (
    account_id      UUID PRIMARY KEY,
    customer_id     UUID          NOT NULL REFERENCES payments.customers (customer_id),
    agencia         VARCHAR(4)    NOT NULL,                -- PII/LGPD/FIN
    conta           VARCHAR(13)   NOT NULL,                -- PII/LGPD/FIN (inclui DV)
    tier            VARCHAR(10)   NOT NULL DEFAULT 'BASIC'
                    CHECK (tier IN ('BASIC', 'PLUS', 'PREMIUM')),
    balance         NUMERIC(15,2) NOT NULL DEFAULT 0.00,   -- nunca float
    overdraft_limit NUMERIC(15,2) NOT NULL DEFAULT 0.00,   -- cheque especial
    status          VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    opened_at       DATE          NOT NULL,
    closed_at       DATE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- Saldo não fica negativo além do cheque especial (regra de domínio)
    CONSTRAINT balance_within_overdraft CHECK (balance >= -overdraft_limit)
);

CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON payments.accounts (customer_id);

-- -----------------------------------------------------------------------------
-- cards — cartões vinculados a contas (entidade master)
-- PAN completo NUNCA existe neste banco: só o mascarado (BIN6 + ****** + last4).
-- CVV/PIN/trilha magnética são PROIBIDOS mesmo em dado sintético.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.cards (
    card_id      UUID PRIMARY KEY,
    account_id   UUID          NOT NULL REFERENCES payments.accounts (account_id),
    pan_masked   VARCHAR(16)   NOT NULL,                   -- PCI: 401712******4242
    brand        VARCHAR(20)   NOT NULL
                 CHECK (brand IN ('VISA', 'MASTERCARD', 'ELO', 'HIPERCARD')),
    card_type    VARCHAR(10)   NOT NULL
                 CHECK (card_type IN ('CREDIT', 'DEBIT', 'MULTIPLE')),
    credit_limit NUMERIC(15,2),                            -- null para débito puro
    uf_emissao   CHAR(2)       NOT NULL,
    status       VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE', 'BLOCKED', 'EXPIRED', 'CANCELLED')),
    issued_at    DATE          NOT NULL,
    expires_at   DATE          NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- Guarda-corpo PCI: nunca um PAN completo (16+ dígitos contíguos) nesta coluna
    CONSTRAINT pan_is_masked CHECK (pan_masked ~ '^[0-9]{6}\*{6}[0-9]{4}$')
);

CREATE INDEX IF NOT EXISTS idx_cards_account_id ON payments.cards (account_id);

-- -----------------------------------------------------------------------------
-- merchants — estabelecimentos (entidade master)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.merchants (
    merchant_id   UUID PRIMARY KEY,
    cnpj          VARCHAR(14)  NOT NULL UNIQUE,            -- PII/LGPD: sem pontuação
    nome_fantasia VARCHAR(200) NOT NULL,
    mcc           CHAR(4)      NOT NULL,                   -- ISO 18245, preserva zero à esquerda
    uf            CHAR(2)      NOT NULL,
    logradouro    VARCHAR(200),
    numero        VARCHAR(10),
    complemento   VARCHAR(100) DEFAULT '',
    bairro        VARCHAR(100),
    cidade        VARCHAR(100),
    cep           VARCHAR(8),
    pais          CHAR(2)      NOT NULL DEFAULT 'BR',
    status        VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE'
                  CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- transactions — transações (entidade de evento; a tabela "viva" da demo de CDC:
-- INSERT/UPDATE/DELETE ao vivo na Aula 2 do Curso 3)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.transactions (
    transaction_id UUID PRIMARY KEY,
    account_id     UUID          NOT NULL REFERENCES payments.accounts (account_id),
    card_id        UUID          REFERENCES payments.cards (card_id),      -- null p/ PIX/boleto
    merchant_id    UUID          REFERENCES payments.merchants (merchant_id),
    channel        VARCHAR(10)   NOT NULL
                   CHECK (channel IN ('PIX', 'CREDIT', 'DEBIT', 'BOLETO')),
    amount         NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    mcc            CHAR(4)       NOT NULL,
    uf_transacao   CHAR(2)       NOT NULL,
    status         VARCHAR(20)   NOT NULL DEFAULT 'CREATED'
                   CHECK (status IN ('CREATED', 'AUTHORIZED', 'SETTLED', 'DECLINED', 'REFUNDED')),
    decline_reason VARCHAR(30)
                   CHECK (decline_reason IS NULL OR decline_reason IN
                          ('INSUFFICIENT_FUNDS', 'SUSPECTED_FRAUD', 'EXPIRED_CARD', 'LIMIT_EXCEEDED')),
    fraud_flag     BOOLEAN       NOT NULL DEFAULT false,   -- ground truth sintético (labs CEP/ML)
    fraud_rule     VARCHAR(30)
                   CHECK (fraud_rule IS NULL OR fraud_rule IN
                          ('RULE_VELOCITY', 'RULE_GEO_IMPOSSIBLE', 'RULE_AMOUNT_OUTLIER', 'RULE_NIGHT_OWL')),
    created_at     TIMESTAMPTZ   NOT NULL,
    authorized_at  TIMESTAMPTZ,
    settled_at     TIMESTAMPTZ,
    -- Consistência temporal: created <= authorized <= settled
    CONSTRAINT temporal_order CHECK (
        (authorized_at IS NULL OR authorized_at >= created_at)
        AND (settled_at IS NULL OR (authorized_at IS NOT NULL AND settled_at >= authorized_at))
    )
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON payments.transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_card_id ON payments.transactions (card_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON payments.transactions (created_at);

-- Verificação: as 5 tabelas existem?
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'payments' ORDER BY 1;
