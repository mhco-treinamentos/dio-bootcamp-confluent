# Instruction: estender o domínio (nova entidade / tabela / sub-domínio)

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para adicionar uma nova entidade ao domínio (ex.: `Refund`, `Dispute`, `PixTransfer`) — cobrindo schema Avro, gerador Python, CDC via connector gerenciado, Flink SQL e materialização Tableflow.**

## Quando usar
- Você quer adicionar uma nova tabela Postgres ao pipeline CDC (ex.: `payments.refunds`)
- Ou um novo evento sintético que ainda não tem gerador (ex.: `FraudEvent`)
- Ou um sub-domínio inteiro (ex.: `pix`, `boletos`)

## Pré-requisitos
- Estrutura `repo/` existente (`shared/`, `labs/`, `desafio-final/`)
- Ambiente Confluent Cloud configurado (ver [cloud-setup_v1.md](cloud-setup_v1.md))
- Para CDC: Postgres fonte rodando (`wal_level=logical`) e connector Debezium gerenciado ativo

---

## Phase 1: RESEARCH

### 1.1 — Requisitos da nova entidade
- Qual o nome da entidade? (Singular, PascalCase: `Refund`)
- Sub-domínio? (Define o namespace Avro: `com.bootcamp.payments.refunds`)
- Campos? Tipos? Quais são PII/PCI?
- Relacionamentos (FKs)? Ex.: `Refund.transaction_id → Transaction.transaction_id`
- Distribuições estatísticas? (Ex.: estornos são ~2% das transações; razão top: "FRAUD" 40%, "NOT_RECOGNIZED" 30%, "DUPLICATE" 20%, outros 10%)
- Vai entrar no pipeline CDC (tabela Postgres) ou só no pipeline sintético direto-para-Kafka?

### 1.2 — Revisar o existente
- Inspecionar `repo/shared/generators/payments/` para entender o pattern
- Inspecionar `repo/shared/schemas/payments/*.avsc` para namespace/tags
- Ver `BaseGenerator` em `repo/shared/generators/base.py`

### 1.3 — Consultar references
- [references/data-modeling.md](../references/data-modeling.md) — relacionamentos, evolução
- [references/brazilian-data-formats.md](../references/brazilian-data-formats.md) — formatos específicos
- [references/cdc-connect-patterns.md](../references/cdc-connect-patterns.md) — se for tabela CDC

---

## Phase 2: PLAN

### 2.1 — Schema Avro

```json
{
  "type": "record",
  "name": "Refund",
  "namespace": "com.bootcamp.payments.refunds",
  "doc": "Refund (chargeback ou reembolso) de uma transação aprovada",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "refund_id", "type": "string"},
    {"name": "transaction_id", "type": "string", "doc": "FK → Transaction.transaction_id"},
    {
      "name": "refund_amount",
      "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}
    },
    {
      "name": "reason",
      "type": {"type": "enum", "name": "RefundReason", "symbols": ["FRAUD", "NOT_RECOGNIZED", "DUPLICATE", "AMOUNT_ERROR", "OTHER"]}
    },
    {"name": "initiated_by", "type": ["null", "string"], "default": null, "doc": "operator id ou null se automático"}
  ]
}
```

### 2.2 — Tabela Postgres (se aplicável)

```sql
CREATE TABLE payments.refunds (
  refund_id       UUID PRIMARY KEY,
  transaction_id  UUID NOT NULL REFERENCES payments.transactions (transaction_id),
  refund_amount   NUMERIC(15, 2) NOT NULL,
  reason          VARCHAR(30) NOT NULL,
  initiated_by    UUID,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Necessário para o Debezium capturar before-image em UPDATE/DELETE
ALTER TABLE payments.refunds REPLICA IDENTITY FULL;

-- Se a publication não é FOR ALL TABLES:
ALTER PUBLICATION payments_cdc ADD TABLE payments.refunds;
```

### 2.3 — Topics & subjects

```
dev.payments.pg-payments.refunds.cdc.v1
└── subject: dev.payments.pg-payments.refunds.cdc.v1-value
```

### 2.4 — Decisões a confirmar com usuário
- Particionamento (partition key): `transaction_id` ou `account_id`?
- Compatibility mode no Schema Registry: **BACKWARD** (default do bootcamp)
- Vai para o lakehouse? Se sim, habilitar Tableflow no topic enriquecido (módulo 5)

---

## Phase 3: ACT

### 3.1 — Criar arquivos

```
repo/
├── shared/
│   ├── schemas/payments/refund.avsc           # novo
│   └── generators/payments/
│       ├── __init__.py                        # adicionar export
│       └── refunds.py                         # novo
└── tests/generators/test_refunds.py           # novo
```

### 3.2 — Gerador Python
Seguir [new-generator_v1.md](new-generator_v1.md). Diferença-chave: o gerador de `Refund` precisa receber um pool de `transaction_id` (de transações reais geradas previamente) para manter integridade referencial:

```python
class RefundGenerator(BaseGenerator):
    def __init__(self, transaction_pool: list[str], config: RefundGeneratorConfig | None = None) -> None:
        self.transaction_pool = transaction_pool
        # ...
```

### 3.3 — CDC (se tabela Postgres)
1. Criar a tabela com `REPLICA IDENTITY FULL` e adicioná-la à publication
2. Atualizar a config do connector gerenciado para incluir `payments.refunds` em `table.include.list`:

```bash
confluent connect cluster update <connector-id> --config-file connectors/pg-payments-cdc.json
```

3. Verificar status e snapshot:

```bash
confluent connect cluster describe <connector-id>
# Topic CDC recebe eventos op=r (snapshot) e depois op=c/u/d
```

### 3.4 — Flink SQL — statement sobre o topic CDC

No Confluent Cloud, o topic com schema já aparece como tabela no catálogo do Flink — não há DDL de conector:

```sql
-- Workspace Flink (compute pool do environment)
SELECT
  refund_id,
  transaction_id,
  refund_amount,
  reason,
  `$rowtime` AS event_time
FROM `dev.payments.pg-payments.refunds.cdc.v1`
WHERE reason = 'FRAUD';
```

Para derivar um topic enriquecido (ex.: join com transações):

```sql
CREATE TABLE `dev.payments.flink.refunds.enriched.v1` AS
SELECT
  r.refund_id,
  r.transaction_id,
  r.refund_amount,
  r.reason,
  t.account_id,
  t.merchant_mcc
FROM `dev.payments.pg-payments.refunds.cdc.v1` r
JOIN `dev.payments.pg-payments.transactions.cdc.v1` FOR SYSTEM_TIME AS OF r.`$rowtime` AS t
  ON r.transaction_id = t.transaction_id;
```

### 3.5 — Tableflow (lakehouse, se aplicável)

```bash
# Habilitar materialização Iceberg no topic enriquecido
confluent tableflow topic enable dev.payments.flink.refunds.enriched.v1 \
  --cluster <cluster-id> --storage-type MANAGED --table-formats ICEBERG
```

Consulta via engine conectada ao catálogo (Glue / Snowflake Open Catalog / Unity) — ver [skills/confluent/tableflow.md](../skills/confluent/tableflow.md).

### 3.6 — Atualizar docs
- [.claude/rules.md](../rules.md) — adicionar `Refunds` na tabela "Domain Rules — Payments" se a entidade trouxer regras novas
- README do lab afetado (`repo/labs/modulo-0X-*/README.md`)
- CHANGELOG

---

## Phase 4: VALIDATE

### 4.1 — Schema (hook PII bloqueia se faltar tag)
```bash
# Save em repo/shared/schemas/payments/refund.avsc dispara validate-avro-pii.sh
# Esperado: sem violações (Refund não tem campos PII além do FK)
```

### 4.2 — Testes
```bash
pytest repo/tests/generators/test_refunds.py -v
```

### 4.3 — CDC end-to-end
```bash
# 1) Insert no Postgres fonte
psql "$PG_SOURCE_DSN" -c "INSERT INTO payments.refunds VALUES (...);"

# 2) Consumir o topic e verificar envelope Debezium
confluent kafka topic consume dev.payments.pg-payments.refunds.cdc.v1 \
  --from-beginning --value-format avro | jq .
# Esperado: envelope com op=c, after.refund_id presente
```

### 4.4 — Tableflow (se aplicável)
```bash
confluent tableflow topic describe dev.payments.flink.refunds.enriched.v1 --cluster <cluster-id>
# Esperado: status ACTIVE; contagem de linhas sobe conforme eventos chegam (query via engine do catálogo)
```

### 4.5 — Schema compatibility
```bash
# Validar BACKWARD compat (deve passar — nova entidade não muda schemas existentes)
confluent schema-registry schema create \
  --subject dev.payments.pg-payments.refunds.cdc.v1-value \
  --schema repo/shared/schemas/payments/refund.avsc --type avro --dry-run
# Esperado: sem erro de compatibilidade
```

---

## Checklist final

- [ ] Schema Avro criado com namespace + tags PII/PCI corretas
- [ ] Tabela Postgres criada com REPLICA IDENTITY FULL e na publication (se aplicável)
- [ ] Gerador Python implementado e testado (≥80% coverage)
- [ ] Topic criado com particionamento adequado (RF=3 no cloud)
- [ ] Connector gerenciado atualizado e RUNNING
- [ ] Statement Flink validado e topic derivado populando
- [ ] Tableflow habilitado e materializando (se aplicável)
- [ ] Compatibility check passou (BACKWARD)
- [ ] Docs atualizadas (rules.md, README do lab, CHANGELOG)
- [ ] Teardown do que for descartável (statements de teste, topics temporários)

---

## Exemplo de uso

```
"Siga domain-extension para adicionar a entidade Dispute
(dispute_id, transaction_id, status, opened_at, resolved_at)
ao domínio payments, com tabela Postgres + CDC + Tableflow"
```
