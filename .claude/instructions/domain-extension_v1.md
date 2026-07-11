# Instruction: estender o domínio (nova entidade / tabela / sub-domínio)

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para adicionar uma nova entidade ao domínio (ex.: `Estorno`, `Disputa`, `PIX`) — cobrindo schema Avro, gerador Python, Oracle CDC, Flink SQL e sink Iceberg.**

## Quando usar
- Você quer adicionar uma nova tabela Oracle ao pipeline CDC (ex.: `PAYMENTS.ESTORNOS`)
- Ou um novo evento sintético que ainda não tem gerador (ex.: `EventoFraude`)
- Ou um sub-domínio inteiro (ex.: `pix`, `boletos`)

## Pré-requisitos
- Estrutura `data-streaming-tour-2026/` existente
- Stack base subindo (ver [kafka-setup_v1.md](kafka-setup_v1.md))
- Para CDC: Oracle XE rodando, supplemental logging habilitado

---

## Phase 1: RESEARCH

### 1.1 — Requisitos da nova entidade
- Qual o nome da entidade? (Singular, PascalCase: `Estorno`)
- Sub-domínio? (Define o namespace Avro: `com.workshop.payments.estornos`)
- Campos? Tipos? Quais são PII/PCI?
- Relacionamentos (FKs)? Ex.: `Estorno.transaction_id → Transacao.transaction_id`
- Distribuições estatísticas? (Ex.: estornos são ~2% das transações; razão top: "FRAUDE" 40%, "CLIENTE_NAO_RECONHECEU" 30%, "DUPLICIDADE" 20%, outros 10%)
- Vai entrar no pipeline CDC (tabela Oracle) ou só no pipeline sintético direto-para-Kafka?

### 1.2 — Revisar o existente
- Inspecionar `shared/generators/payments/` para entender o pattern
- Inspecionar `shared/schemas/payments/*.avsc` para namespace/tags
- Ver `BaseGenerator` em `shared/generators/base.py`

### 1.3 — Consultar references
- [references/data-modeling.md](../references/data-modeling.md) — relacionamentos, evolução
- [references/brazilian-data-formats.md](../references/brazilian-data-formats.md) — formatos específicos
- [references/oracle-cdc-patterns.md](../references/oracle-cdc-patterns.md) — se for tabela Oracle

---

## Phase 2: PLAN

### 2.1 — Schema Avro

```json
{
  "type": "record",
  "name": "Estorno",
  "namespace": "com.workshop.payments.estornos",
  "doc": "Estorno (chargeback ou reembolso) de uma transação aprovada",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "estorno_id", "type": "string"},
    {"name": "transaction_id", "type": "string", "doc": "FK → Transacao.transaction_id"},
    {
      "name": "valor_estornado",
      "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}
    },
    {
      "name": "motivo",
      "type": {"type": "enum", "name": "MotivoEstorno", "symbols": ["FRAUDE", "CLIENTE_NAO_RECONHECEU", "DUPLICIDADE", "ERRO_VALOR", "OUTRO"]}
    },
    {"name": "iniciado_por", "type": ["null", "string"], "default": null, "doc": "operator id ou null se automático"}
  ]
}
```

### 2.2 — Tabela Oracle (se aplicável)

```sql
CREATE TABLE PAYMENTS.ESTORNOS (
  estorno_id         VARCHAR2(36)  PRIMARY KEY,
  transaction_id     VARCHAR2(36)  NOT NULL,
  valor_estornado    NUMBER(15,2)  NOT NULL,
  motivo             VARCHAR2(30)  NOT NULL,
  iniciado_por       VARCHAR2(36),
  created_at         TIMESTAMP(3) WITH TIME ZONE DEFAULT SYSTIMESTAMP,
  CONSTRAINT fk_estorno_trx FOREIGN KEY (transaction_id) REFERENCES PAYMENTS.TRANSACOES(transaction_id)
);

ALTER TABLE PAYMENTS.ESTORNOS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### 2.3 — Topics & subjects

```
dev.payments.oracle-pagamentos.estornos.cdc.v1
└── subject: dev.payments.oracle-pagamentos.estornos.cdc.v1-value
```

### 2.4 — Decisões a confirmar com usuário
- Particionamento (partition key): `transaction_id` ou `account_id`?
- Compatibility mode no Schema Registry: **BACKWARD** (default)
- Vai existir no Iceberg também? Se sim, particionar por `DAYS(event_time)` + `bucket(transaction_id, 8)`?

---

## Phase 3: ACT

### 3.1 — Criar arquivos

```
data-streaming-tour-2026/
├── shared/
│   ├── schemas/payments/estorno.avsc          # novo
│   └── generators/payments/
│       ├── __init__.py                        # adicionar export
│       └── estornos.py                        # novo
└── tests/generators/test_estornos.py          # novo
```

### 3.2 — Gerador Python
Seguir [new-generator_v1.md](new-generator_v1.md). Diferença-chave: o gerador de `Estorno` precisa receber um pool de `transaction_id` (de transações reais geradas previamente) para manter integridade referencial:

```python
class EstornoGenerator(BaseGenerator):
    def __init__(self, transaction_pool: list[str], config: EstornoGeneratorConfig | None = None) -> None:
        self.transaction_pool = transaction_pool
        # ...
```

### 3.3 — Oracle CDC (se tabela Oracle)
1. Criar a tabela com `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`
2. Atualizar a config do connector Debezium para incluir `PAYMENTS.ESTORNOS` em `table.include.list`
3. Reload do connector: `curl -X POST http://localhost:8083/connectors/oracle-payments-cdc/restart`
4. Verificar snapshot: topic CDC vai receber eventos `op=r` iniciais

### 3.4 — Flink SQL — table sobre o topic CDC

```sql
CREATE TABLE estornos_cdc (
  estorno_id STRING,
  transaction_id STRING,
  valor_estornado DECIMAL(15, 2),
  motivo STRING,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
  PRIMARY KEY (estorno_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dev.payments.oracle-pagamentos.estornos.cdc.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081',
  'value.fields-include' = 'EXCEPT_KEY',
  'scan.startup.mode' = 'earliest-offset'
);
```

### 3.5 — Iceberg sink (lakehouse)

```sql
CREATE TABLE iceberg_estornos (
  estorno_id STRING,
  transaction_id STRING,
  valor_estornado DECIMAL(15, 2),
  motivo STRING,
  event_time TIMESTAMP_LTZ(3),
  PRIMARY KEY (estorno_id) NOT ENFORCED
) PARTITIONED BY (DAYS(event_time))
WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'nessie',
  'catalog-database' = 'payments',
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
);

INSERT INTO iceberg_estornos
SELECT estorno_id, transaction_id, valor_estornado, motivo, event_time
FROM estornos_cdc;
```

### 3.6 — Atualizar docs
- [.claude/rules.md](../rules.md) — adicionar `Estornos` na tabela "Domain Rules — Payments" se a entidade trouxer regras novas
- README da edição (`data-streaming-tour-2026/README.md`)
- CHANGELOG

---

## Phase 4: VALIDATE

### 4.1 — Schema (hook PII bloqueia se faltar tag)
```bash
# Save em shared/schemas/payments/estorno.avsc dispara validate-avro-pii.sh
# Esperado: sem violações (Estorno não tem campos PII além do FK)
```

### 4.2 — Testes
```bash
pytest data-streaming-tour-2026/tests/generators/test_estornos.py -v
```

### 4.3 — CDC end-to-end
```bash
# 1) Insert no Oracle
echo "INSERT INTO PAYMENTS.ESTORNOS VALUES (...);" | sqlplus payments/pass@//localhost:1521/XEPDB1

# 2) Consumir o topic e verificar envelope Debezium
kcat -C -b localhost:9092 -t dev.payments.oracle-pagamentos.estornos.cdc.v1 -o end -c 1 | jq .
# Esperado: envelope com op=c, after.estorno_id presente
```

### 4.4 — Iceberg (se aplicável)
```sql
-- No Trino
SELECT COUNT(*) FROM iceberg.payments.estornos;
-- Deve subir conforme novos eventos CDC chegam
```

### 4.5 — Schema compatibility
```bash
# Validar BACKWARD compat (deve passar — nova entidade não muda schemas existentes)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @<(jq -n --argfile s data-streaming-tour-2026/shared/schemas/payments/estorno.avsc '{schema: ($s | tostring)}') \
  http://localhost:8081/compatibility/subjects/dev.payments.oracle-pagamentos.estornos.cdc.v1-value/versions/latest
# Esperado: {"is_compatible": true}
```

---

## Checklist final

- [ ] Schema Avro criado com namespace + tags PII/PCI corretas
- [ ] Tabela Oracle criada com supplemental logging (se aplicável)
- [ ] Gerador Python implementado e testado (≥80% coverage)
- [ ] Topic criado com particionamento adequado
- [ ] Connector Debezium atualizado e RUNNING
- [ ] Flink SQL table criada e snapshot recebido
- [ ] Iceberg sink criado e populando (se aplicável)
- [ ] Compatibility check passou (BACKWARD)
- [ ] Docs atualizadas (rules.md, README, CHANGELOG)

---

## Exemplo de uso

```
"Siga domain-extension para adicionar a entidade Disputa
(disputa_id, transaction_id, status, opened_at, resolved_at)
ao domínio payments, com tabela Oracle + CDC + sink Iceberg"
```
