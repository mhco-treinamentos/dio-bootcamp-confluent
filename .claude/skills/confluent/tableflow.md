# Tableflow (Kafka → Iceberg)

## Skill: `/confluent-tableflow`

Habilita **Tableflow** em topics do Confluent Cloud para materializá-los como tabelas **Apache Iceberg** — a camada lakehouse do bootcamp (módulo 5). Abordagem *shift-left*: o Flink enriquece o stream **antes** de materializar; o analítico consome tabela pronta, sem pipeline de cópia.

> Tableflow é **cloud-only** — não existe no fallback local.

## Usage

```
/confluent-tableflow <action>
```

### Actions
- `enable <topic>` — Habilitar Tableflow no topic
- `verify` — Verificar materialização da tabela Iceberg
- `schema` — Requisitos e evolução de schema
- `catalog` — Integração com catálogos externos (Glue, Snowflake Open Catalog, Unity)
- `disable <topic>` — Desabilitar (teardown)

---

## Pré-requisitos (checklist antes de habilitar)

- [ ] **O topic TEM schema no Schema Registry** (Avro/Protobuf/JSON Schema no subject `<topic>-value`) — Tableflow deriva a tabela Iceberg do schema; topic schemaless não materializa
- [ ] Compatibilidade **BACKWARD** no subject (padrão do bootcamp) — evolução compatível vira evolução de schema Iceberg
- [ ] Cluster e environment selecionados (`confluent environment use` / `confluent kafka cluster use`)
- [ ] Storage: **Confluent-managed** (mais simples, para labs) ou **BYOS** (seu bucket S3 + provider integration)

No desafio final, o topic materializado é a saída enriquecida do Flink:
`dev.payments.flink.transactions.enriched.v1` (já com schema Avro registrado pelo statement).

---

## Habilitar Tableflow em um topic

### Via CLI

```bash
# Storage gerenciado pela Confluent (labs)
confluent tableflow topic create dev.payments.flink.transactions.enriched.v1 \
  --cluster <cluster-id> \
  --storage-type MANAGED

# BYOS (bucket próprio via provider integration — módulo 5, parte 2)
confluent tableflow topic create dev.payments.flink.transactions.enriched.v1 \
  --cluster <cluster-id> \
  --storage-type BYOS \
  --provider-integration <pi-id> \
  --bucket-name bootcamp-payments-lake

# Estado da materialização
confluent tableflow topic list --cluster <cluster-id>
confluent tableflow topic describe dev.payments.flink.transactions.enriched.v1 --cluster <cluster-id>
```

> Flags variam com a versão da CLI — confirme com `confluent tableflow topic create --help`. Na UI: **Topics → (topic) → Enable Tableflow**.

### Verificar a tabela materializada

```bash
# 1. Status SYNCING/RUNNING no describe acima
# 2. Endpoint do catálogo REST Iceberg do environment (para engines externas)
confluent tableflow catalog-integration list --cluster <cluster-id>
```

Consulta de verificação a partir de qualquer engine com suporte a Iceberg REST catalog (Spark, Trino, DuckDB):

```sql
-- ex.: Spark SQL apontando para o Iceberg REST catalog do Tableflow
SELECT COUNT(*) FROM `<cluster-id>`.`dev.payments.flink.transactions.enriched.v1`;
```

Ou, mais simples nos labs, via Flink SQL no próprio Confluent Cloud (a tabela é o topic):

```sql
SELECT COUNT(*) FROM `dev.payments.flink.transactions.enriched.v1`;
```

---

## Schema: conversão de tipos e evolução

### Conversão Avro → Iceberg (casos que aparecem nos labs)

| Avro (Schema Registry) | Iceberg |
|------------------------|---------|
| `string` | `string` |
| `int` / `long` | `int` / `long` |
| `bytes` + `logicalType: decimal(p,s)` | `decimal(p,s)` — **valores BRL chegam corretos** |
| `long` + `logicalType: timestamp-millis` | `timestamp` |
| `enum` | `string` |
| `union ["null", T]` com default | coluna anulável de tipo T |
| `record` aninhado | `struct` |

### Evolução

- Mudanças **BACKWARD** no subject (adicionar campo opcional com default) viram *add column* na tabela Iceberg — consumers analíticos não quebram
- **Type widening** (`int` → `long`) é suportado pelo Iceberg; renomear/remover campo obrigatório é breaking — bloqueado pelo SR se a compatibilidade estiver correta
- Lab do módulo 5: adicionar `fraud_score` (nullable) ao schema, produzir, e observar a coluna nova aparecer na tabela

---

## Integração com catálogos

Tableflow publica as tabelas em catálogos externos para consumo analítico:

| Catálogo | Uso no bootcamp |
|----------|-----------------|
| **AWS Glue Data Catalog** | Athena/EMR/Redshift Spectrum consultam a tabela |
| **Snowflake Open Catalog** (Polaris) | Snowflake lê Iceberg externo sem cópia |
| **Databricks Unity Catalog** | Consumo via Databricks SQL |
| Iceberg REST (embutido) | Qualquer engine compatível — default dos labs |

```bash
# Exemplo: integração com AWS Glue
confluent tableflow catalog-integration create glue-bootcamp \
  --cluster <cluster-id> \
  --type aws-glue \
  --provider-integration <pi-id>

confluent tableflow catalog-integration list --cluster <cluster-id>
```

> A criação da *provider integration* (role AWS com trust policy para a Confluent) é feita uma vez por conta — passos na UI em **Environment → Integrations**.

---

## Teardown (créditos!)

```bash
# Desabilitar Tableflow do topic ao final do lab
confluent tableflow topic delete dev.payments.flink.transactions.enriched.v1 \
  --cluster <cluster-id> --force

# BYOS: conferir/limpar objetos órfãos no bucket
aws s3 ls s3://bootcamp-payments-lake/ --recursive | head
```

Entra no `teardown.sh` do lab do módulo 5 (ver [cloud-setup.md](cloud-setup.md)).

---

## Anti-patterns

1. **Habilitar Tableflow em topic sem schema** — falha na materialização; registrar o Avro primeiro ([schema-registry.md](schema-registry.md))
2. **Materializar o topic CDC cru** — materialize a saída enriquecida do Flink (shift-left); o CDC cru tem envelope Debezium e PII sem tratamento
3. **`float`/`double` para valores monetários** — usar `decimal` no Avro; a conversão preserva a precisão no Iceberg
4. **Esquecer o teardown** — Tableflow ativo continua materializando e cobrando storage/processamento
5. **Compatibilidade NONE no subject** — breaking change quebra a tabela para todos os consumers analíticos

---

**Última atualização**: 2026-07-11
