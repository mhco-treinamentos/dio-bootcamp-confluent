# Governance Reporter

## Skill: `/governance-reporter`

Gera relatórios de governança da plataforma do bootcamp no Confluent Cloud: catálogo de dados, inventário de schemas, auditoria PII/PCI (LGPD), saúde de topics/conectores e acesso. Usa **Stream Governance** (Stream Catalog + Stream Lineage) como fonte — conteúdo da **Live 3**.

## Usage

```
/governance-reporter <report-type> [options]
```

### Report Types
- `catalog` — Catálogo (topics, schemas, ownership)
- `pii-audit` — Inventário de campos PII/PCI (LGPD)
- `schema-inventory` — Versões, compatibilidade, evolução
- `topic-health` — Throughput, lag, DLQ
- `access-audit` — Service accounts, ACLs, API keys
- `full` — Relatório completo

---

## Catálogo de dados

```bash
confluent kafka topic list -o json | jq '[.[] | {name: .name}]'
confluent kafka topic describe <topic>
```

### Template

```markdown
# Data Catalog Report
**Gerado**: YYYY-MM-DD HH:MM
**Environment**: bootcamp-dev
**Cluster**: bootcamp-cluster

## Topic Inventory

| Domain | Topic | Partições | Retenção | Schema | Owner |
|--------|-------|-----------|----------|--------|-------|
| Payments | dev.payments.pg-payments.accounts.cdc.v1 | 6 | 7d | Avro v2 | bootcamp-payments |
| Payments | dev.payments.pg-payments.cards.cdc.v1 | 6 | 7d | Avro v1 | bootcamp-payments |
| Payments | dev.payments.pg-payments.transactions.cdc.v1 | 6 | 7d | Avro v3 | bootcamp-payments |
| Payments | dev.payments.flink.transactions.enriched.v1 | 6 | 7d | Avro v1 | bootcamp-payments |
| Payments | dev.payments.flink.fraud.detected.v1 | 3 | 30d | Avro v1 | bootcamp-payments |
```

> Na UI, o mesmo inventário (com tags e lineage) está em **Environment → Stream Governance → Stream Catalog**.

---

## Auditoria PII/PCI (LGPD)

```bash
# Subjects com campos taggeados como sensíveis
for subject in $(confluent schema-registry subject list -o json | jq -r '.[]'); do
  schema=$(confluent schema-registry schema describe --subject "$subject" --version latest 2>/dev/null)
  pii=$(echo "$schema" | grep -cE '"PII"|"LGPD"' || true)
  pci=$(echo "$schema" | grep -c '"PCI"' || true)
  { [ "$pii" -gt 0 ] || [ "$pci" -gt 0 ]; } && echo "SENSITIVE: $subject (PII=$pii, PCI=$pci)"
done
```

### Template

```markdown
# PII/PCI Audit Report — LGPD
**Gerado**: YYYY-MM-DD

## Inventário PII

| Topic | Campo | Tipo | Proteção | Categoria LGPD |
|-------|-------|------|----------|----------------|
| ...accounts.cdc.v1 | cpf | string | SHA-256 (Flink tokenization) | Identificação |
| ...accounts.cdc.v1 | email | string | MaskField no conector | Contato |
| ...transactions.cdc.v1 | cpf | string | SHA-256 (Flink tokenization) | Identificação |

## Inventário PCI

| Topic | Campo | Proteção |
|-------|-------|----------|
| ...cards.cdc.v1 | card_pan_masked | BIN+last4 sempre; nunca cleartext |

## Status

| Método | Campos | Topics |
|--------|--------|--------|
| Tokenização SHA-256 | X | Y |
| Mascaramento (SMT) | X | Y |
| PII sem proteção | X | Y |  <- DEVE ser 0
| PCI sem proteção | X | Y |  <- DEVE ser 0
```

---

## Inventário de schemas

```bash
for subject in $(confluent schema-registry subject list -o json | jq -r '.[]'); do
  versions=$(confluent schema-registry schema list --subject-prefix "$subject" -o json | jq length)
  echo "$subject | versions=$versions"
done
```

```markdown
# Schema Inventory
| Subject | Formato | Versões | Compatibilidade |
|---------|---------|---------|-----------------|
| ...transactions.cdc.v1-value | Avro | 3 | BACKWARD |
| ...accounts.cdc.v1-value | Avro | 2 | BACKWARD |

## Evolução
- v3: adiciona `fraud_score` (nullable, default null)
- v2: adiciona `mcc` (nullable)
- v1: schema inicial
```

---

## Saúde de topics e pipeline

```bash
# Lag por consumer group
confluent kafka consumer group lag describe <group>

# Conectores
confluent connect cluster list

# Statements Flink
confluent flink statement list

# Consumer lag via Metrics API (automação — módulo 7)
curl -s https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  -u "$CLOUD_API_KEY:$CLOUD_API_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{"aggregations":[{"metric":"io.confluent.kafka.server/consumer_lag_offsets"}],
       "filter":{"field":"resource.kafka.id","op":"EQ","value":"<cluster-id>"},
       "group_by":["metric.consumer_group_id"],
       "granularity":"PT1M","intervals":["PT1H/now"]}' | jq
```

```markdown
# Topic Health Report

## Pipeline CDC
| Conector | Status | Tasks | DLQ |
|----------|--------|-------|-----|
| pg-payments-cdc-source | RUNNING | 1/1 | 0 |

## Consumer Lag
| Group | Topic | Lag | Status |
|-------|-------|-----|--------|
| flink (tx-enrichment) | transactions.cdc | 120 | OK |

## Flink Statements
| Statement | Status | Compute pool |
|-----------|--------|--------------|
| tx-enrichment | RUNNING | bootcamp-flink |
| fraud-velocity | RUNNING | bootcamp-flink |
```

---

## Auditoria de acesso

```bash
confluent iam service-account list -o json
confluent kafka acl list --service-account <sa-id>
confluent api-key list --service-account <sa-id>
```

```markdown
# Access Audit
| SA | Nome | API Keys | ACLs |
|----|------|----------|------|
| sa-xxxxx | sa-lab-modulo-03 | 1 | write dev.payments.* (prefix) |
| sa-yyyyy | sa-lab-modulo-04 | 1 | read dev.payments.* (prefix) |

## Findings
- [permissões excessivas, SAs órfãos de labs encerrados, keys antigas]
```

---

## Frequência (contexto bootcamp)

| Relatório | Quando | Uso |
|-----------|--------|-----|
| Topic health | A cada lab | Verificação de fim de lab |
| PII/PCI audit | Live 3 + desafio final | Demonstração LGPD |
| Schema inventory | Módulo 2 e mudanças | Evolução BACKWARD |
| Access audit | Fim de cada módulo | Limpar SAs/keys órfãs (teardown) |

---

## Best practices

1. **Stream Catalog/Lineage como fonte** — o relatório é um snapshot, a UI é viva
2. **Alertar em PII/PCI sem proteção** — deve ser sempre 0
3. **Auditar acesso no teardown** — SA de lab encerrado é risco e ruído
4. **Versionar relatórios no Git** do lab correspondente
5. **Usar a auditoria PII como material da Live 3** — o relatório é o artefato da aula

---

**Última atualização**: 2026-07-11
