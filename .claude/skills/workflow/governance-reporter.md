# Governance Reporter

## Skill: `/governance-reporter`

Generate governance reports for the workshop's Confluent Cloud data platform covering data catalog, schema inventory, PII/PHI audit, topic health, and compliance status.

## Usage

```
/governance-reporter <report-type> [options]
```

### Report Types
- `catalog` - Full data catalog (topics, schemas, ownership)
- `pii-audit` - PII/PHI field inventory across all topics
- `schema-inventory` - Schema versions, compatibility, evolution
- `topic-health` - Topic throughput, lag, DLQ status
- `access-audit` - Service accounts, ACLs, API keys
- `compliance` - Combined LGPD/ANS/SOC2 status
- `full` - Complete governance report

---

## Data Catalog Report

### Topic Inventory

```bash
# List all topics with configs
confluent kafka topic list --cluster <cluster-id> -o json | jq '[.[] | {
  name: .name,
  partitions: .partitions_count,
  replication: .replication_factor
}]'

# Topic details
confluent kafka topic describe <topic> --cluster <cluster-id> -o json
```

### Report Template

```markdown
# Data Catalog Report
**Generated**: YYYY-MM-DD HH:MM
**Environment**: prod
**Cluster**: workshop-local

## Topic Inventory

| Domain | Topic | Partitions | Retention | Schema | Owner |
|--------|-------|-----------|-----------|--------|-------|
| Saude | prod.saude.oracle-beneficiarios.membros.cdc.v1 | 12 | compact | Avro v2 | data-platform |
| Saude | prod.saude.oracle-beneficiarios.planos.cdc.v1 | 12 | compact | Avro v2 | data-platform |
| Saude | prod.saude.oracle-autorizacoes.guias.cdc.v1 | 24 | 30d | Avro v3 | data-platform |
| Saude | prod.saude.oracle-prestadores.prestadores.cdc.v1 | 12 | compact | Avro v1 | data-platform |
| Saude | prod.saude.oracle-faturamento.faturas.cdc.v1 | 24 | 30d | Avro v1 | data-platform |
| Saude | prod.saude.flink.guias.enriched.v1 | 24 | 30d | Avro v1 | data-platform |
| Saude | prod.saude.flink.glosa_alerts.detected.v1 | 6 | 365d | Avro v1 | auditoria-team |

**Total Topics**: X
**Total Partitions**: Y
**Total Schemas**: Z
```

---

## PII/PHI Audit Report

### PII/PHI Field Discovery

```bash
# Find all schemas with PII/PHI tags
for subject in $(confluent schema-registry subject list -o json | jq -r '.[]'); do
  schema=$(confluent schema-registry schema describe --subject "$subject" --version latest -o json 2>/dev/null)
  pii_count=$(echo "$schema" | jq '[.schema.fields[]? | select(.["confluent:tags"]? | contains(["PII"]))] | length' 2>/dev/null)
  phi_count=$(echo "$schema" | jq '[.schema.fields[]? | select(.["confluent:tags"]? | contains(["PHI"]))] | length' 2>/dev/null)
  if [ "$pii_count" -gt "0" ] || [ "$phi_count" -gt "0" ]; then
    echo "SENSITIVE: $subject (PII=$pii_count, PHI=$phi_count fields)"
  fi
done
```

### Report Template

```markdown
# PII/PHI Audit Report
**Generated**: YYYY-MM-DD

## PII Field Inventory

| Topic | Field | Type | Protection | LGPD Category |
|-------|-------|------|------------|---------------|
| ...membros.cdc.v1 | cpf | string | SHA-256 tokenized | Personal ID |
| ...membros.cdc.v1 | cns | string | SHA-256 tokenized | Personal ID |
| ...membros.cdc.v1 | nome | string | Masked in non-prod | Personal ID |
| ...membros.cdc.v1 | email | string | SHA-256 tokenized | Contact |
| ...membros.cdc.v1 | telefone | string | SHA-256 tokenized | Contact |
| ...guias.cdc.v1 | membro_cpf | string | SHA-256 tokenized | Personal ID |

## PHI Field Inventory

| Topic | Field | Type | Protection | ANS Category |
|-------|-------|------|------------|--------------|
| ...guias.cdc.v1 | cid_10 | string | Strict access control | Health Data |
| ...guias.cdc.v1 | procedimento | string | Strict access control | Health Data |
| ...laudos.cdc.v1 | diagnostico | string | Strict access control | Health Data |
| ...laudos.cdc.v1 | laudo_texto | string | Strict access control | Health Data |

## Protection Status

| Protection Method | Fields | Topics |
|-------------------|--------|--------|
| SHA-256 Tokenization | X | Y |
| Field Masking | X | Y |
| Column Exclusion | X | Y |
| Strict Access Control (PHI) | X | Y |
| Unprotected PII | X | Y |  <- MUST be 0
| Unprotected PHI | X | Y |  <- MUST be 0

## Recommendations
- [List any unprotected PII/PHI that needs immediate action]
```

---

## Schema Inventory Report

```bash
# List all subjects with versions
for subject in $(confluent schema-registry subject list -o json | jq -r '.[]'); do
  versions=$(confluent schema-registry schema list --subject "$subject" -o json | jq length)
  compat=$(confluent schema-registry subject describe "$subject" -o json | jq -r '.compatibilityLevel // "BACKWARD"')
  echo "$subject | versions=$versions | compatibility=$compat"
done
```

### Report Template

```markdown
# Schema Inventory
**Generated**: YYYY-MM-DD

| Subject | Format | Versions | Compatibility | Last Updated |
|---------|--------|----------|---------------|-------------|
| ...guias.cdc.v1-value | Avro | 3 | BACKWARD | 2026-02-10 |
| ...membros.cdc.v1-value | Avro | 2 | BACKWARD | 2026-02-05 |
| ...prestadores.cdc.v1-value | Avro | 1 | BACKWARD | 2026-01-20 |

## Schema Evolution History
- v3 (2026-02-10): Added `coparticipacao` field (nullable)
- v2 (2026-01-15): Added `tipo_guia` field with default "CONSULTA"
- v1 (2026-01-01): Initial schema
```

---

## Topic Health Report

### Health Check Commands

```bash
# Consumer group lag
confluent kafka consumer-group lag describe <group> --cluster <cluster-id>

# DLQ message count
confluent kafka topic consume <dlq-topic> --cluster <cluster-id> --from-beginning --count

# Connector status
confluent connect cluster list --cluster <cluster-id> -o json
```

### Report Template

```markdown
# Topic Health Report
**Generated**: YYYY-MM-DD HH:MM

## CDC Pipeline Health

| Connector | Status | Tasks | Lag (s) | DLQ Count |
|-----------|--------|-------|---------|-----------|
| oracle-cdc-beneficiarios | RUNNING | 1/1 | 12 | 0 |
| oracle-cdc-autorizacoes | RUNNING | 1/1 | 8 | 0 |
| oracle-cdc-prestadores | RUNNING | 1/1 | 5 | 0 |
| oracle-cdc-faturamento | RUNNING | 1/1 | 15 | 0 |

## Consumer Group Lag

| Group | Topic | Lag (records) | Status |
|-------|-------|---------------|--------|
| flink-enrichment | guias.cdc | 245 | OK |
| s3-sink | guias.enriched | 1,230 | OK |
| snowflake-ingest | guias.enriched | 890 | OK |
| glosa-detection | guias.cdc | 12 | OK |

## Flink Jobs

| Job | Status | CFUs | State Size |
|-----|--------|------|------------|
| guia-enrichment | RUNNING | 10 | 2.1 GB |
| glosa-detection | RUNNING | 8 | 1.5 GB |
| hourly-aggregation | RUNNING | 4 | 0.8 GB |
| pii-phi-tokenization | RUNNING | 4 | 0.3 GB |

## Alerts
- [List any active alerts or issues]
```

---

## Access Audit Report

```bash
# Service accounts
confluent iam service-account list -o json

# ACLs per service account
confluent kafka acl list --service-account <sa-id> --cluster <cluster-id>

# API keys
confluent api-key list --service-account <sa-id>
```

### Report Template

```markdown
# Access Audit Report
**Generated**: YYYY-MM-DD

## Service Accounts

| SA ID | Name | API Keys | Last Activity | ACL Count |
|-------|------|----------|---------------|-----------|
| sa-abc123 | sa-workshop-oracle-cdc-beneficiarios | 2 | 2026-02-12 | 8 |
| sa-def456 | sa-workshop-flink-enrichment | 1 | 2026-02-12 | 6 |
| sa-ghi789 | sa-workshop-s3-sink | 1 | 2026-02-12 | 4 |
| sa-jkl012 | sa-workshop-snowflake-ingest | 1 | 2026-02-12 | 4 |

## API Key Rotation Status

| API Key | Service Account | Created | Age (days) | Status |
|---------|----------------|---------|------------|--------|
| KEY-001 | sa-workshop-oracle-cdc-beneficiarios | 2026-01-01 | 42 | OK (<90d) |
| KEY-002 | sa-workshop-flink-enrichment | 2025-11-15 | 89 | WARN (near 90d) |

## Findings
- [List any excessive permissions, stale accounts, or overdue rotations]
```

---

## Report Generation Schedule

| Report | Frequency | Audience | Automation |
|--------|-----------|----------|------------|
| Topic Health | Daily | Engineering | Automated |
| PII/PHI Audit | Weekly | Security + Compliance | Automated |
| Schema Inventory | Weekly | Data Platform | Automated |
| Access Audit | Monthly | Security | Semi-automated |
| Compliance | Quarterly | Management + Audit | Manual review |
| Full Governance | Quarterly | All stakeholders | Manual review |

---

## Best Practices

1. **Automate daily health reports** with scheduled scripts
2. **Alert on any unprotected PII/PHI** discovered in new schemas
3. **Track schema evolution** to prevent breaking changes
4. **Review API key ages** weekly and rotate proactively
5. **Store reports in Git** for historical tracking
6. **Present quarterly** governance status to leadership

---

**Version**: 1.0
**Last Updated**: 2026-02-12
