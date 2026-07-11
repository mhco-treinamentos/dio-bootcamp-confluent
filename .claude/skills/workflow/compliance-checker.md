# Compliance Checker

## Skill: `/compliance-checker`

Validate LGPD, ANS (Agência Nacional de Saúde Suplementar), and SOC2 compliance for the workshop's Confluent Cloud data platform. Checks PII/PHI handling, encryption, access controls, audit logging, and data governance.

## Usage

```
/compliance-checker <scope> [options]
```

### Scopes
- `lgpd` - Brazilian General Data Protection Law
- `ans` - ANS regulations for payments
- `soc2` - SOC 2 Type II controls
- `full` - All compliance frameworks

---

## LGPD Compliance Checklist

### Data Classification

| Category | Fields | Treatment |
|----------|--------|-----------|
| Personal Identification | CPF, RG, CNS, name | Encrypt at rest, tokenize for analytics |
| Contact Data | email, phone, address | Encrypt at rest, mask in non-prod |
| Health Data (PHI) | CID-10, procedures, diagnoses, medical reports | Encrypt at rest, strict access control, ANS audit |
| Sensitive Personal | biometrics | Do not process unless legally required |

### PII/PHI Protection Requirements

```markdown
Kafka Topics:
- [ ] All PII fields tagged with `confluent:tags: ["PII", "LGPD"]` in Avro schemas
- [ ] All PHI fields tagged with `confluent:tags: ["PHI", "SAUDE", "ANS"]` in Avro schemas
- [ ] PII tokenized (SHA-256) before reaching analytics consumers
- [ ] PHI access restricted to authorized clinical systems only
- [ ] PII/PHI masked in non-production environments
- [ ] No PII/PHI in topic names or message keys (use surrogate keys)

Schema Registry:
- [ ] Stream Governance enabled for PII/PHI discovery
- [ ] Tag propagation configured for derived topics
- [ ] Schema audit trail maintained

Encryption:
- [ ] TLS 1.2+ for all Kafka connections (SASL_SSL)
- [ ] At-rest encryption enabled on Confluent Cloud clusters
- [ ] S3 data lake encrypted with AWS KMS (SSE-KMS)
- [ ] Snowflake encrypted with customer-managed keys
- [ ] Oracle connections over SSL/TLS

Access Control:
- [ ] RBAC enabled with least-privilege service accounts
- [ ] PII/PHI topics restricted to authorized consumers only
- [ ] API keys scoped to specific service accounts
- [ ] No shared credentials between services
```

### Data Subject Rights

| Right | Implementation |
|-------|---------------|
| Access | Query by CPF/CNS across all topics via Flink SQL |
| Rectification | Publish correction event, propagate via CDC |
| Erasure | Publish tombstone events, compact topics |
| Portability | Export via Flink SQL to structured format |

---

## ANS Compliance

### Domain Regulations (legacy: health insurance — ANS/TISS; current: payments — LGPD/PCI-DSS)

```markdown
TISS (Troca de Informações em Saúde Suplementar):
- [ ] All guia types comply with TISS standard (CONSULTA, SP_SADT, INTERNACAO, ODONTOLOGICA, HONORARIOS)
- [ ] TISS XML/JSON interchange format validated
- [ ] Guia lifecycle tracked end-to-end (SOLICITADA → AUTORIZADA → EXECUTADA → FATURADA → PAGA/GLOSADA)
- [ ] Guia authorization SLA monitored (<24h for non-urgent, immediate for emergencies)

Data Retention:
- [ ] Health records retained minimum 20 years (ANS RN 305)
- [ ] Guia data retained minimum 10 years
- [ ] S3/Snowflake data lake with lifecycle policies meeting retention
- [ ] Kafka topic retention set for operational needs (30d CDC, infinite state)

Authorization & Claims:
- [ ] Real-time authorization validation with <30s response
- [ ] Glosa detection with automated rules per TUSS procedures
- [ ] Sinistralidade monitoring per prestador/plano (target 75-85%)
- [ ] Coparticipação calculation automated per contract

Operational:
- [ ] 99.9% availability SLA for authorization processing
- [ ] Disaster recovery plan with RTO < 4h, RPO < 1h
- [ ] Business continuity tested quarterly
```

### ANS Reporting

```markdown
- [ ] SIB (Sistema de Informações de Beneficiários) data updated monthly
- [ ] SIP (Sistema de Informações de Produtos) compliance verified
- [ ] DIOPS (Documento de Informações Periódicas) data pipeline automated
- [ ] Sinistralidade report per plano/prestador/procedimento
- [ ] Carência periods enforced per ANS rules (24h urgência, 180d consultas, 300d parto)
```

---

## SOC 2 Type II Controls

### Security Controls

```markdown
Access Management:
- [ ] Service accounts follow naming convention: sa-workshop-{service}-{role}
- [ ] API keys rotated every 90 days
- [ ] No shared or default credentials
- [ ] Service account inventory maintained

Network Security:
- [ ] Production cluster uses PrivateLink (no public internet)
- [ ] Security groups restrict to known CIDR ranges
- [ ] VPC flow logs enabled for audit

Change Management:
- [ ] All infrastructure changes via Terraform (IaC)
- [ ] Terraform plans reviewed before apply
- [ ] Git history tracks all configuration changes
- [ ] No manual changes to production resources

Monitoring:
- [ ] CloudWatch alarms on connector failures
- [ ] CDC lag monitoring with <5min alert threshold
- [ ] DLQ monitoring with immediate alerts on errors
- [ ] API key usage audited via Confluent Cloud audit log
```

### Availability Controls

```markdown
- [ ] Multi-AZ deployment for Confluent Cloud Dedicated
- [ ] Connect workers in Auto Scaling Group across AZs
- [ ] S3 cross-region replication for critical data
- [ ] Snowflake fail-safe for critical tables
- [ ] Runbooks documented for all failure scenarios
```

---

## Compliance Validation Commands

### Check PII/PHI Tags in Schema Registry

```bash
# List subjects with PII/PHI tags
confluent schema-registry subject list | while read subject; do
  tags=$(confluent schema-registry subject describe "$subject" --show-tags 2>/dev/null | grep -cE "PII|PHI")
  if [ "$tags" -gt 0 ]; then
    echo "SENSITIVE: $subject ($tags fields)"
  fi
done
```

### Audit Service Account Access

```bash
# List all service accounts and their ACLs
confluent iam service-account list -o json | jq -r '.[].id' | while read sa; do
  echo "=== $sa ==="
  confluent kafka acl list --service-account "$sa" --cluster <cluster-id>
done
```

### Check Encryption Status

```bash
# Verify cluster encryption
confluent kafka cluster describe <cluster-id> -o json | jq '.spec.config.encryption'

# Verify Schema Registry encryption
confluent schema-registry cluster describe -o json | jq '.spec.config'
```

---

## Compliance Report Template

```markdown
# Compliance Report — the workshop Data Platform
**Date**: YYYY-MM-DD
**Reviewer**: [name]

## LGPD Status
- PII/PHI Fields Cataloged: X/Y topics
- PII Tokenization: [PASS/FAIL]
- PHI Access Controls: [PASS/FAIL]
- Data Subject Rights: [PASS/FAIL]

## ANS Status
- TISS Compliance: [PASS/FAIL]
- Data Retention (20yr): [PASS/FAIL]
- Authorization SLA: [PASS/FAIL]
- Sinistralidade Monitoring: [PASS/FAIL]
- ANS Reporting (SIB/SIP/DIOPS): [PASS/FAIL]

## SOC 2 Status
- Access Management: [PASS/FAIL]
- Network Security: [PASS/FAIL]
- Change Management: [PASS/FAIL]
- Monitoring & Alerting: [PASS/FAIL]

## Action Items
1. [action] — Owner: [name] — Due: [date]
```

---

## Best Practices

1. **Tag PII/PHI at schema registration time** — never deploy a schema with sensitive data without tags
2. **Use Confluent Stream Governance** for automated PII/PHI discovery
3. **Automate compliance checks** in CI/CD pipeline
4. **Review access quarterly** and remove unused service accounts
5. **Test data subject rights** (access, erasure) in staging monthly
6. **Document all compliance decisions** with rationale
7. **Encrypt everything** — at rest, in transit, in backups

---

**Version**: 1.0
**Last Updated**: 2026-02-12
