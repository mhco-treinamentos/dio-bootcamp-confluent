# VALIDATE Phase

> **Ensure quality, compliance, and completeness before delivery**

The VALIDATE phase is the final quality gate before work is considered complete. It verifies that all artifacts meet standards, comply with regulations, and are properly documented.

---

## Purpose

- Verify code quality and standards compliance
- Validate regulatory compliance (ANS, LGPD)
- Run tests and quality checks
- Ensure documentation is complete
- Generate compliance reports
- Prepare for deployment

---

## When to Use VALIDATE Phase

### Always Validate When:
- ACT phase is complete
- Before creating PR/MR
- Before deployment
- For compliance-sensitive features

### Validation is Mandatory For:
- Any PII handling
- ANS-regulated data
- Schema changes
- Infrastructure changes
- Security configurations

---

## VALIDATE Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATE PHASE FLOW                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. CODE QUALITY                                                 │
│     └─► Style checks (Black, Ruff)                              │
│     └─► Type checking (mypy)                                    │
│     └─► Code review                                             │
│                                                                  │
│  2. TESTS                                                        │
│     └─► Unit tests                                              │
│     └─► Integration tests                                       │
│     └─► Contract tests                                          │
│                                                                  │
│  3. COMPLIANCE                                                   │
│     └─► ANS checklist                                         │
│     └─► LGPD checklist                                          │
│     └─► Security scan                                           │
│                                                                  │
│  4. DATA QUALITY                                                 │
│     └─► Schema validation                                       │
│     └─► SodaCL checks                                           │
│     └─► Data contract verification                              │
│                                                                  │
│  5. DOCUMENTATION                                                │
│     └─► CHANGELOG complete                                      │
│     └─► README updated                                          │
│     └─► API docs generated                                      │
│                                                                  │
│  6. FINAL REPORT                                                 │
│     └─► Generate validation report                              │
│     └─► List all checks passed                                  │
│     └─► Ready for PR/deployment                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## SLO & Threshold Reference

Every `VALIDATE` gate below is pass/fail against these hard thresholds. A run that meets every threshold is green; any miss returns the task to `ACT`.

### Code Quality Thresholds
| Check | Threshold | Tool |
|-------|-----------|------|
| Ruff errors | 0 | `ruff check src/` |
| Ruff warnings | ≤ 5 (deferrable) | `ruff check src/` |
| Black diff | 0 lines | `black --check --line-length 100 src/` |
| mypy errors (strict) | 0 on library code | `mypy src/ --strict` |
| Terraform fmt diff | 0 | `terraform fmt -check -recursive` |
| Terraform validate | PASS | `terraform validate` per module |

### Test Thresholds
| Test layer | Threshold |
|------------|-----------|
| Unit-test coverage | ≥ 80% line, ≥ 70% branch |
| Unit-test pass rate | 100% |
| Integration-test pass rate | 100% on critical paths |
| Property-based (Hypothesis) | 0 falsifying examples |
| Flaky tests | 0 tolerated — quarantine or fix |

### Schema & Compatibility Thresholds
| Check | Threshold |
|-------|-----------|
| Avro schema validity | 100% parseable |
| Schema Registry compatibility | BACKWARD (default) or per-subject policy |
| Untagged PII/PHI fields | **0** — enforced by [hooks/validate-avro-pii.sh](../hooks/validate-avro-pii.sh) |
| Data Contract freshness | Updated within 7 days of schema change |

### Runtime SLOs (production-bound features)
| Metric | Target |
|--------|--------|
| Oracle → Confluent CDC lag (p95) | < 30s |
| Flink job end-to-end latency (p95) | < 60s for enrichment, < 5m for aggregation |
| Snowflake MERGE task end-to-end latency (p95) | < 5 min for silver, < 15 min for gold |
| Exactly-once guarantees | No duplicate / missed records after restart test |
| Kafka replication factor | ≥ 3 (prod), ≥ 2 (staging) |
| `min.insync.replicas` | ≥ 2 (prod) |

### Compliance Thresholds
| Check | Threshold |
|-------|-----------|
| ANS retention on regulated topics (guias, sinistros) | ≥ 365 days |
| LGPD PII tag coverage | 100% of sensitive-named fields tagged |
| Secrets in code / `.tfvars` | 0 — enforced by code review + `tfsec` |
| Service accounts per service | 1:1 (no shared credentials) |
| API key rotation age | ≤ 90 days |

### Documentation Thresholds
| Check | Threshold |
|-------|-----------|
| `CHANGELOG.md` entries for code changes | 100% — enforced by [hooks/pre-commit-changelog.sh](../hooks/pre-commit-changelog.sh) |
| README updates for new features | Required |
| Data contract present for new topics | Required |

---

## Step 1: Code Quality

### Style Checks

```bash
# Python formatting
black --check --line-length 100 src/

# Python linting
ruff check src/

# Type checking
mypy src/ --strict

# SQL formatting (manual review)
# Ensure UPPERCASE keywords
```

### Code Review Checklist

```markdown
Using code-reviewer skill, verify:

Structure:
- [ ] Functions are focused (single responsibility)
- [ ] No code duplication
- [ ] Proper error handling
- [ ] Logging (not print statements)

Python Specific:
- [ ] Type hints on all functions
- [ ] NumPy-style docstrings
- [ ] No hardcoded values
- [ ] Imports organized (isort)

SQL Specific:
- [ ] UPPERCASE keywords
- [ ] Parameterized queries
- [ ] Proper comments
- [ ] Efficient queries

Terraform Specific:
- [ ] Variables for environment values
- [ ] Descriptions on resources
- [ ] Tags on all resources
- [ ] Sensitive values marked
```

### Using code-reviewer Skill

```markdown
"Use code-reviewer skill to review all generated
artifacts for quality and standards compliance"

Output:
- Code quality score
- Issues found (critical, warning, info)
- Suggested improvements
```

---

## Step 2: Tests

### Test Coverage Requirements

| Test Type | Coverage | Purpose |
|-----------|----------|---------|
| Unit tests | 80%+ | Function-level correctness |
| Integration tests | Critical paths | Component interaction |
| Contract tests | All schemas | API/schema compatibility |

### Running Tests

```bash
# Unit tests with coverage
pytest tests/unit/ -v --cov=src --cov-report=term-missing

# Integration tests
pytest tests/integration/ -v

# Contract tests (schema compatibility)
schema-registry-tools test-compatibility \
  --schema-file schemas/guia_v2.avsc \
  --subject guias-value
```

### Test Checklist

```markdown
- [ ] All unit tests pass
- [ ] Coverage >= 80%
- [ ] Integration tests pass
- [ ] Schema compatibility verified
- [ ] No flaky tests
- [ ] Edge cases covered
```

---

## Step 3: Compliance

### ANS Compliance Checklist

```markdown
Using compliance-checker skill with ANS rules:

Data Retention:
- [ ] Domain-specific claims (legacy: guias de saúde; current workshop: estornos): 1 year minimum per ANS
- [ ] Glosa alerts: 365 days minimum
- [ ] No automatic deletion of regulated data

Audit Trail:
- [ ] All changes are traceable
- [ ] Event sourcing maintains history
- [ ] Timestamps on all events
- [ ] User/system identification

Data Integrity:
- [ ] Replication factor >= 3
- [ ] min.insync.replicas >= 2
- [ ] Exactly-once semantics where required

Reporting:
- [ ] Data available for regulatory queries
- [ ] Lineage can be demonstrated
- [ ] Access logs maintained
```

### LGPD Compliance Checklist

```markdown
Using compliance-checker skill with LGPD rules:

PII/PHI Protection:
- [ ] PII fields identified and tagged (CPF, CNS, NOME)
- [ ] PHI fields identified and tagged (CID_10, PROCEDIMENTOS)
- [ ] Tokenization/encryption applied
- [ ] Access controls in place
- [ ] Consent tracking (where applicable)

Data Subject Rights (LGPD):
- [ ] Right to access: Can export beneficiary data
- [ ] Right to erasure: Tombstoning implemented
- [ ] Right to portability: Data exportable

Documentation:
- [ ] Data processing records
- [ ] Privacy impact assessment
- [ ] Data flow documentation

Schema Annotations:
- [ ] PII fields have `confluent:tags: ["PII", "LGPD"]`
- [ ] PHI fields have `confluent:tags: ["PHI", "SAUDE", "ANS"]`
- [ ] Sensitive fields documented
```

### Security Scan

```markdown
Security checks:
- [ ] No hardcoded secrets
- [ ] No exposed credentials
- [ ] Encryption at rest configured
- [ ] Encryption in transit (TLS)
- [ ] ACLs properly configured
- [ ] Service accounts with minimal privileges
```

### Using compliance-checker Skill

```markdown
"Use compliance-checker skill to validate all artifacts
against ANS and LGPD requirements"

Output:
- Compliance status (PASS/FAIL)
- Issues by regulation
- Remediation steps for failures
```

---

## Step 4: Data Quality

### Schema Validation

```markdown
Schema checks:
- [ ] Schema is valid (Avro/Protobuf/JSON Schema)
- [ ] Compatibility mode verified
- [ ] Evolution is backward compatible
- [ ] Required fields have defaults (for new optional fields)
- [ ] Field types are appropriate
- [ ] Documentation on all fields
```

### SodaCL Quality Checks

```yaml
# Example SodaCL checks
checks for guia_events:
  # Completeness
  - row_count > 0
  - missing(guia_id) = 0
  - missing(timestamp) = 0

  # Validity
  - invalid(valor_autorizado) = 0:
      valid min: 0.01
  - invalid(timestamp) = 0:
      valid max: now()

  # Uniqueness
  - duplicate(guia_id) = 0

  # Timeliness
  - freshness(timestamp) < 5m

  # Custom
  - failed rows:
      name: invalid_membro_cpf_format
      fail query: |
        SELECT * FROM guia_events
        WHERE membro_cpf NOT SIMILAR TO '[0-9]{11}'
```

### Data Contract Verification

```markdown
Using validation skill with data contract:

Contract checks:
- [ ] Schema matches contract specification
- [ ] Quality rules defined
- [ ] SLA terms documented
- [ ] Owner and contact specified
- [ ] Terms of use documented
```

---

## Step 5: Documentation

### CHANGELOG Verification

```markdown
CHANGELOG.md must have:
- [ ] Entries under `## [Unreleased]` for all code changes
- [ ] Correct categories (Added/Changed/Fixed/Deprecated/Removed/Security/Infrastructure/Documentation)
- [ ] Author attribution on each entry: `(@MateusHenriqueOliveira)` or `(@claude-code)`
- [ ] Imperative mood ("Add support for..." not "Added support for...")
- [ ] Specific descriptions (module names, file paths when relevant)
```

### Documentation Checklist

```markdown
- [ ] CHANGELOG.md updated
- [ ] README.md updated (if new features)
- [ ] API documentation generated
- [ ] Architecture diagrams updated
- [ ] Runbooks updated (if operational)
- [ ] Data dictionary updated (if schema changed)
```

### Using doc-generator Skill

```markdown
"Use doc-generator skill to verify documentation
completeness and generate any missing docs"
```

---

## Step 6: Final Report

### Validation Report Template

```markdown
# Validation Report

**Feature**: [Feature Name]
**Date**: YYYY-MM-DD
**Validator**: Claude Code

## Summary

| Category | Status | Score |
|----------|--------|-------|
| Code Quality | PASS | 95% |
| Tests | PASS | 87% coverage |
| ANS Compliance | PASS | 100% |
| LGPD Compliance | PASS | 100% |
| Data Quality | PASS | All checks |
| Documentation | PASS | Complete |

**Overall Status**: READY FOR DEPLOYMENT ✓

## Code Quality

### Style Checks
- Black: PASS
- Ruff: PASS (0 errors, 2 warnings)
- mypy: PASS

### Code Review
- Critical issues: 0
- Warnings: 2 (addressed)
- Suggestions: 3 (deferred)

## Tests

### Unit Tests
- Total: 45
- Passed: 45
- Failed: 0
- Coverage: 87%

### Integration Tests
- Total: 12
- Passed: 12
- Failed: 0

### Contract Tests
- Schema compatibility: BACKWARD ✓

## Compliance

### ANS
- Retention: 5 years ✓
- Audit trail: Complete ✓
- Replication: 3 ✓

### LGPD
- PII annotated: ✓
- Tokenization: Applied ✓
- Access controls: Configured ✓

## Data Quality

### Schema
- Valid: ✓
- Compatible: BACKWARD ✓
- Documented: ✓

### SodaCL
- Checks defined: 8
- All passing: ✓

## Documentation

- CHANGELOG: Updated ✓
- README: Updated ✓
- Data contract: Created ✓

## Artifacts Ready for Deployment

| Artifact | Location | Status |
|----------|----------|--------|
| Terraform | /terraform/topics/ | Ready |
| Schema | /schemas/ | Ready |
| Flink SQL | /src/flink/ | Ready |
| Data Contract | /contracts/ | Ready |

## Approvals Required

- [ ] Data Platform Team
- [ ] Data Governance (PII)
- [ ] InfoSec (if ACLs changed)

## Next Steps

1. Create PR with all artifacts
2. Request reviews from required teams
3. Deploy to staging environment
4. Run smoke tests
5. Deploy to production

---
**Validation Phase Complete** ✓
Ready for PR/deployment.
```

---

## Skills Used in VALIDATE Phase

| Skill | Purpose |
|-------|---------|
| `code-reviewer` | Review code quality |
| `compliance-checker` | ANS/LGPD validation |
| `validation` | Schema and config validation |
| `governance-reporter` | Generate compliance reports |
| `doc-generator` | Verify documentation |

---

## Validation Failures

### Handling Failures

```markdown
If validation fails:

1. DO NOT proceed to deployment
2. Document the failure
3. Categorize severity:
   - CRITICAL: Must fix before any deployment
   - HIGH: Should fix before production
   - MEDIUM: Can fix in next iteration
   - LOW: Nice to have

4. Actions:
   - CRITICAL/HIGH: Return to ACT phase
   - MEDIUM/LOW: Document and proceed with acknowledgment
```

### Failure Categories

| Category | Example | Action |
|----------|---------|--------|
| Code Quality | Style violations | Fix and retry |
| Test Failure | Unit test fails | Fix code and retry |
| Compliance | ANS violation | MUST fix |
| Schema | Incompatible change | Review evolution strategy |
| Documentation | Missing CHANGELOG | Add and retry |

---

## Transition to Complete

### Validation Complete Checklist

- [ ] All code quality checks pass
- [ ] All tests pass with required coverage
- [ ] ANS compliance verified
- [ ] LGPD compliance verified
- [ ] Security scan clean
- [ ] Schema validated
- [ ] Data quality rules defined
- [ ] Documentation complete
- [ ] Validation report generated

### Completion Command

```
"VALIDATE phase complete. All checks passed.

Validation Report Summary:
- Code Quality: PASS
- Tests: 87% coverage
- Compliance: PASS
- Documentation: Complete

Ready to create PR and proceed to deployment."
```

---

## Examples

### Example 1: Validate Kafka Topic Implementation

```markdown
VALIDATE Phase for: Guias CDC Topic

1. Code Quality:
   - Terraform fmt: PASS
   - Terraform validate: PASS
   - tfsec security scan: PASS

2. Tests:
   - Plan test: PASS
   - Apply dry-run: PASS

3. Compliance:
   - ANS retention (1 year): PASS
   - Replication factor (3): PASS
   - min.insync.replicas (2): PASS

4. Data Quality:
   - Schema valid: PASS
   - PII annotated: PASS

5. Documentation:
   - CHANGELOG updated: PASS
   - Data contract created: PASS

Result: READY FOR DEPLOYMENT ✓
```

### Example 2: Validate Flink Job

```markdown
VALIDATE Phase for: Glosa Detection Flink Job

1. Code Quality:
   - SQL syntax: PASS
   - Watermark strategy: PASS
   - Error handling: PASS

2. Tests:
   - Unit tests (CEP patterns): PASS
   - Integration test: PASS

3. Compliance:
   - LGPD (no PII/PHI in alerts): PASS
   - Audit logging: PASS

4. Data Quality:
   - Input schema compatible: PASS
   - Output schema valid: PASS
   - SodaCL rules defined: PASS

5. Documentation:
   - Job documented: PASS
   - Runbook created: PASS

Result: READY FOR DEPLOYMENT ✓
```

---

**Version**: 1.0
**Last Updated**: 2026-01-27
**Phase**: VALIDATE (4 of 4)
