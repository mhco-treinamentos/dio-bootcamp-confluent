# ACT Phase

> **Execute the plan by creating all necessary artifacts**

The ACT phase is where implementation happens. Following the approved plan, Claude Code generates production-ready code, configurations, and documentation.

---

## Purpose

- Generate production-quality code
- Create infrastructure configurations
- Design and register schemas
- Configure connectors and pipelines
- Build all required artifacts
- Follow the approved todo list

---

## When to Use ACT Phase

### Always ACT After:
- PLAN phase is approved
- Dependencies are met
- Previous steps are validated

### Direct ACT (Skip RESEARCH/PLAN):
- Simple, well-defined tasks
- Bug fixes with clear solution
- Following existing patterns exactly

---

## ACT Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                       ACT PHASE FLOW                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. LOAD PLAN                                                    │
│     └─► Load approved todo list                                 │
│     └─► Verify dependencies met                                 │
│                                                                  │
│  2. EXECUTE TASKS                                                │
│     └─► Mark task as in_progress                                │
│     └─► Generate artifacts                                      │
│     └─► Self-review output                                      │
│     └─► Mark task as completed                                  │
│     └─► Move to next task                                       │
│                                                                  │
│  3. GENERATE ARTIFACTS                                           │
│     └─► Code files                                              │
│     └─► Terraform configurations                                │
│     └─► Schema definitions                                      │
│     └─► Connector configurations                                │
│     └─► Flink SQL jobs                                          │
│     └─► Data contracts                                          │
│                                                                  │
│  4. SELF-REVIEW                                                  │
│     └─► Check against rules.md                                  │
│     └─► Verify compliance requirements                          │
│     └─► Ensure consistency                                      │
│                                                                  │
│  5. DOCUMENT                                                     │
│     └─► Update relevant documentation                           │
│     └─► Create CHANGELOG entry                                  │
│     └─► Generate inline comments                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Load Plan

### Verify Prerequisites

```markdown
Before starting ACT phase:

1. Plan Approved
   - [ ] User approved the plan
   - [ ] Scope is confirmed
   - [ ] Approach is validated

2. Dependencies Met
   - [ ] Required systems available
   - [ ] Team approvals received
   - [ ] Blocking tasks completed

3. Context Loaded
   - [ ] Todo list from PLAN phase
   - [ ] Architecture decisions
   - [ ] Research findings
```

### Load Todo List

```markdown
Retrieve the todo list created in PLAN phase:

Example:
1. [ ] Design fraud detection event schema
2. [ ] Create Kafka topic for transaction events
3. [ ] Create Kafka topic for fraud alerts
...
```

---

## Step 2: Execute Tasks

### Task Execution Pattern

```markdown
For each task in the todo list:

1. UPDATE STATUS
   TodoWrite: Mark task as "in_progress"

2. EXECUTE
   - Read relevant references
   - Use appropriate skill
   - Generate artifact

3. SELF-REVIEW
   - Check against rules.md
   - Verify completeness
   - Test locally if possible

4. COMPLETE
   TodoWrite: Mark task as "completed"

5. NEXT
   Move to next task in order
```

### Skill Selection by Task Type

| Task Type | Primary Skill | Supporting Skills |
|-----------|---------------|-------------------|
| Design Kafka topic | `topic-designer` | `validation` |
| Design schema | `schema-designer` | `validation` |
| Configure connector | `connector-configurator` | `validation` |
| Build Flink job | `flink-job-builder` | `validation` |
| Analyze performance | `performance-analyzer` | - |
| Create data contract | `data-contract-designer` | `validation` |
| Generate documentation | `doc-generator` | - |
| Review code | `code-reviewer` | `validation` |

---

## Step 3: Generate Artifacts

### Artifact Types and Standards

#### Code Files (Python)

```python
# Standards from rules.md:
# - Type hints on all functions
# - NumPy-style docstrings
# - Black formatter (100 char)
# - No hardcoded secrets

def process_transaction_event(
    event: TransactionEvent,
    config: ProcessorConfig
) -> ProcessedEvent:
    """
    Process a transaction event for fraud detection.

    Parameters
    ----------
    event : TransactionEvent
        Raw transaction event from Kafka
    config : ProcessorConfig
        Processor configuration

    Returns
    -------
    ProcessedEvent
        Enriched event ready for analysis

    Raises
    ------
    ValidationError
        If event fails validation
    """
    # Implementation
    pass
```

#### Terraform Configurations

```hcl
# Standards:
# - Use variables for environment-specific values
# - Include descriptions for all resources
# - Follow naming conventions
# - Tag all resources

resource "confluent_kafka_topic" "transaction_events" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "${var.environment}.payments.pg-payments.transactions.cdc.v1"
  partitions_count = var.partitions
  rest_endpoint    = var.rest_endpoint

  config = {
    "retention.ms"          = var.retention_ms        # lab budget — não deixar retention infinita
    "min.insync.replicas"   = "2"
    "cleanup.policy"        = "delete"
  }

  credentials {
    key    = var.api_key
    secret = var.api_secret
  }
}
```

#### Schema Definitions (Avro)

```json
{
  "type": "record",
  "name": "TransactionEvent",
  "namespace": "com.bootcamp.payments",
  "doc": "Card/PIX transaction event",
  "fields": [
    {
      "name": "transaction_id",
      "type": "string",
      "doc": "Unique transaction identifier"
    },
    {
      "name": "account_id",
      "type": "string",
      "doc": "Account identifier (tokenized)",
      "confluent:tags": ["PII", "LGPD"]
    },
    {
      "name": "amount",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 15,
        "scale": 2
      },
      "doc": "Transaction amount in BRL"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Event timestamp"
    }
  ]
}
```

#### Flink SQL Jobs

```sql
-- Standards:
-- - UPPERCASE keywords
-- - Clear comments
-- - Proper watermarks
-- - Error handling

-- No Flink gerenciado do Confluent Cloud, o topic com schema já aparece
-- como tabela no catálogo — não há DDL de conector Kafka.

-- Fraud detection with CEP (velocity: 3+ transactions in <60s)
CREATE TABLE `dev.payments.flink.fraud.detected.v1` AS
SELECT *
FROM `dev.payments.pg-payments.transactions.cdc.v1`
MATCH_RECOGNIZE (
    PARTITION BY account_id
    ORDER BY `$rowtime`
    MEASURES
        FIRST(A.transaction_id) AS first_txn,
        LAST(A.transaction_id) AS last_txn,
        COUNT(A.transaction_id) AS txn_count
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A{3,}) WITHIN INTERVAL '60' SECOND
    DEFINE
        A AS A.amount > 0.00
) AS velocity_pattern;
```

#### Connector Configurations

```json
{
  "name": "pg-payments-cdc",
  "config": {
    "connector.class": "PostgresCdcSourceV2",
    "database.hostname": "${DATABASE_HOST}",
    "database.port": "5432",
    "database.user": "${DATABASE_USER}",
    "database.password": "${DATABASE_PASSWORD}",
    "database.dbname": "payments",
    "topic.prefix": "pg-payments",
    "schema.include.list": "payments",
    "table.include.list": "payments.transactions",
    "output.data.format": "AVRO",
    "transforms": "route,mask",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "pg-payments.payments.(.*)",
    "transforms.route.replacement": "dev.payments.pg-payments.$1.cdc.v1",
    "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.mask.fields": "customer_cpf",
    "transforms.mask.replacement": "[MASKED]"
  }
}
```

#### Data Contracts

```yaml
dataContractSpecification: 0.9.3
id: urn:datacontract:bootcamp:payments:transactions:events:v1
info:
  title: Payment Transaction Events
  version: 1.0.0
  description: Real-time card/PIX transaction events
  owner: Data Platform Team
  contact:
    name: Bootcamp Data Platform
    email: data-platform@example.com

servers:
  dev:
    type: kafka
    host: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
    topic: dev.payments.pg-payments.transactions.cdc.v1
    format: avro

schema:
  type: avro
  specification: |
    {
      "type": "record",
      "name": "TransactionEvent",
      ...
    }

quality:
  type: SodaCL
  specification: |
    checks for transaction_events:
      - row_count > 0
      - missing(transaction_id) = 0
      - invalid(amount) = 0:
          valid min: 0.01
      - duplicate(transaction_id) = 0

terms:
  usage: Internal use for fraud detection and analytics
  limitations: PII/PCI must remain tokenized/masked
  billing: N/A
  noticePeriod: 30 days

sla:
  availability: 99.9%
  retention: 30 days  # lab budget — retention curta por default
  latency:
    p99: 100ms
    description: Event processing latency
```

---

## Step 4: Self-Review

### Review Checklist

```markdown
For each artifact, verify:

Code Quality:
- [ ] Type hints present (Python)
- [ ] Docstrings complete (NumPy style)
- [ ] No hardcoded secrets
- [ ] Proper error handling
- [ ] Logging (not print statements)

Compliance:
- [ ] PII fields tokenized/masked (PAN nunca cleartext)
- [ ] Retention adequada ao lab (não deixar infinita)
- [ ] LGPD annotations present
- [ ] Audit fields included

Standards:
- [ ] Naming conventions followed
- [ ] Formatting correct (Black/SQL uppercase)
- [ ] Comments explain WHY not WHAT
- [ ] Tests referenced or included

Integration:
- [ ] Compatible with existing code
- [ ] Follows project patterns
- [ ] Dependencies documented
```

### Using code-reviewer Skill

```markdown
After generating artifacts:
"Use code-reviewer skill to review the generated
Flink SQL job for compliance and best practices"
```

---

## Step 5: Document

### Documentation Requirements

```markdown
Every ACT phase must produce:

1. Inline Documentation
   - Code comments (why, not what)
   - Docstrings for functions
   - Schema field descriptions

2. CHANGELOG Entry
   - What was added/changed
   - Files affected
   - Impact assessment

3. README Updates (if needed)
   - New features documented
   - Configuration explained
   - Usage examples
```

### CHANGELOG Entry Template

Every ACT phase must add entries to `CHANGELOG.md` under `## [Unreleased]`.
Use [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format:

```markdown
## [Unreleased]

### Added
- Add `EntityName` generator with realistic Brazilian data patterns `(@claude-code)`
- Add `ScenarioName` scenario orchestrating X, Y, Z generators `(@claude-code)`

### Changed
- Refactor `ExistingModule` to support new configuration options `(@claude-code)`

### Fixed
- Fix race condition in `KafkaSink` when buffer is full `(@claude-code)`

### Infrastructure
- Update Docker Compose with new service configuration `(@claude-code)`
```

See `CHANGELOG.md` header and `.claude/rules.md` for full format reference.

---

## Artifact Output Summary

### Files Generated by ACT Phase

```markdown
After ACT phase, you should have:

/src/
  ├── events/
  │   └── transaction_event.py    # Event handler code
  ├── flink/
  │   └── fraud_detection.sql     # Flink SQL statement
  └── connectors/
      └── pg_payments_cdc.json    # Managed connector config

/terraform/
  ├── topics.tf                   # Kafka topics
  ├── schemas.tf                  # Schema registry
  └── connectors.tf               # Managed connectors

/schemas/
  └── transaction_event_v1.avsc   # Avro schema

/contracts/
  └── transaction_events.yaml     # Data contract

/docs/
  └── CHANGELOG.md                # Updated changelog
```

---

## Skills Used in ACT Phase

| Skill | Purpose |
|-------|---------|
| `topic-designer` | Design and configure Kafka topics |
| `schema-designer` | Design Avro/Protobuf schemas |
| `connector-configurator` | Configure Kafka Connect |
| `flink-job-builder` | Build Flink SQL jobs |
| `data-contract-designer` | Create data contracts |
| `code-reviewer` | Self-review generated code |
| `validation` | Validate configurations |

---

## Error Handling During ACT

### When Errors Occur

```markdown
If an error occurs during ACT:

1. DO NOT proceed to next task
2. Document the error
3. Analyze root cause
4. Options:
   a. Fix and retry
   b. Return to PLAN for revision
   c. Ask user for guidance

Never leave tasks partially completed.
```

### Error Recovery

```markdown
| Error Type | Action |
|------------|--------|
| Validation failure | Fix and retry |
| Dependency missing | Return to PLAN |
| Permission denied | Ask user |
| Unknown error | Document and ask user |
```

---

## Transition to VALIDATE Phase

### ACT Complete Checklist

- [ ] All todo items completed
- [ ] All artifacts generated
- [ ] Self-review passed
- [ ] Documentation updated
- [ ] CHANGELOG entry created
- [ ] No errors pending

### Transition Command

```
"ACT phase complete. All artifacts generated:
- [List of files created]

Proceeding to VALIDATE phase to verify
compliance and quality."
```

---

## Examples

### Example 1: ACT - Create Kafka Topic

```markdown
Task: Create Kafka topic for transactions

ACT Phase:
1. Mark task in_progress
2. Use topic-designer skill:
   - Name: dev.payments.pg-payments.transactions.cdc.v1
   - Partitions: 6
   - Retention: 30 days (lab budget)
   - Replication: 3
3. Generate Terraform:
   - terraform/topics/transactions_cdc.tf
4. Self-review:
   - Naming ✓
   - Retention ✓
   - Compliance tags ✓
5. Mark task completed

Output: Terraform file ready for deployment
```

### Example 2: ACT - Create Flink Job

```markdown
Task: Create fraud detection Flink statement

ACT Phase:
1. Mark task in_progress
2. Use flink-job-builder skill:
   - Source: transactions CDC topic
   - Pattern: CEP for transaction velocity
   - Sink: fraud alerts topic
3. Generate SQL:
   - src/flink/fraud_detection.sql
4. Self-review:
   - SQL syntax ✓
   - Watermarks ✓
   - Error handling ✓
5. Mark task completed

Output: Flink SQL job ready for deployment
```

---

**Version**: 1.0
**Last Updated**: 2026-01-27
**Phase**: ACT (3 of 4)
