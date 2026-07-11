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
1. [ ] Design glosa detection event schema
2. [ ] Create Kafka topic for guia events
3. [ ] Create Kafka topic for glosa alerts
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

def process_guia_event(
    event: GuiaEvent,
    config: ProcessorConfig
) -> ProcessedEvent:
    """
    Process a guia event for glosa detection.

    Parameters
    ----------
    event : GuiaEvent
        Raw guia event from Kafka
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

resource "confluent_kafka_topic" "guia_events" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "${var.environment}.saude.oracle-autorizacoes.guias.cdc.v1"
  partitions_count = var.partitions
  rest_endpoint    = var.rest_endpoint

  config = {
    "retention.ms"          = var.retention_ms        # ANS compliance retention
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
  "name": "GuiaEvent",
  "namespace": "com.workshop.payments.autorizacoes",
  "doc": "Guia TISS authorization event",
  "fields": [
    {
      "name": "guia_id",
      "type": "string",
      "doc": "Unique guia identifier"
    },
    {
      "name": "membro_id",
      "type": "string",
      "doc": "Beneficiary membro_id (tokenized)",
      "confluent:tags": ["PII", "LGPD"]
    },
    {
      "name": "valor_autorizado",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 15,
        "scale": 2
      },
      "doc": "Valor autorizado in BRL"
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

-- Create source table from Kafka
CREATE TABLE guia_events (
    guia_id STRING,
    membro_id STRING,
    valor_autorizado DECIMAL(15, 2),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'prod.saude.oracle-autorizacoes.guias.cdc.v1',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'glosa-detection',
    'format' = 'avro-confluent',
    'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);

-- Glosa detection with CEP
CREATE TABLE glosa_alerts AS
SELECT *
FROM guia_events
MATCH_RECOGNIZE (
    PARTITION BY membro_id
    ORDER BY event_time
    MEASURES
        FIRST(A.guia_id) AS first_guia,
        LAST(A.guia_id) AS last_guia,
        COUNT(A.guia_id) AS guia_count
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A{3,})
    DEFINE
        A AS A.valor_autorizado > 5000.00
) AS glosa_pattern;
```

#### Connector Configurations

```json
{
  "name": "source-oracle-autorizacoes",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "${DATABASE_HOST}",
    "database.port": "1521",
    "database.user": "${DATABASE_USER}",
    "database.password": "${DATABASE_PASSWORD}",
    "database.dbname": "PAYMENTS_DB",
    "database.server.name": "oracle-autorizacoes",
    "schema.include.list": "AUTORIZACOES",
    "table.include.list": "AUTORIZACOES.GUIAS",
    "database.history.kafka.bootstrap.servers": "${BOOTSTRAP_SERVERS}",
    "transforms": "route,mask",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "oracle-autorizacoes.AUTORIZACOES.(.*)",
    "transforms.route.replacement": "prod.saude.oracle-autorizacoes.$1.cdc.v1",
    "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.mask.fields": "membro_cpf",
    "transforms.mask.replacement": "[MASKED]"
  }
}
```

#### Data Contracts

```yaml
dataContractSpecification: 0.9.3
id: urn:datacontract:workshop:saude:autorizacoes:guias:events:v1
info:
  title: Guia TISS Events
  version: 1.0.0
  description: Real-time guia TISS authorization events
  owner: Data Platform Team
  contact:
    name: the workshop Data Platform
    email: data-platform@example.com

servers:
  production:
    type: kafka
    host: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
    topic: prod.saude.oracle-autorizacoes.guias.cdc.v1
    format: avro

schema:
  type: avro
  specification: |
    {
      "type": "record",
      "name": "GuiaEvent",
      ...
    }

quality:
  type: SodaCL
  specification: |
    checks for guia_events:
      - row_count > 0
      - missing(guia_id) = 0
      - invalid(valor_autorizado) = 0:
          valid min: 0.01
      - duplicate(guia_id) = 0

terms:
  usage: Internal use for glosa detection and analytics
  limitations: PII/PHI must remain tokenized
  billing: N/A
  noticePeriod: 30 days

sla:
  availability: 99.9%
  retention: 1 year  # ANS compliance requirement
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
- [ ] PII fields tokenized/masked
- [ ] ANS retention met (5 years)
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
  │   └── guia_event.py           # Event handler code
  ├── flink/
  │   └── glosa_detection.sql     # Flink SQL job
  └── connectors/
      └── oracle_autorizacoes.json # Connector config

/terraform/
  ├── topics.tf                   # Kafka topics
  ├── schemas.tf                  # Schema registry
  └── connectors.tf               # Managed connectors

/schemas/
  └── guia_event_v1.avsc          # Avro schema

/contracts/
  └── guia_events.yaml            # Data contract

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
Task: Create Kafka topic for guias

ACT Phase:
1. Mark task in_progress
2. Use topic-designer skill:
   - Name: prod.saude.oracle-autorizacoes.guias.cdc.v1
   - Partitions: 24
   - Retention: 1 year (ANS)
   - Replication: 3
3. Generate Terraform:
   - terraform/topics/guias_cdc.tf
4. Self-review:
   - Naming ✓
   - ANS retention ✓
   - Compliance tags ✓
5. Mark task completed

Output: Terraform file ready for deployment
```

### Example 2: ACT - Create Flink Job

```markdown
Task: Create glosa detection Flink job

ACT Phase:
1. Mark task in_progress
2. Use flink-job-builder skill:
   - Source: guia events topic
   - Pattern: CEP for rapid guias
   - Sink: glosa alerts topic
3. Generate SQL:
   - src/flink/glosa_detection.sql
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
