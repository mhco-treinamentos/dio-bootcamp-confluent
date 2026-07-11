# RESEARCH Phase

> **Gather all necessary context before making any decisions**

The RESEARCH phase ensures you have complete understanding before planning or implementing anything. This phase is critical for avoiding rework and ensuring solutions align with current best practices.

---

## Purpose

- Understand existing patterns and implementations
- Find latest features and updates from official sources
- Identify relevant compliance requirements
- Gather domain expertise from specialized agents
- Update outdated reference documentation

---

## When to Use RESEARCH Phase

### Always Research When:
- Implementing a new feature or pattern
- Technology version has changed
- Compliance requirements may have updated
- You're unfamiliar with the domain
- References are older than 6 months

### Skip Research When:
- Making simple fixes (typos, formatting)
- Following an existing, recent pattern
- Task is well-defined with clear requirements

---

## RESEARCH Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    RESEARCH PHASE FLOW                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. CHECK REFERENCES                                             │
│     └─► Read relevant .claude/references/*.md                   │
│     └─► Note version/date of documentation                      │
│                                                                  │
│  2. WEB SEARCH                                                   │
│     └─► Search for latest official documentation                │
│     └─► Check for version updates                               │
│     └─► Find new patterns or deprecations                       │
│                                                                  │
│  3. COMPARE & UPDATE                                             │
│     └─► Compare reference docs with web findings                │
│     └─► Update references if significantly outdated             │
│     └─► Document what changed                                   │
│                                                                  │
│  4. CONSULT AGENTS                                               │
│     └─► Ask domain experts for recommendations                  │
│     └─► Validate approach against best practices                │
│                                                                  │
│  5. EXPLORE CODEBASE                                             │
│     └─► Find existing similar implementations                   │
│     └─► Understand current patterns in use                      │
│                                                                  │
│  6. SYNTHESIZE                                                   │
│     └─► Create research summary                                 │
│     └─► List recommendations                                    │
│     └─► Identify risks and considerations                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Check References

### Relevant Reference Files

| Topic | Reference File | Check For |
|-------|---------------|-----------|
| Stack do bootcamp | `bootcamp-stack-guide.md` | Confluent Cloud updates, fallback local |
| CDC / Connect | `cdc-connect-patterns.md` | Debezium updates, SMT changes |
| Producers Kafka | `kafka-producer-guide.md` | Library versions, delivery semantics |
| Modelagem de dados | `data-modeling.md` | Schema evolution patterns |
| Dados sintéticos | `faker-patterns.md` | Provider updates |
| Formatos brasileiros | `brazilian-data-formats.md` | CPF/CNPJ/PIX rules |
| Padrões comportamentais | `behavioral-patterns-guide.md` | Fraud pattern shapes |
| Poison pills / DLQ | `poison-pill-patterns.md` | DLQ handling updates |

### Check Reference Currency

```markdown
When reading a reference, note:
- Last Updated date at bottom of file
- Version numbers of libraries mentioned
- Any deprecated patterns mentioned

If reference is >6 months old OR mentions outdated versions:
→ Proceed to Web Search to verify/update
```

---

## Step 2: Web Search

### Search Strategy

For each technology, search official sources first:

#### Kafka / Confluent
```
Sites to search:
- docs.confluent.io (Confluent official)
- kafka.apache.org (Apache Kafka official)
- developer.confluent.io (Developer portal)
- github.com/confluentinc (Release notes)

Example searches:
"site:docs.confluent.io Flink SQL CEP 2024"
"site:kafka.apache.org KRaft migration"
"confluent cloud flink release notes 2024"
```

#### Apache Flink
```
Sites to search:
- nightlies.apache.org/flink (Flink docs)
- flink.apache.org (Official site)
- github.com/apache/flink (Release notes)

Example searches:
"site:nightlies.apache.org flink MATCH_RECOGNIZE"
"apache flink 1.19 new features"
```

#### Compliance Regulations
```
Sites to search:
- gov.br/lgpd (LGPD official)
- bcb.gov.br (BACEN — PIX, open finance)
- pcisecuritystandards.org (PCI-DSS)

Example searches:
"site:bcb.gov.br resolução PIX 2026"
"LGPD anonimização streaming dados pessoais"
```

### Using web-research Skill

```markdown
Invoke web-research skill with:
- Specific technology + version
- "latest" or "2024" for currency
- Official documentation preference

Example:
"Use web-research skill to find the latest Confluent Cloud Flink
features released in 2024, focusing on CEP and MATCH_RECOGNIZE"
```

---

## Step 3: Compare & Update References

### When to Update References

| Condition | Action |
|-----------|--------|
| Minor version bump (1.18 → 1.19) | Add notes, keep existing content |
| Major version bump (1.x → 2.x) | Significant update needed |
| New feature relevant to project | Add new section |
| Deprecated pattern in use | Mark as deprecated, add alternative |
| Security advisory | Immediate update with warning |

### Update Process

```markdown
1. Create a backup note of what changed
2. Update the reference file with new information
3. Update the "Last Updated" date
4. Add a version history note at bottom
5. Mark any deprecated patterns clearly
```

### Example Update

```markdown
## Version History

### 2026-01-27
- Updated Flink SQL section for Confluent Cloud Flink 1.19
- Added new MATCH_RECOGNIZE timeout clause
- Deprecated old watermark syntax (use new WATERMARK FOR clause)

### 2024-06-15
- Initial version based on Flink 1.17
```

---

## Step 4: Consult Agents

### Agent Selection for Research

| Research Topic | Primary Agent | Supporting Agents |
|---------------|---------------|-------------------|
| Kafka / streaming patterns | `kafka-streaming-expert` | `data-modeler` |
| Stream processing (Flink SQL) | `flink-confluent-expert` | `kafka-streaming-expert` |
| Confluent Cloud (setup, clusters, custos) | `confluent-cloud-expert` | `observability-cost-expert` |
| Data integration (Connect/CDC) | `connect-cdc-expert` | `kafka-streaming-expert` |
| Lakehouse (Tableflow/Iceberg/catálogos) | `tableflow-lakehouse-expert` | `data-modeler` |
| Real-Time AI (ML_PREDICT, Vector Search) | `realtime-ai-expert` | `flink-confluent-expert` |
| Security/Compliance (RBAC, LGPD) | `kafka-security-expert` | `data-modeler` |
| Observabilidade e produção | `observability-cost-expert` | `confluent-cloud-expert` |
| Domain-specific (payments/fraude) | `data-modeler` | `data-quality-expert` |

### Questions to Ask Agents

```markdown
Architecture questions:
- "What is the recommended pattern for [X] in 2024?"
- "Are there any deprecated approaches we should avoid?"
- "What are the trade-offs between [A] and [B]?"

Compliance questions:
- "Does this approach meet LGPD/BACEN requirements?"
- "What LGPD considerations apply to this data?"
- "Are there any new regulatory requirements?"

Implementation questions:
- "What is the most performant approach for [X]?"
- "What configuration is recommended for [Y] throughput?"
```

---

## Step 5: Explore Codebase

### What to Look For

```markdown
1. Similar implementations
   - How was this pattern used before?
   - What worked well? What didn't?

2. Existing utilities
   - Are there helper functions we can reuse?
   - Is there a common pattern library?

3. Configuration patterns
   - How are similar configs structured?
   - What naming conventions are used?

4. Test patterns
   - How are similar features tested?
   - What test fixtures exist?
```

### Exploration Commands

```bash
# Find similar implementations
grep -r "pattern_name" --include="*.py"
grep -r "ClassName" --include="*.java"

# Find configuration examples
find . -name "*.tf" | xargs grep "resource_type"
find . -name "*.yaml" | xargs grep "config_key"

# Find test patterns
find . -path "*/tests/*" -name "*.py" | xargs grep "test_similar"
```

---

## Step 6: Synthesize Research

### Research Summary Template

```markdown
# Research Summary: [Topic]

**Date**: YYYY-MM-DD
**Researcher**: Claude Code
**Request**: [Original user request]

## Context
[Brief description of what was researched and why]

## Key Findings

### From References
- [Finding 1 from reference docs]
- [Finding 2 from reference docs]

### From Web Search
- [Finding 1 from web] (Source: [URL])
- [Finding 2 from web] (Source: [URL])

### From Agents
- [Recommendation from agent] (Agent: [name])

### From Codebase
- [Existing pattern found]
- [Reusable component identified]

## Reference Updates Made
- [Reference file]: [What was updated]

## Recommendations

### Recommended Approach
[Description of recommended approach]

### Alternatives Considered
| Approach | Pros | Cons |
|----------|------|------|
| Option A | ... | ... |
| Option B | ... | ... |

### Risks and Considerations
- [Risk 1]
- [Risk 2]

## Next Steps
Ready to proceed to PLAN phase with:
1. [Item to plan]
2. [Item to plan]

---
**Research Phase Complete** ✓
```

---

## Reference Update Automation

### Automatic Update Triggers

When web search finds significant updates, Claude Code should:

1. **Notify User**
   ```
   "I found that [reference] is outdated. The latest version of [X]
   is [Y], which includes [new features]. Should I update the reference?"
   ```

2. **Propose Changes**
   ```
   "Here are the proposed updates to [reference]:
   - Add section on [new feature]
   - Update [config] from [old] to [new]
   - Mark [pattern] as deprecated
   ```

3. **Update with Approval**
   - Make changes to reference file
   - Add version history entry
   - Update "Last Updated" date

### Reference Freshness Check

```markdown
For each reference, check:
- [ ] Last Updated < 6 months ago
- [ ] Library versions are current
- [ ] No deprecated patterns unmarked
- [ ] Links are still valid
- [ ] Examples still work
```

---

## Skills Used in RESEARCH Phase

| Skill | Purpose |
|-------|---------|
| `web-research` | Search and analyze web content |
| `reference-updater` | Check and update reference documentation |
| `validation` | Verify reference currency |
| `code-reviewer` | Analyze existing codebase patterns |

---

## Transition to PLAN Phase

### Research Complete Checklist

- [ ] All relevant references checked
- [ ] Web search completed for updates
- [ ] Outdated references updated (if any)
- [ ] Domain agents consulted
- [ ] Existing codebase patterns identified
- [ ] Research summary created
- [ ] Recommendations documented

### Transition Command

```
"Research complete. Ready to plan the implementation of [feature].
Key findings: [summary]. Proceed to PLAN phase?"
```

---

## Examples

### Example 1: Research Flink CEP for Fraud Detection

```markdown
User: "Research how to implement fraud detection using Flink CEP"

RESEARCH Phase:
1. Read references/flink-sql-patterns.md (CEP section)
2. Web search: "Confluent Cloud Flink MATCH_RECOGNIZE 2024"
3. Found: New timeout clause in Flink 1.19
4. Update: Added timeout examples to flink-sql-patterns.md
5. Consult: flink-streaming-expert for patterns
6. Consult: data-modeler for payments domain requirements
7. Explore: Found existing fraud patterns in /src/flink/fraud/
8. Synthesize: Research summary with CEP approach recommendation

Output: Research summary ready for PLAN phase
```

### Example 2: Research Schema Evolution

```markdown
User: "Research how to evolve our transaction schema"

RESEARCH Phase:
1. Read references/schema-registry-guide.md
2. Web search: "Schema Registry compatibility modes 2024"
3. Found: No significant updates needed
4. Consult: kafka-architecture-expert for evolution strategy
5. Consult: health-insurance-data-expert for PII/PHI field handling
6. Explore: Found current schema in /schemas/transaction-v1.avsc
7. Synthesize: Evolution options with compatibility analysis

Output: Research summary with schema evolution options
```

---

**Version**: 1.0
**Last Updated**: 2026-01-27
**Phase**: RESEARCH (1 of 4)
