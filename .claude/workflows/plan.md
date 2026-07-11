# PLAN Phase

> **Design the solution before writing any code**

The PLAN phase ensures you have a clear, approved roadmap before implementation. This phase prevents scope creep, ensures alignment with stakeholders, and creates a traceable decision log.

---

## Purpose

- Break complex tasks into actionable steps
- Design architecture before implementation
- Identify dependencies and blockers
- Get stakeholder approval on approach
- Create traceable decision documentation

---

## When to Use PLAN Phase

### Always Plan When:
- Implementing new features
- Making architectural changes
- Working on compliance-sensitive features
- Task involves multiple files or components
- Task has cross-team dependencies
- Estimated effort > 1 hour

### Skip Planning When:
- Simple bug fix with clear solution
- Single-line changes
- Following an existing approved plan
- Emergency hotfix (document after)

---

## PLAN Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                      PLAN PHASE FLOW                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. ENTER PLAN MODE                                              │
│     └─► Use EnterPlanMode tool                                  │
│     └─► Set context from RESEARCH phase                         │
│                                                                  │
│  2. CREATE TODO LIST                                             │
│     └─► Break down into atomic tasks                            │
│     └─► Order by dependencies                                   │
│     └─► Estimate complexity                                     │
│                                                                  │
│  3. DESIGN ARCHITECTURE                                          │
│     └─► Create diagrams (if needed)                             │
│     └─► Document technical decisions                            │
│     └─► Identify integration points                             │
│                                                                  │
│  4. IDENTIFY DEPENDENCIES                                        │
│     └─► What must exist first?                                  │
│     └─► What teams need to be involved?                         │
│     └─► What approvals are needed?                              │
│                                                                  │
│  5. RISK ASSESSMENT                                              │
│     └─► What could go wrong?                                    │
│     └─► What's the rollback plan?                               │
│     └─► What's the compliance impact?                           │
│                                                                  │
│  6. GET APPROVAL                                                 │
│     └─► Present plan to user                                    │
│     └─► Address questions/concerns                              │
│     └─► Confirm proceed to ACT                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Enter Plan Mode

### Using Claude Code Plan Mode

```markdown
When to use EnterPlanMode:
- Complex multi-step implementations
- Architectural decisions needed
- Multiple valid approaches exist
- User approval required before proceeding

Claude Code will:
1. Activate plan mode
2. Allow exploration without making changes
3. Present plan for approval
4. Exit plan mode to execute
```

### Plan Mode Commands

| Command | Action |
|---------|--------|
| `EnterPlanMode` | Start planning (no code changes) |
| `ExitPlanMode` | Submit plan for approval |
| `AskUserQuestion` | Clarify requirements |

---

## Step 2: Create Todo List

### Todo List Structure

```markdown
Use TodoWrite tool to create structured tasks:

Each todo item must have:
- content: What needs to be done (imperative)
- activeForm: What's happening (present continuous)
- status: pending | in_progress | completed

Example:
{
  "content": "Create Avro schema for transaction events",
  "activeForm": "Creating Avro schema for transaction events",
  "status": "pending"
}
```

### Task Breakdown Guidelines

```markdown
Good task breakdown:
✅ Atomic - Can be completed independently
✅ Verifiable - Clear completion criteria
✅ Ordered - Dependencies are clear
✅ Sized - 15-60 minutes each

Bad task breakdown:
❌ "Implement the feature" - Too vague
❌ "Fix everything" - Not atomic
❌ "Maybe add tests" - Not verifiable
```

### Example Todo List

```markdown
For: "Implement fraud detection with Flink CEP"

1. [ ] Design fraud detection event schema
2. [ ] Create Kafka topic for transaction events
3. [ ] Create Kafka topic for fraud alerts
4. [ ] Create Kafka topic for DLQ
5. [ ] Write Avro schema for transaction events
6. [ ] Write Avro schema for fraud alerts
7. [ ] Create Flink SQL table definitions
8. [ ] Write CEP pattern for transaction velocity
9. [ ] Write CEP pattern for unusual valores
10. [ ] Write CEP pattern for merchant anomalies
11. [ ] Create Flink job configuration
12. [ ] Write unit tests for CEP patterns
13. [ ] Create Terraform for topics
14. [ ] Create data contract
15. [ ] Update documentation
```

---

## Step 3: Design Architecture

### When to Create Diagrams

| Scenario | Diagram Type |
|----------|--------------|
| New data flow | Flow diagram |
| System integration | Architecture diagram |
| Event sequences | Sequence diagram |
| State changes | State diagram |
| Component relationships | Component diagram |

### Using diagram-generator Skill

```markdown
Request architecture diagrams:
"Create an architecture diagram showing the fraud detection
data flow from transaction events through Flink to alerts"

Output: Mermaid diagram in documentation
```

### Architecture Decision Template

```markdown
# Architecture Decision: [Title]

## Context
[What is the situation that requires a decision?]

## Decision
[What is the decision that was made?]

## Rationale
[Why was this decision made?]

## Alternatives Considered

| Alternative | Pros | Cons | Why Not Chosen |
|-------------|------|------|----------------|
| Option A | ... | ... | ... |
| Option B | ... | ... | ... |

## Consequences
- [Consequence 1]
- [Consequence 2]

## Compliance Considerations
- BACEN/LGPD: [Impact]
- LGPD: [Impact]
```

---

## Step 4: Identify Dependencies

### Dependency Categories

```markdown
1. Technical Dependencies
   - What systems must exist?
   - What APIs are needed?
   - What infrastructure is required?

2. Team Dependencies
   - Who needs to approve?
   - Who needs to be informed?
   - Who needs to implement related work?

3. Sequence Dependencies
   - What must happen first?
   - What can happen in parallel?
   - What blocks other work?

4. External Dependencies
   - Vendor approvals?
   - Third-party services?
   - Regulatory approvals?
```

### Dependency Matrix

```markdown
| Task | Depends On | Blocks | Team | Status |
|------|------------|--------|------|--------|
| Create topic | - | Schema, Flink | Platform | Ready |
| Create schema | Topic | Flink | Platform | Waiting |
| Flink job | Topic, Schema | Tests | Data | Blocked |
| Tests | Flink job | Deploy | QA | Blocked |
```

### the workshop Team Dependencies

| Team | Approval Needed For |
|------|---------------------|
| Data Platform | Topic creation, schema changes |
| Data Governance | PII/PHI fields, data contracts |
| InfoSec | ACLs, encryption, sensitive data |
| Architecture | New patterns, system integrations |
| Compliance | LGPD/PCI impacting changes |

---

## Step 5: Risk Assessment

### Risk Categories

```markdown
1. Technical Risks
   - Performance issues
   - Integration failures
   - Data loss scenarios

2. Compliance Risks
   - LGPD/PCI violations
   - LGPD non-compliance
   - Audit failures

3. Operational Risks
   - Deployment failures
   - Rollback complexity
   - Monitoring gaps

4. Business Risks
   - Customer impact
   - Revenue impact
   - Reputation impact
```

### Risk Assessment Template

```markdown
| Risk | Likelihood | Impact | Mitigation | Owner |
|------|------------|--------|------------|-------|
| Schema incompatibility | Medium | High | Use BACKWARD compatibility | Platform |
| Flink job failure | Low | Medium | DLQ + alerting | Data |
| LGPD violation | Low | Critical | PII tokenization | Governance |
```

### Rollback Plan

```markdown
Every plan must include rollback strategy:

1. What to rollback
   - [ ] Schema (registry rollback)
   - [ ] Topic config (Terraform)
   - [ ] Flink job (suspend/delete)
   - [ ] Connectors (pause/delete)

2. How to rollback
   - Commands to execute
   - Order of operations
   - Verification steps

3. When to rollback
   - Error rate > X%
   - Latency > Y ms
   - Data quality < Z%
```

---

## Step 6: Get Approval

### Plan Presentation

```markdown
Present plan to user with:

1. Summary
   - What will be implemented
   - Why this approach was chosen
   - Estimated effort

2. Todo List
   - All tasks in order
   - Dependencies marked
   - Estimated completion

3. Architecture
   - Diagrams (if applicable)
   - Key decisions
   - Integration points

4. Risks
   - Identified risks
   - Mitigation strategies
   - Rollback plan

5. Questions
   - Any clarifications needed
   - Approval request
```

### Approval Questions

```markdown
Use AskUserQuestion tool for:

- Approach selection (when multiple valid options)
- Scope clarification
- Priority decisions
- Timeline expectations

Example:
"I have two approaches for the fraud detection:
A) Single Flink job with all patterns (simpler, less flexible)
B) Separate jobs per pattern (complex, more maintainable)
Which approach do you prefer?"
```

### Exit Plan Mode

```markdown
When plan is complete:
1. Summarize the plan
2. List all todos
3. Confirm approach
4. Use ExitPlanMode tool
5. Wait for user approval

User can:
- Approve → Proceed to ACT
- Request changes → Revise plan
- Reject → Return to RESEARCH or cancel
```

---

## Plan Document Template

```markdown
# Implementation Plan: [Feature Name]

**Date**: YYYY-MM-DD
**Author**: Claude Code
**Status**: Draft | Approved | In Progress | Complete

## Executive Summary
[2-3 sentences describing what will be implemented and why]

## Background
[Context from RESEARCH phase, why this is needed]

## Scope

### In Scope
- [Item 1]
- [Item 2]

### Out of Scope
- [Item 1]
- [Item 2]

## Technical Design

### Architecture
[Diagram or description]

### Components
| Component | Purpose | Technology |
|-----------|---------|------------|
| ... | ... | ... |

### Data Flow
[Description or diagram]

## Implementation Plan

### Todo List
| # | Task | Depends On | Est. Time |
|---|------|------------|-----------|
| 1 | ... | - | 30m |
| 2 | ... | 1 | 45m |

### Timeline
- Phase 1: [Tasks 1-5] - Day 1
- Phase 2: [Tasks 6-10] - Day 2
- Phase 3: [Tasks 11-15] - Day 3

## Dependencies

### Technical
- [Dependency 1]

### Team
- [Team approval needed]

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| ... | ... | ... | ... |

## Rollback Plan
[How to undo changes if needed]

## Compliance Checklist
- [ ] LGPD/PCI requirements met
- [ ] LGPD requirements met
- [ ] PII properly handled
- [ ] Audit trail maintained

## Approval

- [ ] Technical approach approved
- [ ] Scope confirmed
- [ ] Timeline accepted
- [ ] Ready to proceed to ACT phase

---
**Plan Phase Complete** ✓
Awaiting approval to proceed.
```

---

## Skills Used in PLAN Phase

| Skill | Purpose |
|-------|---------|
| `diagram-generator` | Create architecture diagrams |
| `doc-generator` | Create plan documentation |
| `validation` | Validate approach against standards |

---

## Claude Code Tools in PLAN Phase

| Tool | Purpose |
|------|---------|
| `EnterPlanMode` | Start planning mode |
| `TodoWrite` | Create structured todo list |
| `AskUserQuestion` | Clarify requirements |
| `ExitPlanMode` | Submit plan for approval |

---

## Transition to ACT Phase

### Plan Approval Checklist

- [ ] Todo list created and ordered
- [ ] Architecture documented
- [ ] Dependencies identified
- [ ] Risks assessed
- [ ] Rollback plan defined
- [ ] Compliance checked
- [ ] User approval received

### Transition Command

```
"Plan approved. Proceeding to ACT phase.
Starting with: [First todo item]"
```

---

## Examples

### Example 1: Plan Kafka Topic Deployment

```markdown
User: "Plan deploying a new Kafka topic for transactions"

PLAN Phase:
1. Enter plan mode
2. Create todo list:
   - Design topic configuration
   - Create Avro schema
   - Write Terraform code
   - Create data contract
   - Configure ACLs
   - Update documentation
3. Document decisions:
   - 12 partitions (10K msgs/sec / 1K per partition)
   - 30-day retention (lab budget)
   - Dedicated cluster (compliance)
4. Identify dependencies:
   - Data Governance approval for PII fields
   - InfoSec approval for ACLs
5. Risk assessment:
   - Schema compatibility risk → BACKWARD mode
6. Present plan, get approval

Output: Approved plan ready for ACT phase
```

### Example 2: Plan Schema Evolution

```markdown
User: "Plan evolving the transaction schema to add new field"

PLAN Phase:
1. Enter plan mode
2. Create todo list:
   - Analyze current schema
   - Design schema change
   - Test compatibility
   - Update consumers
   - Update producers
   - Deploy schema
3. Document decisions:
   - Add optional field (BACKWARD compatible)
   - Default value for new field
4. Dependencies:
   - Consumer team must update before producer
5. Risk assessment:
   - Consumer failure → Add default value

Output: Approved evolution plan
```

---

**Version**: 1.0
**Last Updated**: 2026-01-27
**Phase**: PLAN (2 of 4)
