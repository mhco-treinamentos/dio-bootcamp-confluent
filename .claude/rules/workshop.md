---
globs: "**/ws-*/**,**/workshop*/**,**/samples/**"
---

# Workshop Rules

> **Pedagogical standards for hands-on workshops: verifiable steps, time-boxed scenes, and recovery commands.**

Applies to anything under `ws-*/`, `workshop*/`, or `samples/` directories. Workshops are **teaching artifacts** — correctness, reproducibility, and explicit failure modes matter more than elegance.

## Structure

Every workshop has three mandatory sections in its README:
1. **Pre-requisites** — exact versions, RAM, disk, network access
2. **Scene-by-scene walkthrough** — numbered scenes, each with a clear outcome
3. **Troubleshooting** — top 5–10 failure modes with copy-pasteable fixes

## Scene Design

Each scene MUST include:
- **Goal** (one sentence — what the attendee will see working)
- **Commands** (copy-pasteable, in order)
- **Expected output** (a real snippet, not "you should see something")
- **Validation query** (a `SELECT` or `curl` that proves the scene succeeded)
- **Cleanup** (how to undo this scene if the attendee gets stuck)

## Time-Boxing

| Scene type | Max duration |
|------------|--------------|
| Setup / bootstrap | 10 min |
| Conceptual scene | 15 min |
| Hands-on exercise | 25 min |
| Integration end-to-end | 30 min |

If a scene exceeds its budget, split it. Attendees lose engagement after ~30 minutes of a single task.

## Verifiability

Every step must be independently verifiable — no "trust me, it works". Use:
- `curl localhost:<port>/health` for services
- `SELECT COUNT(*)` for data arrival
- `docker compose ps` for container state
- `docker compose logs <service> | grep <expected-marker>` for readiness signals

Example readiness check from the Hospital workshop:
```bash
docker compose logs -f oracle | grep -E "DATABASE IS READY|Completed|ERROR|ORA-[0-9]"
```

## Commands: Copy-Pasteable

- Use full paths or relative paths from the workshop root — never "cd into some directory"
- Show `$` or `#` prompts only when distinguishing user vs root
- For heredocs, use quoted delimiters (`<<'EOF'`) to prevent shell expansion surprises
- Annotate dangerous commands (`docker compose down -v` destroys data)

## Cleanup & Recovery

Every workshop MUST have:
- **Full reset**: `docker compose down -v && docker compose up -d`
- **Partial reset per scene**: targeted cleanup (drop a topic, remove a connector)
- **Expected reset duration** (e.g., "full reset ~5 min")

## Version Pinning

Workshop samples MUST pin every external version explicitly — no `latest` tags, no floating versions:
```yaml
# GOOD
image: flink:1.19.1-scala_2.12-java11

# BAD — breaks when upstream releases a new major
image: flink:latest
```

Document the version matrix in a dedicated section, especially when compatibility is narrow (e.g., Flink 1.19 + Java 11 + iceberg-flink-runtime 1.5.2).

## Resource Sizing

State minimum RAM, CPU, and disk up front:
```markdown
## Requirements
- 12 GB free RAM (Oracle XE alone needs 3.5 GB)
- 20 GB free disk
- Docker Desktop with memory limit ≥ 10 GB
```

## Troubleshooting Sections

Structure:
```markdown
### <Symptom the attendee sees>
# What the attendee pastes to diagnose
<diagnostic command>
# Expected output for healthy state
<healthy marker>
# Fix (most common cause first)
<fix command>
```

Don't write exhaustive theory — give them the fix. Link to deeper docs for attendees who want to learn more.

## Data Realism

Workshop data MUST be:
- **Synthetic** — no real PII/PHI, even "anonymized"
- **Domain-plausible** — CIDs, TISS codes, CPFs must look real even if fake
- **Deterministic with a seed** — `seed=42` so every attendee sees identical data on scene N
- **Bounded** — workshops shouldn't generate >10k records per scene; attendees wait

## Pedagogical Anti-Patterns

Avoid:
- ❌ "As an exercise, figure out X" — workshop has a time budget; show the answer
- ❌ Undocumented side effects (`docker compose up` that silently creates a volume elsewhere)
- ❌ Magic ports with no explanation
- ❌ Screenshots instead of commands (attendees can't grep screenshots)
- ❌ "Here's a 2000-line `docker-compose.yml`" without section-by-section commentary
- ❌ "Restart Docker Desktop" as a real fix — diagnose the real problem

## Language

Workshops for Brazilian teams should be in **pt-BR** in the prose and **English** in the code (identifiers, log messages, table names). Mixed-language codebases are brittle; prose can be translated without breaking anything.

## Cross-References

When teaching a topic the project's production platform handles differently, note the delta:
> "The workshop uses Nessie as the Iceberg catalog because it's a single container. In production scenarios you might use AWS Glue or Hive Metastore — see [references/workshop-stack-guide.md](../references/workshop-stack-guide.md) for the mapping."

This saves attendees confusion when they return to production work.
