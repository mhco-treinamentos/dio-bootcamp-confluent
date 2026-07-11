# Quick Tasks Workflow

> **For simple tasks that don't require full RESEARCH → PLAN → ACT → VALIDATE**

## When to Use

Use quick workflow when:
- Task is straightforward (< 3 steps)
- No architectural decisions needed
- Changes are localized
- Low risk of breaking existing functionality

## Examples

### Add a Field to Generator
1. Identify generator file
2. Add field to `_generate_single()`
3. Update schema if needed
4. Run existing tests

### Fix a Bug
1. Identify the issue
2. Apply fix
3. Verify with test
4. Done

### Update Configuration
1. Locate config file
2. Make change
3. Verify service starts
4. Done

## When NOT to Use

Switch to full workflow when:
- Multiple files affected
- New entity or domain
- Architectural changes
- User approval needed
