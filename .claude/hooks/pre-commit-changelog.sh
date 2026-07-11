#!/bin/bash
# Hook: PreToolUse (Bash git commit*)
# Block commits that modify code without updating CHANGELOG.md.
# Workshop rule: every code change MUST have a CHANGELOG entry (see .claude/rules.md).
# Docs-only changes (markdown READMEs, scene docs) are exempt — those are covered by git log.
set -e

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if ! echo "$COMMAND" | grep -qE "git commit\b"; then
  exit 0
fi

if echo "$COMMAND" | grep -q -- "--no-edit"; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 0

STAGED_FILES=$(git diff --cached --name-only 2>/dev/null || echo "")

if [ -z "$STAGED_FILES" ]; then
  exit 0
fi

# Files that count as "code" for the CHANGELOG requirement.
# Includes Terraform (.tf), Avro schemas (.avsc), Docker/K8s manifests, Flink SQL (.sql), Python (.py).
CODE_FILES=$(echo "$STAGED_FILES" | grep -E '\.(py|sql|tf|tfvars|avsc|yaml|yml|json)$' | grep -vE '(settings\.local\.json|\.lock$)' || true)

if [ -z "$CODE_FILES" ]; then
  exit 0
fi

if echo "$STAGED_FILES" | grep -q "CHANGELOG.md"; then
  exit 0
fi

CHANGED_LIST=$(echo "$CODE_FILES" | head -5 | tr '\n' ', ' | sed 's/,$//')

jq -n --arg files "$CHANGED_LIST" '{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": ("CHANGELOG.md must be updated before committing code changes. Staged code files: " + $files + ". Add an entry under ## [Unreleased] in CHANGELOG.md first.")
  }
}'
exit 0
