#!/bin/bash
# Hook: PostToolUse (Edit|Write *.py)
# Auto-lint Python files with ruff after edits
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ] || [[ ! "$FILE_PATH" =~ \.py$ ]]; then
  exit 0
fi

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 0

if ! command -v ruff &> /dev/null; then
  exit 0
fi

ruff check "$FILE_PATH" --fix --quiet 2>/dev/null || true
ruff format "$FILE_PATH" --quiet 2>/dev/null || true

exit 0
