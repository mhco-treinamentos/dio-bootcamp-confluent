#!/bin/bash
# Hook: PostToolUse (Edit|Write *.py)
# Check and auto-format Python files with Black (100 char line length)
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

if ! command -v black &> /dev/null; then
  exit 0
fi

black --line-length 100 --quiet "$FILE_PATH" 2>/dev/null || true

exit 0
