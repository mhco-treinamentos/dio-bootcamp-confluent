#!/bin/bash
# Hook: PostToolUse (Edit|Write *.sql)
# Validate SQL files follow project standards (UPPERCASE keywords).
# Covers ANSI SQL, Flink SQL, and Snowflake/Trino specifics used by the workshop.
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ] || [[ ! "$FILE_PATH" =~ \.sql$ ]]; then
  exit 0
fi

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

# Common lowercase SQL keywords that should be UPPERCASE (soft check - warns only)
LOWERCASE_KEYWORDS=$(grep -n -E '\b(select|from|where|group by|order by|having|join|left|right|inner|outer|create|table|insert|update|delete|alter|drop|as|on|and|or|not|in|between|like|is|null|case|when|then|else|end|with|union|except|intersect|interval|partition by|window|over|lateral|watermark|tumble|hop|session|match_recognize|pattern|within|copy into|merge|stream|task)\b' "$FILE_PATH" 2>/dev/null | head -5)

if [ -n "$LOWERCASE_KEYWORDS" ]; then
  echo "Warning: Found lowercase SQL keywords (project standard requires UPPERCASE):" >&2
  echo "$LOWERCASE_KEYWORDS" >&2
fi

exit 0
