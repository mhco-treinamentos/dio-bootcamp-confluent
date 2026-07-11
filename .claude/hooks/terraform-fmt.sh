#!/bin/bash
# Hook: PostToolUse (Edit|Write *.tf, *.tfvars)
# Auto-format Terraform files with `terraform fmt` and run `terraform validate` on the module directory.
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ]; then
  exit 0
fi

if [[ ! "$FILE_PATH" =~ \.(tf|tfvars)$ ]]; then
  exit 0
fi

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 0

if ! command -v terraform &> /dev/null; then
  exit 0
fi

# Format the specific file (non-blocking)
terraform fmt "$FILE_PATH" > /dev/null 2>&1 || true

# Validate the containing module (non-blocking warning)
MODULE_DIR=$(dirname "$FILE_PATH")
if [ -d "$MODULE_DIR/.terraform" ]; then
  VALIDATE_OUTPUT=$(terraform -chdir="$MODULE_DIR" validate -no-color 2>&1 || true)
  if echo "$VALIDATE_OUTPUT" | grep -qE "Error:"; then
    echo "Warning: terraform validate reported errors in $MODULE_DIR:" >&2
    echo "$VALIDATE_OUTPUT" | head -20 >&2
  fi
fi

exit 0
