#!/bin/bash
# Hook: PostToolUse (Edit|Write *.avsc, *.avro)
# Block Avro schemas that declare sensitive-named fields without the required
# confluent:tags. Enforces the PII/PCI tagging policy from .claude/rules/schemas.md
# (payments domain: LGPD personal data + PCI-DSS card data + banking identifiers).
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [ -z "$FILE_PATH" ] || [[ ! "$FILE_PATH" =~ \.(avsc|avro)$ ]]; then
  exit 0
fi

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

if ! command -v jq &> /dev/null; then
  exit 0
fi

# Extract every field (flat + nested) whose name matches a sensitive pattern and lacks confluent:tags.
# Sensitive name patterns (payments domain):
#   PII (LGPD): cpf, cnpj, rg, email, telefone, phone, endereco, address, nome, name, cep,
#               data_nascimento, birth_date, lat, lon, latitude, longitude
#   PCI-DSS:    pan, card_number, cvv, cvc, pin, magnetic_stripe, track_data
#   Banking:    account_number, conta, agencia, branch, iban, swift
SENSITIVE_REGEX='^(cpf|cnpj|rg|email|telefone|phone|endereco|address|nome|name|cep|data_nascimento|birth_date|lat|lon|latitude|longitude|pan|pan_full|card_number|cvv|cvc|pin|magnetic_stripe|track_data|account_number|numero_conta|conta|agencia|branch|iban|swift)$'

VIOLATIONS=$(jq -r --arg re "$SENSITIVE_REGEX" '
  def walk_fields(prefix):
    (.fields // [])[]
    | (prefix + "." + .name) as $full
    | (if (.name | test($re; "i")) and ((."confluent:tags" // []) | length == 0)
       then $full
       else empty
       end),
      (if (.type | type == "object") then (.type | walk_fields($full)) else empty end);
  walk_fields(.name // "root")
' "$FILE_PATH" 2>/dev/null || echo "")

if [ -z "$VIOLATIONS" ]; then
  exit 0
fi

VIOLATION_LIST=$(echo "$VIOLATIONS" | head -10 | tr '\n' ', ' | sed 's/,$//')

jq -n --arg fields "$VIOLATION_LIST" --arg file "$FILE_PATH" '{
  "hookSpecificOutput": {
    "hookEventName": "PostToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": ("Avro schema " + $file + " has sensitive fields missing confluent:tags (PII/PCI policy in .claude/rules/schemas.md). Untagged: " + $fields + ". Add confluent:tags: [\"PII\",\"LGPD\"] for personal data, [\"PCI\",\"PCI_DSS\"] for card data, or [\"PII\",\"LGPD\",\"FIN\"] for banking identifiers.")
  }
}'
exit 0
