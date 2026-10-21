#!/usr/bin/env bash
# Teardown do ambiente do bootcamp no Confluent Cloud.
#
# Uso:
#   ./teardown.sh              # só relata o que existe (dry-run)
#   ./teardown.sh --execute    # destrói de verdade
#
# ⚠️ Com --execute, isto APAGA o environment inteiro e tudo dentro dele.
# Só rode ao final do bootcamp — o tabuleiro do Curso 2 é a base dos Cursos 3 a 7.
#
# Para o ritual de FIM DE SESSÃO (que você faz todo dia), você não precisa deste
# script: basta conferir que não há conector nem compute pool ligado. Cluster
# Basic ocioso não queima crédito relevante.

set -euo pipefail

ENV_NAME="${ENV_NAME:-bootcamp-dio}"
EXECUTE="${1:-}"

say() { printf '\n\033[1m%s\033[0m\n' "$1"; }

say "Inventário do ambiente '$ENV_NAME'"

ENV_ID=$(confluent environment list -o json 2>/dev/null \
  | grep -B2 "\"name\": \"$ENV_NAME\"" | grep '"id"' | head -1 \
  | sed 's/.*: "\(.*\)".*/\1/')

if [ -z "$ENV_ID" ]; then
  echo "Environment '$ENV_NAME' não encontrado — nada a fazer."
  exit 0
fi

echo "environment: $ENV_ID ($ENV_NAME)"
confluent environment use "$ENV_ID" >/dev/null

# --- o que cobra por existir (o que REALMENTE importa) ---
say "Recursos que cobram por hora ligada"
echo "conectores:"
confluent connect cluster list 2>&1 | tail -n +1 | sed 's/^/  /'
echo "compute pools Flink:"
confluent flink compute-pool list 2>&1 | tail -n +1 | sed 's/^/  /'

# --- o que cobra por uso (ok deixar parado) ---
say "Recursos que cobram por uso"
confluent kafka cluster list 2>&1 | sed 's/^/  /'

if [ "$EXECUTE" != "--execute" ]; then
  say "Dry-run — nada foi apagado"
  echo "Para destruir o environment inteiro: $0 --execute"
  exit 0
fi

say "⚠️  APAGANDO o environment $ENV_ID e TUDO dentro dele"
read -r -p "Digite o nome do environment para confirmar: " CONFIRM
if [ "$CONFIRM" != "$ENV_NAME" ]; then
  echo "Confirmação não bateu. Abortado."
  exit 1
fi

confluent environment delete "$ENV_ID" --force
say "Environment apagado."

echo
echo "Service accounts NÃO pertencem ao environment — confira e apague à parte:"
confluent iam service-account list 2>&1 | sed 's/^/  /'
echo
echo "Consumo recente:"
confluent billing cost list \
  --start-date "$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d '7 days ago' +%Y-%m-%d)" \
  --end-date "$(date +%Y-%m-%d)" 2>&1 | tail -5 | sed 's/^/  /'
