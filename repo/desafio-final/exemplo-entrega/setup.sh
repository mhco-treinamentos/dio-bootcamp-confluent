#!/usr/bin/env bash
# Setup do desafio — cria tudo, na ordem, com echo do que está criando.
# Pré-requisito: .env preenchido (cp .env.example .env) e `confluent login --save`.
set -euo pipefail

source .env

echo "==> Contexto"
confluent environment use "$CC_ENV"
confluent kafka cluster use "$CC_CLUSTER"

echo "==> Tópicos"
for t in desafio.transactions.enriched desafio.fraud.detected; do
  confluent kafka topic list -o json | grep -q "\"$t\"" \
    || confluent kafka topic create "$t" --partitions 3
done

echo "==> Conector CDC (config sem secrets — env vars expandidas aqui)"
envsubst < connectors/cdc.json > /tmp/cdc-resolved.json
confluent connect cluster create --config-file /tmp/cdc-resolved.json
rm -f /tmp/cdc-resolved.json

echo "==> Statements Flink"
echo "    (submeta os arquivos de sql/ pelo workspace ou 'confluent flink statement create')"

echo "==> Validação"
confluent connect cluster list
confluent kafka topic list | grep desafio
echo "OK: setup concluído. Lembre: conector ligado = crédito queimando. Rode ./teardown.sh ao fim da sessão."
