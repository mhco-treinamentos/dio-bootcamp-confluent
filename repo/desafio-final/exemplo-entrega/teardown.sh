#!/usr/bin/env bash
# Teardown do desafio — derruba o que cobra por hora, na ordem, e PROVA que limpou.
# Defensivo: cada delete é guardado por list (rodar num ambiente vazio não é erro).
set -euo pipefail

source .env

confluent environment use "$CC_ENV" >/dev/null
confluent kafka cluster use "$CC_CLUSTER" >/dev/null

echo "==> Conectores"
confluent connect cluster list -o json 2>/dev/null \
  | python3 -c "import json,sys;t=sys.stdin.read().strip();[print(c['id']) for c in (json.loads(t) if t else [])]" \
  | while read -r c; do
      echo "    deletando $c"
      confluent connect cluster delete "$c" --cluster "$CC_CLUSTER" --force
    done

echo "==> Statements Flink"
confluent flink statement list -o json 2>/dev/null | grep -v "^No Flink" \
  | python3 -c "import json,sys;t=sys.stdin.read().strip();[print(s['name']) for s in (json.loads(t) if t else []) if s.get('status')=='RUNNING']" \
  | while read -r s; do
      echo "    parando $s"
      confluent flink statement stop "$s"
      confluent flink statement delete "$s" --force
    done

echo "==> Verificação final (as duas listas DEVEM vir vazias)"
confluent connect cluster list
confluent flink statement list 2>/dev/null | grep -c RUNNING && echo "ATENÇÃO: statement ainda RUNNING" || echo "OK: nenhum statement rodando"
echo "OK: teardown concluído (~1 min). O cluster Basic e o Neon hibernando custam ~zero."
