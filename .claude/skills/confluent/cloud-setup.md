# Cloud Setup (Aula 0)

## Skill: `/confluent-cloud-setup`

Setup completo do Confluent Cloud para o bootcamp (Aula 0): login na CLI, environment, cluster, API keys (cluster + Schema Registry), service accounts com least privilege, plugin `flink quickstart` e — obrigatório — o padrão de **teardown** para não queimar os créditos gratuitos (**US$ 400 / 30 dias**).

## Usage

```
/confluent-cloud-setup <action>
```

### Actions
- `login` — Instalar CLI e autenticar
- `environment` — Criar environment + cluster
- `keys` — API keys de cluster e Schema Registry
- `service-account` — Service account por lab com ACLs mínimas
- `flink` — Plugin flink quickstart (compute pool)
- `teardown` — Script de teardown do lab

---

## 1. CLI e login

```bash
# Instalar a CLI confluent (macOS)
brew install confluentinc/tap/cli

# Ou script oficial (Linux/macOS)
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest

confluent version          # v4.x
confluent login            # abre browser; conta criada em confluent.cloud com créditos gratuitos
```

## 2. Environment + cluster

```bash
# Environment do bootcamp (Stream Governance Essentials vem incluso)
confluent environment create bootcamp-dev
confluent environment list
confluent environment use <env-id>            # env-xxxxx

# Cluster Basic (mais barato; suficiente para todos os labs)
confluent kafka cluster create bootcamp-cluster \
  --cloud aws \
  --region us-east-1 \
  --type basic
confluent kafka cluster list
confluent kafka cluster use <cluster-id>      # lkc-xxxxx

# Endpoint do bootstrap (vai para CC_BOOTSTRAP no .env)
confluent kafka cluster describe <cluster-id>
```

> **Basic vs. Standard vs. Freight**: labs usam **Basic** (paga por uso, sem custo fixo por hora ociosa de CKU). Freight/Dedicated são discutidos no módulo 7 (custos) — não criar nos labs.

## 3. API keys

### Cluster Kafka

```bash
# API key do ALUNO para exploração manual (não usar em producers de lab!)
confluent api-key create --resource <cluster-id> --description "bootcamp manual"
confluent api-key use <api-key> --resource <cluster-id>
```

### Schema Registry

```bash
# O SR é habilitado por environment; descobrir o cluster SR e o endpoint
confluent schema-registry cluster describe    # id lsrc-xxxxx + endpoint (SR_URL)

# API key do SR
confluent api-key create --resource <sr-cluster-id> --description "bootcamp sr"
```

### .env do lab (nunca commitado — `.gitignore`)

```bash
# .env.example (commitar SEM valores)
KAFKA_PROFILE=cloud
CC_BOOTSTRAP=<bootstrap-endpoint>       # pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
CC_API_KEY=
CC_API_SECRET=
SR_URL=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
SR_API_KEY=
SR_API_SECRET=
```

## 4. Service accounts (least privilege)

Um service account **por lab**, com ACLs mínimas — nunca usar a API key de admin nos producers:

```bash
# Criar SA do lab
confluent iam service-account create sa-lab-modulo-02 \
  --description "Producer do lab modulo-02 (primeira mensagem + SR)"
confluent iam service-account list           # sa-xxxxx

# ACLs mínimas: WRITE + DESCRIBE apenas no prefixo do lab
confluent kafka acl create --allow \
  --service-account <sa-id> \
  --operations write,describe \
  --topic "dev.payments." --prefix

# Consumer (quando o lab tiver): READ no topic + no consumer group
confluent kafka acl create --allow \
  --service-account <sa-id> \
  --operations read \
  --topic "dev.payments." --prefix
confluent kafka acl create --allow \
  --service-account <sa-id> \
  --operations read \
  --consumer-group "bootcamp-" --prefix

# API key AMARRADA ao service account (esta vai no .env do lab)
confluent api-key create --resource <cluster-id> --service-account <sa-id>

# Conferir
confluent kafka acl list --service-account <sa-id>
```

## 5. Flink quickstart (módulo 4)

```bash
# Plugin oficial da CLI: cria compute pool e abre o shell SQL em um comando
confluent plugin install confluent-flink-quickstart

confluent flink quickstart \
  --name bootcamp-flink \
  --max-cfu 10 \
  --region us-east-1 \
  --cloud aws

# Depois do quickstart, o dia a dia é:
confluent flink compute-pool list
confluent flink shell --compute-pool <pool-id>
```

> `--max-cfu 10` é teto de proteção de créditos: o pool escala a zero quando ocioso, mas statements esquecidos rodando continuam cobrando. Ver teardown.

## 6. Teardown — OBRIGATÓRIO (disciplina de créditos)

**Todo script de setup de lab tem um `teardown.sh` par.** Créditos gratuitos (US$ 400/30 dias) acabam rápido com recursos ociosos — em especial conectores e statements Flink.

```bash
#!/usr/bin/env bash
# teardown.sh — padrão do bootcamp: derrubar TUDO que cobra, na ordem inversa do setup
set -euo pipefail

ENV_ID="${1:?uso: ./teardown.sh <env-id>}"
confluent environment use "$ENV_ID"

# 1. Statements Flink (cobram enquanto RUNNING)
for st in $(confluent flink statement list -o json | jq -r '.[].name'); do
  confluent flink statement delete "$st" --force
done

# 2. Compute pools
for pool in $(confluent flink compute-pool list -o json | jq -r '.[].id'); do
  confluent flink compute-pool delete "$pool" --force
done

# 3. Conectores gerenciados (cobram por task-hora + throughput)
for lcc in $(confluent connect cluster list -o json | jq -r '.[].id'); do
  confluent connect cluster delete "$lcc" --force
done

# 4. Clusters Kafka (Basic sem tráfego custa pouco, mas zere mesmo assim)
for lkc in $(confluent kafka cluster list -o json | jq -r '.[].id'); do
  confluent kafka cluster delete "$lkc" --force
done

# 5. Environment inteiro (remove SR, tags, tudo) — só no fim do bootcamp
# confluent environment delete "$ENV_ID" --force

echo "Teardown concluído. Confira o billing: https://confluent.cloud/settings/billing"
```

Checklist de fim de lab:

- [ ] `confluent flink statement list` vazio
- [ ] `confluent flink compute-pool list` vazio
- [ ] `confluent connect cluster list` vazio
- [ ] Tableflow desabilitado nos topics do lab ([tableflow.md](tableflow.md))
- [ ] Billing conferido na UI (Settings → Billing & payment)

---

## Verificação end-to-end da Aula 0

```bash
confluent kafka topic create dev.payments.gen.hello.created.v1 --partitions 3
echo '{"msg":"hello bootcamp"}' | confluent kafka topic produce dev.payments.gen.hello.created.v1
confluent kafka topic consume dev.payments.gen.hello.created.v1 --from-beginning
confluent kafka topic delete dev.payments.gen.hello.created.v1 --force
```

Se as quatro linhas funcionam, o aluno está pronto para o módulo 2.

---

**Última atualização**: 2026-07-11
