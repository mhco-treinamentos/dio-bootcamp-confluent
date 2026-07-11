# Instruction: setup do Confluent Cloud (Aula 0)

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para o setup da Aula 0: conta Confluent Cloud, CLI `confluent`, environment + cluster, API keys, service account least-privilege, plugin flink quickstart e teardown. 100% cloud — o fallback local vive em [skills/confluent/local-dev-fallback.md](../skills/confluent/local-dev-fallback.md).**

## Pré-requisitos

- Conta Confluent Cloud com créditos gratuitos ativos (US$ 400 / 30 dias)
- CLI `confluent` v4+ instalado (ver Phase 3.1)
- `jq` instalado para parsear outputs JSON: `brew install jq`
- Diretório alvo: `repo/labs/modulo-02-plataforma/` guarda os scripts da Aula 0 (`setup.sh` / `teardown.sh`)

---

## Phase 1: RESEARCH

### 1.1 — Coletar requisitos
- Quais módulos vão usar este environment? (Todos — o environment e o cluster Basic da Aula 0 são compartilhados entre labs)
- Cloud/região? Default do bootcamp: AWS `us-east-1` (menor latência para Tableflow/catálogos nos módulos 5–6)
- Tipo de cluster? **Basic** — suficiente para todos os labs; Freight/Dedicated são conteúdo teórico do módulo 7, não se cria
- Throughput esperado? (Labs < 1k msg/s — sem tuning a fazer)

### 1.2 — Revisar ambiente
- Já existe environment/cluster de execuções anteriores? `confluent environment list` — não duplicar
- Créditos restantes: Console → Billing & payment (checar ANTES de criar compute pool Flink)

### 1.3 — Consultar references
- [references/bootcamp-stack-guide.md](../references/bootcamp-stack-guide.md) — decisões de stack
- [skills/confluent/cloud-setup.md](../skills/confluent/cloud-setup.md) — skill par desta instruction
- [.claude/rules.md](../rules.md) — naming, env vars `CC_*`/`SR_*`, defaults de confiabilidade

---

## Phase 2: PLAN

### 2.1 — Recursos a criar (nesta ordem)

| # | Recurso | Nome | Observação |
|---|---------|------|------------|
| 1 | Environment | `bootcamp-dev` | Com Stream Governance Essentials (SR incluso) |
| 2 | Cluster | `bootcamp-basic` | Basic, single-zone |
| 3 | Service account | `sa-bootcamp-labs` | Least privilege — NUNCA usar a conta de admin nos producers |
| 4 | API key (cluster) | — | Vinculada ao service account |
| 5 | API key (Schema Registry) | — | Vinculada ao service account |
| 6 | ACLs | — | Mínimas: `WRITE`/`READ` nos prefixos `dev.payments.*` e `lab.payments.*` |

Compute pool Flink NÃO entra na Aula 0 — é criado (e destruído) pelo lab do módulo 4.

### 2.2 — Topic naming (ver [.claude/rules.md](../rules.md))

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}

Exemplos:
dev.payments.gen.transactions.created.v1
dev.payments.pg-payments.transactions.cdc.v1
dev.payments.flink.fraud.detected.v1
```

### 2.3 — Env vars resultantes (`.env`, nunca commitado)

```
CC_ENV_ID=env-xxxxx
CC_CLUSTER_ID=lkc-xxxxx
CC_BOOTSTRAP=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
CC_API_KEY=...
CC_API_SECRET=...
SR_URL=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
SR_API_KEY=...
SR_API_SECRET=...
```

Fornecer `.env.example` sem valores; `.env` no `.gitignore`.

---

## Phase 3: ACT

### 3.1 — Instalar o CLI e fazer login

```bash
# macOS
brew install confluentinc/tap/cli

# Verificar versão (labs assumem v4+)
confluent version

# Login (abre o browser)
confluent login --save
```

### 3.2 — Criar environment e cluster

```bash
# Environment com Stream Governance Essentials
confluent environment create bootcamp-dev \
  --governance-package essentials -o json | jq -r '.id'
# Guarde o id: env-xxxxx

confluent environment use env-xxxxx

# Cluster Basic
confluent kafka cluster create bootcamp-basic \
  --cloud aws --region us-east-1 --type basic -o json | jq -r '.id'
# Guarde o id: lkc-xxxxx

confluent kafka cluster use lkc-xxxxx

# Bootstrap endpoint (vai para CC_BOOTSTRAP)
confluent kafka cluster describe lkc-xxxxx -o json | jq -r '.endpoint'
```

### 3.3 — Service account least-privilege

```bash
confluent iam service-account create sa-bootcamp-labs \
  --description "Producers/consumers dos labs do bootcamp" -o json | jq -r '.id'
# Guarde o id: sa-xxxxx

# ACLs mínimas: ler/escrever apenas nos prefixos dos labs
for op in WRITE READ DESCRIBE; do
  confluent kafka acl create --allow --service-account sa-xxxxx \
    --operations "$op" --topic "dev.payments." --prefix --cluster lkc-xxxxx
  confluent kafka acl create --allow --service-account sa-xxxxx \
    --operations "$op" --topic "lab.payments." --prefix --cluster lkc-xxxxx
done

# Consumer groups dos labs
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations READ --consumer-group "bootcamp-" --prefix --cluster lkc-xxxxx
```

### 3.4 — API keys (cluster + Schema Registry)

```bash
# Key do cluster, dona = service account (vai para CC_API_KEY/CC_API_SECRET)
confluent api-key create --resource lkc-xxxxx --service-account sa-xxxxx -o json

# Endpoint e key do Schema Registry (vai para SR_URL/SR_API_KEY/SR_API_SECRET)
confluent schema-registry cluster describe -o json | jq -r '.endpoint_url'
confluent api-key create --resource <sr-cluster-id> --service-account sa-xxxxx -o json
```

> **Atenção**: o secret aparece **uma única vez**. Cole imediatamente no `.env` — nunca em arquivo commitado, nunca no histórico do shell de gravação de aula.

### 3.5 — Plugin flink quickstart (usado no módulo 4)

```bash
# Instala o plugin de quickstart do Flink
confluent plugin install confluent-flink-quickstart

# NÃO rodar o quickstart agora — ele cria compute pool (custo).
# O lab do módulo 4 roda:
#   confluent flink quickstart --name bootcamp-flink --max-cfu 5 \
#     --region us-east-1 --cloud aws
```

### 3.6 — Persistir em `setup.sh` + `.env.example`

Consolidar os passos 3.2–3.4 em `repo/labs/modulo-02-plataforma/setup.sh` idempotente (checa se o recurso existe antes de criar) e gerar `.env.example`. Todo `setup.sh` tem um `teardown.sh` par (ver Cleanup).

---

## Phase 4: VALIDATE

### 4.1 — CLI autenticado e contexto correto
```bash
confluent environment list
# Esperado: bootcamp-dev com "*" (em uso)

confluent kafka cluster list
# Esperado: bootcamp-basic com status UP e "*"
```

### 4.2 — Smoke test: primeiro topic + produce/consume
```bash
confluent kafka topic create dev.payments.gen.smoke.test.v1 --partitions 1 --cluster lkc-xxxxx

# Produzir com a API key do service account (valida as ACLs)
echo '{"hello":"bootcamp"}' | confluent kafka topic produce dev.payments.gen.smoke.test.v1 \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" --cluster lkc-xxxxx

confluent kafka topic consume dev.payments.gen.smoke.test.v1 -b --cluster lkc-xxxxx
# Esperado: {"hello":"bootcamp"}

# Limpar o topic de smoke test
confluent kafka topic delete dev.payments.gen.smoke.test.v1 --cluster lkc-xxxxx --force
```

### 4.3 — Schema Registry acessível
```bash
curl -s -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects"
# Esperado: []
```

### 4.4 — Least privilege de verdade
```bash
# A key do service account NÃO pode criar topic fora do prefixo permitido
echo x | confluent kafka topic produce forbidden.topic \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" --cluster lkc-xxxxx
# Esperado: erro de autorização (TopicAuthorizationFailed)
```

### 4.5 — `.env` fora do git
```bash
git check-ignore repo/labs/modulo-02-plataforma/.env
# Esperado: o path é ecoado (está ignorado)
```

---

## Cleanup (teardown.sh — disciplina de créditos)

`repo/labs/modulo-02-plataforma/teardown.sh`, na ordem inversa da criação:

```bash
# Teardown leve (fim de um dia de gravação): remove topics de trabalho
confluent kafka topic list --cluster lkc-xxxxx -o json | \
  jq -r '.[].name | select(startswith("dev.") or startswith("lab."))' | \
  xargs -I{} confluent kafka topic delete {} --cluster lkc-xxxxx --force

# Teardown completo (fim do bootcamp / créditos no limite) — DESTRÓI TUDO
confluent api-key list --service-account sa-xxxxx -o json | \
  jq -r '.[].key' | xargs -I{} confluent api-key delete {} --force
confluent iam service-account delete sa-xxxxx --force
confluent kafka cluster delete lkc-xxxxx --environment env-xxxxx --force
confluent environment delete env-xxxxx --force
# Tempo esperado: ~2 min

# Verificação pós-teardown (nada rodando = nada cobrando)
confluent environment list
```

> Cluster Basic idle custa pouco, mas conectores, compute pools Flink e Tableflow cobram por hora — o teardown de cada lab (módulos 3–7) destrói esses recursos; este teardown da Aula 0 é o "botão de desligar geral".

---

## Checklist final

- [ ] CLI `confluent` v4+ instalado e logado
- [ ] Environment `bootcamp-dev` com Stream Governance Essentials
- [ ] Cluster Basic `bootcamp-basic` UP
- [ ] Service account `sa-bootcamp-labs` com ACLs mínimas (prefixos `dev.payments.`/`lab.payments.`)
- [ ] API keys (cluster + SR) criadas para o service account e salvas só no `.env`
- [ ] Plugin flink quickstart instalado (sem compute pool criado)
- [ ] Smoke test produce/consume passou; teste de autorização negado passou
- [ ] `.env` no `.gitignore`; `.env.example` commitável sem valores
- [ ] `setup.sh` + `teardown.sh` pares e idempotentes
- [ ] CHANGELOG atualizado

---

## Exemplo de uso

```
"Siga cloud-setup para criar o environment e o cluster da Aula 0,
com service account least-privilege e teardown.sh completo"
```

---

**Última atualização**: 2026-07-11
