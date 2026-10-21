# Instruction: criar um novo lab

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para construir um lab hands-on de um módulo do bootcamp cumprindo [rules/labs.md](../rules/labs.md): time-boxing, comandos verificáveis, teardown de recursos cloud.**

## O que é um "lab"
Um lab é a unidade hands-on de um curso do bootcamp, vivendo em `repo/labs/modulo-0X-<slug>/`, com:
- README time-boxed em seções de 10–30 min, uma por conceito da aula
- Par `setup.sh` / `teardown.sh` (todo recurso cloud criado tem destruição correspondente)
- Comandos copy-pasteáveis com output esperado real
- Validação independente por seção (`confluent ... list`, `SELECT COUNT(*)`, `curl`)
- Uma camada do Desafio Final (build-as-you-go)

## Pré-requisitos
- Aula 0 concluída: environment + cluster + API keys (ver [cloud-setup_v1.md](cloud-setup_v1.md))
- Módulo alvo identificado (modulo-02..07) e conceitos da aula mapeados
- Topics / conectores / statements dos quais o lab depende já existem, OU o lab tem seção "Replay rápido"

---

## Phase 1: RESEARCH

### 1.1 — Definir o propósito pedagógico
- O que o aluno vai **aprender** ao final? (Uma frase — vira o Goal do README)
- Quais conceitos pré-requisitos? (Ex.: lab de MATCH_RECOGNIZE precisa que watermarks já tenham sido cobertos no lab anterior)
- Qual é a "moeda de troca" — o aluno troca tempo (e créditos) por que insight?
- Qual camada do Desafio Final este lab constrói?

### 1.2 — Decidir o time budget
Da matriz em [rules/labs.md](../rules/labs.md):
| Tipo de seção | Máx |
|---------------|-----|
| Setup / bootstrap | 10 min |
| Conceitual | 15 min |
| Hands-on | 25 min |
| Integração E2E | 30 min |

Cada aula gravada tem 15–20 min; se o lab passa do orçamento, **divida** em dois labs ou mova conteúdo para o Desafio Final.

### 1.3 — Mapear riscos e custos
- Que falha pode aparecer no ambiente do aluno? (Créditos esgotados, API key sem ACL, região errada, conector em `FAILED`)
- Qual é o failure mode mais provável? Esse vai pro **Troubleshooting**.
- Quanto o lab custa em créditos? Compute pool Flink e conectores gerenciados cobram por hora — dimensionar mínimo (ex.: `--max-cfu 5`) e reforçar o teardown.

### 1.4 — Consultar references e agents
- [references/bootcamp-stack-guide.md](../references/bootcamp-stack-guide.md) — decisões de stack e arquitetura
- [references/cdc-connect-patterns.md](../references/cdc-connect-patterns.md) — se o lab é do módulo 3
- [references/poison-pill-patterns.md](../references/poison-pill-patterns.md) — se o lab ensina DLQ/qualidade de dados
- Agents relevantes (ver [.claude/agents/README.md](../agents/README.md)): `confluent-cloud-expert`, `connect-cdc-expert`, `flink-confluent-expert`, `tableflow-lakehouse-expert`, `realtime-ai-expert`, `observability-cost-expert`, `lab-architect`

---

## Phase 2: PLAN

### 2.1 — Estrutura do lab (template)

```
repo/labs/modulo-0X-<slug>/
├── README.md          # seções time-boxed (template abaixo)
├── setup.sh           # cria os recursos cloud do lab (idempotente)
├── teardown.sh        # destrói TUDO que o setup.sh criou
├── sql/               # statements Flink (se aplicável)
├── configs/           # configs de conector (se aplicável)
└── scripts/           # helpers Python (usam repo/dataset/)
```

### 2.2 — Template de seção do README

```markdown
### Seção N: <Título curto> (X min)
- **Goal**: <uma frase do que vai estar funcionando ao final>
- **Pré-requisito**: <seções/labs anteriores que precisam estar OK>

**Comandos:**
\`\`\`bash
<comando 1>
<comando 2>
\`\`\`

**Output esperado:**
\`\`\`
<snippet real do output, não generalizado>
\`\`\`

**Validação:**
\`\`\`bash
<confluent .../SELECT/curl que confirma sucesso>
\`\`\`

**Cleanup:**
\`\`\`bash
<como reverter SÓ essa seção, sem destruir o lab todo>
\`\`\`
```

### 2.3 — Pensar no roteiro de tempo
- Setup que não dá pra acelerar (provisionamento de conector, compute pool) vai cedo — e o README diz quanto esperar
- Conceitos densos (watermarks, changelog semantics) vão depois do warm-up
- Encerrar com algo "visual" (consume no CLI mostrando fraude detectada, query Iceberg com resultado bonito) — aluno sai satisfeito

### 2.4 — Decidir o domínio dos dados
Sempre payments/fraude. Usar geradores existentes (`repo/dataset/generators/payments/`). Se o lab precisa de dados sem gerador, seguir antes [new-generator_v1.md](new-generator_v1.md).

### 2.5 — Mapa de recursos cloud (vira o teardown.sh)
Listar TODO recurso que o `setup.sh` cria, com custo e ordem de destruição:

| Recurso | Criado por | Custo | Destruído em |
|---------|-----------|-------|--------------|
| topic `dev.payments...` | setup.sh §1 | retenção/GB | teardown.sh |
| conector Debezium | setup.sh §2 | por task/hora | teardown.sh (primeiro) |
| compute pool (5 CFU) | setup.sh §3 | por CFU/min | teardown.sh (primeiro) |

---

## Phase 3: ACT

### 3.1 — Criar a estrutura e o README
Seguir o template do 2.1/2.2. Prosa pt-BR, código/identificadores em inglês.

### 3.2 — Escrever `setup.sh` / `teardown.sh`
- `setup.sh` idempotente: checa existência antes de criar (`confluent kafka topic list | grep` antes de `create`)
- `teardown.sh` destrói na ordem inversa, recursos que cobram por hora primeiro
- Ambos usam env vars do `.env` da Aula 0 (`CC_*`, `SR_*`) — zero segredo inline

### 3.3 — Adicionar seção "Replay rápido" (se o lab depende de camadas anteriores)
Comandos mínimos para recriar o estado necessário (gerador com `seed=42`, reaplicar conector) sem refazer o lab anterior inteiro — regra de [rules/labs.md](../rules/labs.md).

### 3.4 — Adicionar Troubleshooting

```markdown
### <Sintoma que o aluno vê>
\`\`\`bash
# Diagnóstico
<comando de diagnóstico>
# Output saudável esperado
<linha "healthy">
# Fix (causa mais comum primeiro)
<comando de fix>
\`\`\`
```

### 3.5 — Registrar no CHANGELOG
Entrada sob `## [Unreleased]` com prefixo do módulo:
```markdown
### Added
- modulo-04: Lab de janelas TUMBLE/HOP com transações sintéticas `(@author)`
```

---

## Phase 4: VALIDATE

### 4.1 — Cronometrar o lab (regra labs.md)
Rodar de cabo a rabo cronometrando, incluindo esperas de provisionamento cloud. Se passou do budget, **divida**. Não tente otimizar comandos para caber.

### 4.2 — Verificar reprodutibilidade
- Rodar o lab duas vezes seguidas, com `teardown.sh` entre elas, partindo de estados diferentes (environment limpo vs recursos de outros labs no ar)
- Os outputs batem? Senão, o lab tem estado escondido — investigar

### 4.3 — Output esperado é real
- Não escrever "you should see something like X". Colar o output literal.
- Campos não-determinísticos (IDs `lkc-*`, UUIDs, timestamps) marcados explicitamente: `<CLUSTER_ID>`, `<UUID>`, `<TS>`

### 4.4 — Teardown auditado (créditos)
- `teardown.sh` destrói TODO recurso listado no mapa do 2.5?
- Verificação pós-teardown incluída (`confluent connect cluster list` vazio, `confluent flink compute-pool list` vazio)?
- Rodar o lab + teardown e conferir no Billing que nada continua cobrando

### 4.5 — Anti-pattern check (regras labs.md)
- [ ] Sem "as an exercise, figure out X" — lab não é trabalho de casa
- [ ] Sem recurso cloud criado fora do `teardown.sh`
- [ ] Sem dependência de estado de outro lab sem seção "Replay rápido"
- [ ] Sem API key de admin em producer — service account do lab
- [ ] Sem screenshot da Console como única referência — sempre o comando CLI equivalente
- [ ] Sem IDs mágicos — todo `<cluster-id>`/`<pool-id>` mostra de onde veio

---

## Checklist final

- [ ] Lab cabe no time budget da sua categoria
- [ ] Goal em uma frase, com pré-requisitos declarados
- [ ] Comandos copy-pasteáveis (paths relativos à raiz do lab, segredos via env vars)
- [ ] Output esperado é literal (não "should see")
- [ ] Validação independente por seção passa
- [ ] `setup.sh` idempotente + `teardown.sh` par, auditado contra o mapa de recursos
- [ ] Seção "Replay rápido" presente (se há dependência entre labs)
- [ ] Troubleshooting cobre 1–3 falhas mais prováveis
- [ ] Testado fim-a-fim pelo menos uma vez (com cronômetro)
- [ ] CHANGELOG atualizado com prefixo `modulo-0X:`

---

## Exemplo de uso

```
"Siga new-lab para criar o lab do módulo 4 com MATCH_RECOGNIZE detectando
3 transações no mesmo cartão em <60s. Categoria: hands-on, 25 min.
Pré-requisito: topics CDC do módulo 3 (com Replay rápido)."
```

---

**Última atualização**: 2026-07-11
