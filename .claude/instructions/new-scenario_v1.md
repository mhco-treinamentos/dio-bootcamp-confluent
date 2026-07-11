# Instruction: criar uma nova cena de workshop

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para construir uma cena pedagógica (uma seção do workshop) cumprindo [rules/workshop.md](../rules/workshop.md): time-boxing, comandos verificáveis, cleanup explícito.**

## O que é uma "cena"
Uma cena é uma unidade pedagógica autocontida de **10 a 30 min** com:
- Goal claro (uma frase)
- Comandos copy-pasteáveis
- Output esperado (snippet real, não "you should see something")
- Validação independente (`SELECT COUNT`, `curl /health`, etc.)
- Cleanup para resetar o estado

## Pré-requisitos
- Edição da tour: `data-streaming-tour-2026/<cena-pai>/`
- Stack base subindo (ver [kafka-setup_v1.md](kafka-setup_v1.md))
- Topic / connector / table dependentes já criados, OU a cena cria-os no Goal

---

## Phase 1: RESEARCH

### 1.1 — Definir o propósito pedagógico
- O que o atendente vai **aprender** ao final?
- Quais conceitos pré-requisitos? (Ex.: cena de CEP precisa que watermarks já tenham sido cobertos antes)
- Qual é a "moeda de troca" — o atendente troca tempo por que insight?

### 1.2 — Decidir o time budget
Da matriz em [rules/workshop.md](../rules/workshop.md):
| Tipo de cena | Máx |
|--------------|-----|
| Setup / bootstrap | 10 min |
| Conceitual | 15 min |
| Hands-on | 25 min |
| Integração E2E | 30 min |

Se passa do orçamento, **divida** em duas cenas.

### 1.3 — Mapear riscos
- Que falha pode aparecer no laptop do atendente? (RAM insuficiente, port collision, host clock skew)
- Qual é o failure mode mais provável? Esse vai pro **Troubleshooting**.
- Há alguma dependência de internet (pull de imagem, JAR remoto)? Pinar e cachear.

### 1.4 — Consultar references
- [references/workshop-stack-guide.md](../references/workshop-stack-guide.md) — decisões de versão e arquitetura
- [references/poison-pill-patterns.md](../references/poison-pill-patterns.md) — se a cena ensina sobre qualidade de dados
- Agents relevantes (ver [.claude/agents/README.md](../agents/README.md)) — `confluent-platform-expert`, `oracle-cdc-expert`, `flink-self-managed-expert`, `iceberg-expert`, `trino-expert`

---

## Phase 2: PLAN

### 2.1 — Estrutura da cena (template)

```markdown
### Cena N: <Título curto> (X min)
- **Goal**: <uma frase do que vai estar funcionando ao final>
- **Pré-requisito**: <cenas anteriores que precisam estar OK>

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
<query/curl/grep que confirma sucesso>
\`\`\`

**Cleanup:**
\`\`\`bash
<como reverter SÓ essa cena, sem destruir a stack toda>
\`\`\`
```

### 2.2 — Pensar no roteador de tempo
- Setup que não dá pra acelerar (Oracle XE startup) vai cedo
- Conceitos densos (watermarks) vão depois do warm-up
- Encerrar com algo "visual" (Control Center, Trino query com resultado bonito) — atendente sai satisfeito

### 2.3 — Decidir o domínio dos dados
Para tour 2026: sempre payments. Usar geradores existentes (`shared/generators/payments/`). Se a cena precisa de dados que ainda não temos gerador, levar para `new-generator_v1.md` antes.

---

## Phase 3: ACT

### 3.1 — Adicionar a cena ao README da cena-pai

Local: `data-streaming-tour-2026/<cena-pai>/README.md`

Editar a seção "Cenas" para incluir a nova cena no slot correto, seguindo o template do 2.1.

### 3.2 — Adicionar scripts auxiliares (se necessário)

```
data-streaming-tour-2026/<cena-pai>/
└── scenes/
    └── cena-N-<slug>/
        ├── commands.sh         # comandos cronometrados (opcional)
        ├── expected-output.txt # output dourado para diff
        └── validate.sh         # script de validação
```

### 3.3 — Adicionar entrada de Troubleshooting

```markdown
### <Sintoma que o atendente vê>
\`\`\`bash
# Diagnóstico
<comando de diagnóstico>
# Output saudável esperado
<linha "healthy">
# Fix (causa mais comum primeiro)
<comando de fix>
\`\`\`
```

### 3.4 — Atualizar Cleanup completo do workshop
Se a cena criou um topic/tabela/conector novos, garantir que o "Cleanup completo" no fim do README da cena-pai zera tudo.

---

## Phase 4: VALIDATE

### 4.1 — Cronometrar a cena (regra workshop)
Rodar a cena de cabo a rabo cronometrando. Se passou do budget, **divida**. Não tente otimizar comandos para caber.

### 4.2 — Verificar reprodutibilidade
- Roda a cena duas vezes seguidas, com `cleanup` entre elas, em estados de partida diferentes (laptop frio vs já-com-stack-no-ar)
- Os outputs batem? Senão, a cena tem state escondido — investigar

### 4.3 — Output esperado é real
- Não escrever "you should see something like X". Colar o output literal.
- Se há campos não-determinísticos (UUID, timestamp), marcar com `<UUID>`, `<TS>` explicitamente

### 4.4 — Test em laptop "pobre"
- Se possível, testar em um laptop com RAM/CPU no limite do pré-requisito declarado
- Marcar tempos: se Oracle XE leva 4 min para subir, documentar "espere ~4 min após `docker compose up -d`"

### 4.5 — Cleanup funciona
- O `Cleanup` da cena reverte só ela, sem afetar cenas posteriores que dependem da stack base?
- O `Cleanup completo` no fim do workshop zera tudo?

### 4.6 — Anti-pattern check (regras workshop)
- [ ] Sem "as an exercise, figure out X" — workshop não é trabalho de casa
- [ ] Sem efeitos colaterais não documentados
- [ ] Sem portas mágicas — toda porta exposta justificada
- [ ] Sem screenshots como única referência (atendente não pode grep em screenshot)
- [ ] Sem `docker-compose.yml` de 2000 linhas sem section-by-section commentary
- [ ] Sem "restart Docker Desktop" como fix real — diagnosticar a causa

---

## Checklist final

- [ ] Cena cabe no time budget da sua categoria
- [ ] Goal em uma frase, com pré-requisitos
- [ ] Comandos copy-pasteáveis (paths absolutos ou relativos à raiz da cena)
- [ ] Output esperado é literal (não "should see")
- [ ] Validação independente passa (consigo confirmar sucesso sem olhar logs)
- [ ] Cleanup da cena funciona
- [ ] Troubleshooting cobre 1–3 falhas mais prováveis
- [ ] Versões pinned (sem `latest`)
- [ ] Testado em laptop pelo menos uma vez fim-a-fim
- [ ] CHANGELOG atualizado

---

## Exemplo de uso

```
"Siga new-scenario para criar uma cena de Flink CEP detectando
3 transações no mesmo cartão em <60s. Categoria: hands-on, 20 min.
Pré-requisito: cenas de watermark + topic CDC já cobertas."
```
