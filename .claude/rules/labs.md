---
globs: "**/labs/**,**/desafio-final/**"
---

# Labs Rules

> **Padrões pedagógicos para os labs do bootcamp: passos verificáveis, seções time-boxed e teardown de recursos cloud.**

Aplica-se a tudo sob `repo/labs/modulo-0X-*/` e `repo/desafio-final/`. Labs são **artefatos de ensino** — correção, reprodutibilidade e disciplina de créditos importam mais que elegância. Cada lab corresponde a um curso do bootcamp (modulo-02..07) e é uma camada do Desafio Final (build-as-you-go).

## Estrutura

Todo lab tem no README quatro seções obrigatórias:
1. **Pré-requisitos** — Aula 0 concluída (environment/cluster/API keys), versões exatas do CLI, env vars esperadas (`CC_*`, `SR_*`)
2. **Walkthrough seção a seção** — seções numeradas, cada uma com outcome claro
3. **Troubleshooting** — top 5–10 modos de falha com fix copy-pasteável
4. **Teardown** — como destruir TODO recurso cloud criado pelo lab (`teardown.sh`)

Além do README, todo lab tem o par `setup.sh` / `teardown.sh` na raiz.

## Design de seção

Cada seção DEVE incluir:
- **Goal** (uma frase — o que o aluno vai ver funcionando)
- **Comandos** (copy-pasteáveis, em ordem)
- **Output esperado** (snippet real, não "você deve ver algo assim")
- **Validação** (um `confluent kafka topic list`, `SELECT COUNT(*)` no Flink, ou `curl` que prova que a seção funcionou)
- **Cleanup** (como desfazer só esta seção se o aluno travar)

## Time-Boxing

| Tipo de seção | Duração máx |
|---------------|-------------|
| Setup / bootstrap | 10 min |
| Seção conceitual | 15 min |
| Exercício hands-on | 25 min |
| Integração end-to-end | 30 min |

Cada aula gravada tem 15–20 min; o lab correspondente deve caber em 1–2 seções. Se uma seção estoura o budget, divida. Alunos perdem engajamento após ~30 min de uma única tarefa.

## Verificabilidade

Todo passo deve ser verificável de forma independente — nada de "confia, funciona". Use:
- `confluent kafka topic list` / `confluent kafka topic consume <topic> -b` para chegada de dados
- `confluent connect cluster list` + status `RUNNING` para conectores gerenciados
- `SELECT COUNT(*)` em statement Flink para dados processados
- `confluent flink statement list` para statements ativos
- No fallback local: `docker compose ps` e `curl localhost:8081/subjects`

Exemplo de verificação de conector CDC:
```bash
confluent connect cluster describe <connector-id> --cluster <cluster-id> -o json | jq -r '.connector.state'
# Esperado: RUNNING
```

## Comandos: copy-pasteáveis

- Paths relativos à raiz do lab (`repo/labs/modulo-0X-*/`) — nunca "entre em algum diretório"
- Segredos sempre via env vars (`$CC_API_KEY`) — nunca inline no comando
- Para heredocs, delimitador com aspas (`<<'EOF'`) para evitar expansão de shell
- Anotar comandos destrutivos (`confluent environment delete` apaga TUDO do environment)

## Teardown & Créditos (regra de ouro)

Créditos gratuitos (US$ 400/30 dias) acabam rápido com recursos ociosos. Todo lab DEVE ter:
- **`teardown.sh`** par do `setup.sh`: deleta conectores, statements Flink, compute pools, topics e service accounts criados pelo lab — nessa ordem
- **Teardown parcial por seção**: cleanup direcionado (deletar um topic, pausar um conector)
- **Duração esperada** (ex.: "teardown completo ~2 min")
- **Verificação pós-teardown**: comando que confirma que nada ficou rodando (`confluent flink compute-pool list` vazio, `confluent connect cluster list` vazio)

O cluster básico e o environment da Aula 0 são compartilhados entre labs — o teardown do lab NÃO os destrói (isso é o `teardown.sh` da Aula 0).

## Encadeamento entre labs (build-as-you-go)

- Cada lab declara no README de quais camadas anteriores depende (ex.: modulo-04 depende dos topics CDC do modulo-03)
- Se o aluno fez teardown do lab anterior, o README DEVE ter uma seção **"Replay rápido"**: comandos mínimos para recriar o estado necessário (rodar o gerador com `seed=42`, reaplicar o setup do conector) sem refazer o lab inteiro
- O Desafio Final referencia os labs por camada — nunca duplica código que já vive em `repo/shared/`

## Versões

Pinar toda versão de ferramenta local explicitamente (CLI `confluent` v4+, Python 3.11, imagem `postgres:16-alpine` do lab de CDC) — ver [.claude/versions.lock](../versions.lock). Serviços gerenciados (Kafka, SR, Flink, Tableflow) não têm pin: os labs acompanham o que o Confluent Cloud roda; documentar a data em que o lab foi validado.

## Recursos e custos

Declarar upfront o que o lab cria e o que custa:
```markdown
## Recursos cloud deste lab
- 1 cluster Basic (compartilhado da Aula 0) — cobra por uso
- 1 compute pool Flink (5 CFU max) — MAIOR custo do lab; teardown obrigatório
- 1 conector gerenciado Debezium — cobra por task/hora
- 3 topics (retenção 24h)
```

## Seções de Troubleshooting

Estrutura:
```markdown
### <Sintoma que o aluno vê>
# O que o aluno cola para diagnosticar
<comando de diagnóstico>
# Output esperado no estado saudável
<marcador saudável>
# Fix (causa mais comum primeiro)
<comando de fix>
```

Não escreva teoria exaustiva — dê o fix. Linke docs para quem quiser se aprofundar.

## Realismo dos dados

Dados de lab DEVEM ser:
- **Sintéticos** — nenhum PII real, nem "anonimizado"
- **Plausíveis no domínio** — CPFs, MCCs, PANs mascarados devem parecer reais mesmo sendo fake
- **Determinísticos com seed** — `seed=42` para todo aluno ver os mesmos dados na seção N
- **Limitados** — labs não geram >10k registros por seção; aluno espera (e créditos queimam)

## Anti-patterns pedagógicos

Evitar:
- ❌ "Como exercício, descubra X" — lab tem orçamento de tempo; mostre a resposta
- ❌ Recurso cloud criado sem entrada correspondente no `teardown.sh`
- ❌ Lab que depende de estado de outro lab sem seção "Replay rápido"
- ❌ API key de admin usada no producer — service account least-privilege por lab
- ❌ Screenshots da Console como única referência (aluno não faz grep em screenshot) — sempre o comando CLI equivalente
- ❌ Portas/IDs mágicos sem explicação (todo `<cluster-id>` mostra de onde veio)
- ❌ "Recrie o cluster" como fix real — diagnostique o problema de verdade
- ❌ Efeitos colaterais não documentados (setup que silenciosamente cria um service account extra)

## Idioma

Prosa em **pt-BR**; código em **inglês** (identificadores, logs, nomes de tabela/topic). Codebase de idioma misto é frágil; prosa pode ser traduzida sem quebrar nada.

## Cross-References

Ao ensinar um tópico que difere entre o fallback local e o Confluent Cloud, anote o delta:
> "O fallback local usa 1 broker KRaft com RF=1 porque é um único container. No Confluent Cloud os defaults são RF=3 e min.insync.replicas=2 — ver [references/bootcamp-stack-guide.md](../references/bootcamp-stack-guide.md)."

Isso evita confusão quando o aluno migra do modo offline para o cloud.

---

**Última atualização**: 2026-07-11
