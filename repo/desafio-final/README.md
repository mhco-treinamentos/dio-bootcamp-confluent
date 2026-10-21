# Desafio Final — Pipeline de pagamentos fim a fim no Confluent Cloud

> **O entregável do bootcamp.** Você vai montar, sozinho, o pipeline que os 7 cursos
> construíram em camadas: banco → CDC → contrato de dados → Flink → consumo — e
> **operá-lo** como profissional: medido, seguro, precificado e com garantias declaradas.
>
> ⏱ Estimativa: 4–8 h (em sessões — nunca deixe recursos ligados entre elas)
> ☁️ Custo estimado: **< US$ 5** dos seus créditos, *se* fizer teardown por sessão

---

## O que você vai entregar

Um **repositório público no GitHub** contendo:

1. O pipeline funcionando (configs, SQL e scripts — não binários, não prints soltos)
2. Um `README.md` que documenta cada camada **com evidências reais** (outputs de
   comando, não descrições)
3. As **quatro respostas de operação** do Curso 7 (Seção "Camada 5", abaixo)
4. `setup.sh` e `teardown.sh` na raiz — quem corrigir precisa conseguir subir e
   destruir tudo

**A régua é simples**: outro aluno do bootcamp, com uma conta Confluent Cloud nova,
deve conseguir reproduzir seu pipeline seguindo apenas o seu README.

---

## A arquitetura

```
Postgres (Neon)                 CONFLUENT CLOUD                          consumo
┌─────────────┐   ┌──────────────────────────────────────────────┐   ┌───────────┐
│ payments.*  │──►│ CDC Debezium ──► topics Avro ──► Flink SQL   │──►│ consumer  │
│ (5 tabelas) │   │ (managed)        (Schema Reg.)  (enriquece + │   │ ou sink   │
└─────────────┘   │                                  regra fraude)│   └───────────┘
                  └──────────────────────────────────────────────┘
                       ▲ tudo medido, com ACLs, custo conhecido e teardown
```

Cada camada corresponde a um curso — e você já fez todas elas uma vez, guiado.
Agora é sem trilhos, no **seu** environment, com os **seus** nomes.

> 🚀 **Comece pelo [SETUP.md](SETUP.md)**: provisiona o Confluent Cloud e o Neon
> (Camadas 1 e a base da 3) com validação a cada passo.
>
> 🎥 **Preferiu ver antes de fazer?** O [ROTEIRO-RESOLUCAO.md](ROTEIRO-RESOLUCAO.md)
> é a demo completa passo a passo — a mesma que o vídeo de resolução executa; dá para
> acompanhar comando a comando.

---

## As camadas (e onde revisar cada uma)

### Camada 1 — Fundação: environment, cluster e contrato de custo
*Passo a passo pronto: **[SETUP.md](SETUP.md)** (Confluent Cloud + Neon) · Revisão: [Curso 2](../docs/curso2/README.md)*

- Environment + cluster **Basic** dedicados ao desafio (não reuse o dos labs)
- Uma **service account por papel** (produtor, consumidor, admin de setup) — a key
  de admin pessoal não aparece em nenhum client
- **Evidência pedida**: `confluent kafka cluster describe` + a lista de service
  accounts com uma frase por papel

### Camada 2 — Contrato de dados: Avro + Schema Registry
*Revisão: [Curso 2, Aulas 4–5](../docs/curso2/README.md) · Schemas prontos em [`repo/schemas/payments/`](../schemas/payments/)*

- Todos os tópicos de negócio com schema Avro no namespace `com.bootcamp.payments.*`
- Compatibilidade **BACKWARD** e uma evolução demonstrada (adicionar campo com default
  → aceita; remover campo obrigatório → rejeitada)
- Campos sensíveis taggeados (`PII`/`LGPD`, `PCI`/`PCI_DSS`, `FIN`) — PAN **sempre**
  mascarado (BIN + last4)
- **Evidência pedida**: o output do `test-compatibility` nos dois cenários

### Camada 3 — Ingestão: CDC com Debezium gerenciado
*Revisão: [Curso 3](../docs/curso3/README.md) · DDL e seed prontos em [`repo/dataset/neon/`](../dataset/neon/)*

- Postgres no **Neon free tier** com o schema `payments` (5 tabelas, seed determinístico)
- Conector `PostgresCdcSourceV2` com usuário **`cdc_user` de menor privilégio** (não o
  owner) e `heartbeat.interval.ms` configurado
- Demonstre o envelope Debezium: um INSERT, um UPDATE e um DELETE capturados, com
  `op`, `before`/`after` e `ts_ms` comentados
- **Evidência pedida**: os 3 eventos consumidos do tópico, com o `op` de cada um
- ⚠️ **Conector ligado impede o Neon de hibernar** e queima os créditos dos dois lados
  — o teardown desta camada é obrigatório ao fim de cada sessão

### Camada 4 — Processamento: Flink SQL
*Revisão: [Curso 4](../docs/curso4/README.md)*

- Um statement de **enriquecimento**: transações ⋈ contas via **temporal join**
  (lembre: PRIMARY KEY nos dois lados + watermark andando nos dois lados)
- Uma **regra de fraude determinística** com janela ou `MATCH_RECOGNIZE`
  (ex.: `RULE_VELOCITY` — 3+ transações do mesmo cartão em < 60 s), materializada
  via CTAS num tópico `*.fraud.detected.*`
- `SET 'sql.state-ttl'` em qualquer join/agregação não-janelada
- **Evidência pedida**: o SQL dos statements + um alerta de fraude real emitido no
  tópico (e a sequência de eventos que o disparou)

### Camada 5 — Operação: as quatro respostas
*Revisão: [Curso 7](../docs/curso7/README.md) — esta é a seção que o Curso 7, Aula 5 prometeu*

Responda **no README, com números medidos** (não estimados):

| Pilar | O que responder | Como medir |
|---|---|---|
| ① Observabilidade | Throughput de pico e consumer lag por grupo do SEU pipeline | Metrics API ([Curso 7, Aula 2](../docs/curso7/aula2/README.md)) |
| ② Segurança | Quem acessa o quê: a lista de ACLs por service account, e por que cada uma existe | `confluent kafka acl list` ([Aula 3](../docs/curso7/aula3/README.md)) |
| ③ Custos | O que o pipeline custou (billing real) e as 2 maiores alavancas se fosse produção | `confluent billing cost list` + rate card ([Aula 4](../docs/curso7/aula4/README.md)) |
| ④ Confiabilidade | A garantia de entrega **trecho a trecho** (a tabela da [Aula 5](../docs/curso7/aula5/README.md), preenchida) e onde uma duplicata ainda chegaria ao usuário | Auditoria da configuração real |

### Camada 6 (opcional, vale destaque) — Lakehouse
*Revisão: Curso 5*

- Tableflow no tópico de fraude (`--storage-type MANAGED` funciona no Basic)
- Atenção à lição do changelog: tópico **retract** não materializa — só append/upsert
- **Evidência pedida**: `confluent tableflow topic describe` com phase `RUNNING`

---

## Regras do jogo

1. **Dados 100% sintéticos** — os geradores de [`repo/dataset/generators/`](../dataset/generators/)
   com `seed=42`. Nenhum dado real, nem "anonimizado". CVV/PIN não existem nem fake.
2. **Nenhum segredo no repositório** — credenciais via env vars (`CC_*`, `SR_*`);
   `.env` no `.gitignore`. Um secret commitado **reprova** a camada de segurança
   (e no mundo real, o incidente é seu).
3. **Teardown é parte da nota** — o `teardown.sh` deve deletar, nesta ordem:
   conectores → statements Flink → compute pool → tópicos → API keys → service
   accounts, e terminar com os comandos de verificação (`connect cluster list` e
   `flink compute-pool list` vazios).
4. **Nomeie seus recursos** — `desafio-<coisa>`, nunca nomes gerados. O Curso 7
   mostrou o que acontece com o entulho anônimo.
5. **Prosa em pt-BR, código em inglês** — como em todo o bootcamp.

---

## Estrutura sugerida do seu repositório

```
seu-repo/
├── README.md            # a documentação com evidências (o entregável principal)
├── setup.sh             # sobe tudo, na ordem, com echo do que está criando
├── teardown.sh          # destrói tudo + verificação final
├── sql/                 # DDL do Postgres + statements Flink
├── connectors/          # JSONs de config (sem secrets — use ${ENV_VAR})
├── schemas/             # os .avsc que você registrou
└── evidencias/          # outputs de comando citados no README
```

---

## Antes de entregar

Um exemplo de entrega bem estruturada está em [`exemplo-entrega/`](exemplo-entrega/) —
use como referência de formato: evidências com output real, scripts com verificação,
configs com env vars em vez de secrets.

---

## Rubrica

| Critério | Peso | O que separa o bom do ótimo |
|---|---|---|
| Pipeline funciona fim a fim | 30% | Evento nasce no Postgres e vira alerta de fraude sem intervenção manual |
| Contrato de dados | 15% | Evolução demonstrada + tags de PII/PCI corretas |
| As quatro respostas de operação | 30% | Números **medidos** na sua conta, não copiados dos guias |
| Reprodutibilidade | 15% | `setup.sh`/`teardown.sh` funcionam; README não assume contexto |
| Disciplina de custo | 10% | Custo total declarado; nada ficou ligado ao final |

---

## Armadilhas conhecidas (você foi avisado)

- **Temporal join "rodando mas sem emitir"**: watermark dos dois lados precisa passar
  do timestamp do evento — conector RUNNING + `sql.tables.scan.idle-timeout` + um
  evento novo na dimensão destravam ([Curso 4, Aula 3, Passo 6](../docs/curso4/aula3/README.md))
- **Janela sobre tópico CDC**: changelog não é append-only — o Flink recusa; converta
  para upsert com `ADD PRIMARY KEY NOT ENFORCED` ou janele um stream derivado
- **Recriar conector com o mesmo nome retoma os offsets** — não refaz o snapshot; se
  precisa do snapshot, use outro nome
- **`--from-beginning` com grupo já commitado não volta ao início** — grupo novo ou
  reset de offsets
- **Créditos**: conector (~US$ 0,50/h por task + base), statement Flink (CFU-hora) e
  Tableflow cobram **por existir**. O compute pool ocioso, não. Sessão terminou →
  `teardown.sh`.

---

| ← Trilha completa | ↑ Repo |
|---|---|
| [docs/](../docs/README.md) | [repo/](../README.md) |
