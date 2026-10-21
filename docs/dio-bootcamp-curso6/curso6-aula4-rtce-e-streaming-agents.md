# Curso 6 · Aula 4 — Real-Time Context e Streaming Agents: alimentando agentes com dados operacionais ao vivo

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aulas 1–3 do Curso 6
> Status na gravação: **Real-Time Context Engine: GA** · **Streaming Agents: conferir status atual (preview → evolução rápida)**

---

## 1. Objetivo da Aula

Fechar o curso com quem **usa** tudo isso: agentes. Dissecar o **Real-Time Context Engine (RTCE)** — o estado vivo do negócio servido a **qualquer agente via MCP**, sem o agente saber que Kafka existe — e os **Streaming Agents**: agentes que não esperam pergunta, porque **vivem no fluxo** e reagem a eventos. O contexto fresco da Aula 1, finalmente entregue a quem decide.

---

## 2. Conceitos Simplificados

### 2.1 O agente e a última milha do contexto *(~3 min)*

Um **agente** = LLM + ferramentas + objetivo: ele raciocina, **consulta** e **age**. E a qualidade da ação depende da pergunta da Aula 1: *o que ele enxerga quando consulta?*

As opções ruins de dar visão ao agente:

- **Acesso direto ao banco operacional** — acoplamento (Curso 1!), risco, e o dado cru sem o enriquecimento do pipeline
- **Montar um MCP server caseiro sobre o Kafka** — consumidores, cache, auth, RBAC, telemetria... um projeto de infraestrutura inteiro para servir um lookup
- **Snapshot batch** — o agente decidindo com o ontem (a Aula 1 inteira)

A resposta da plataforma tem duas metades — **contexto servido** (RTCE, para qualquer agente de fora) e **agente embutido** (Streaming Agents, vivendo no Flink).

### 2.2 Anatomia do Real-Time Context Engine *(~5 min)*

```
 TOPICS (com schema — o de sempre)
    │  habilitação por tópico (como o Tableflow!)
    ▼
 RTCE — materializa o tópico numa camada de serving de BAIXA LATÊNCIA
    │   dois modos (decididos pelo cleanup.policy do tópico — Curso 1!):
    │   · APPEND — todo o histórico consultável (fatos, event logs)
    │   · UPSERT — só o ÚLTIMO estado por chave (pedido atual, saldo, estoque)
    ▼
 MCP SERVER gerenciado (auth, RBAC, audit — de fábrica)
    ▼
 QUALQUER AGENTE (Claude, LangChain, Bedrock, Agentforce...)
    "qual o status do pedido #4812?" → resposta com segundos de idade
```

As decisões de design que valem a dissecação:

- **O agente nunca vê o Kafka** — ele fala **MCP** (o protocolo aberto de contexto para agentes) e consulta com filtros, ranges e predicados; consumidores, offsets e schemas ficam atrás da cortina. A fronteira de gestão (Curso 2) chegando à IA
- **Append × upsert** é o par exato da dualidade (Curso 4) e da materialização CDC (Curso 5): o RTCE escolhe o modo pelo `cleanup.policy` do tópico — **a decisão de compaction lá do Curso 1 agora define o que o agente enxerga** (histórico × estado atual)
- **Governança de fábrica** — auth, RBAC e audit log em cada consulta do agente: quem perguntou o quê, quando (a resposta corporativa ao "agente com acesso a dados": Live 3 na veia)
- **Dois MCPs, papéis diferentes** — o MCP do RTCE serve **dados de negócio** aos agentes; o MCP de gestão do Confluent administra **recursos** (clusters, tópicos). Complementares, não concorrentes

### 2.3 Streaming Agents: o agente que não espera pergunta *(~5 min)*

O RTCE serve agentes **reativos** (respondem quando perguntados). Os **Streaming Agents** invertem: o agente é **event-driven** — mora no Flink e age quando o evento chega:

```
 AGENTE TRADICIONAL (request/response):     STREAMING AGENT (event-driven):
 humano pergunta → agente responde          evento acontece → agente AGE
 "tem fraude nesse pedido?"                 PedidoCriado + score anômalo (Aula 2)
                                              → agente investiga (tools via MCP)
                                              → decide → emite FraudeSinalizada
                                              → (e o resultado é EVENTO, claro)
```

- **É o EDA aplicado a agentes** — a Aula 1 do Curso 1 fechando o círculo: o agente é um consumidor inteligente que publica fatos. Monitoração contínua, sem polling, sem espera
- A anatomia no Flink: **Agent Definition** (o agente declarado — modelo + instruções + tools), **tool calling via MCP** (o agente consulta sistemas externos e o próprio RTCE), e o trio da plataforma de graça: **replay para testar** (rodar o agente sobre eventos históricos!), **observabilidade e recuperação** (checkpoints — Curso 4 — valendo para agentes)
- O caso do bootcamp desenhado: a anomalia da Aula 2 dispara → o agente busca o contexto do cliente (RTCE) e casos parecidos (Vector Search, Aula 3) → decide → publica a decisão como evento → o pipeline segue. **As três aulas do curso compostas num fluxo só**
- Status honesto: a área mais nova da plataforma — conferir o estágio atual; o **padrão** (agente event-driven sobre log replayável) é o que fica

### 2.4 O desenho completo do curso *(~2 min)*

```
 pipeline (C1–C5) ─► scores/ML (Aula 2) ─► embeddings/busca (Aula 3) ─┐
        │                                                             │
        └──► RTCE: estado vivo via MCP (Aula 4) ◄─────────────────────┤
                      ▲                                               │
        agentes externos (perguntam)          Streaming Agents (agem)─┘
```

Contexto fresco **servido** e **agido** — a promessa da Aula 1, cumprida com as peças dos seis cursos.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Hands-on: **RTCE + um agente MCP real** (o quickstart oficial usa um cliente MCP como o Claude — encaixe natural). Streaming Agents: **demonstração do instrutor/conceitual** conforme status. Tempo de tela: ~7 min. **Teardown fecha o curso.**

**O que a demo deve mostrar, nesta ordem:**

1. **Habilitar o RTCE num tópico do bootcamp** — `pedidos_enriquecidos` (upsert: o estado atual por pedido) e/ou o tópico de fatos (append); apontar o modo escolhido pelo cleanup.policy.
   - **Observação-alvo:** a habilitação espelha o Tableflow (Curso 5) — mesma filosofia, outra serving layer: lá pro analítico, aqui pro agente.

2. **Conectar um agente via MCP** — um cliente MCP (ex.: Claude/assistente compatível) apontado pro endpoint do RTCE com a credencial adequada.
   - **Observação-alvo:** a configuração é um endpoint + auth — o agente **descobre** os tópicos e schemas sozinho pelas tools MCP.

3. **A conversa que fecha o bootcamp** — perguntar em linguagem natural: "qual o status do pedido X?", "quantos pedidos do cliente Y hoje?", "algum pedido acima de R$ 500 na última hora?".
   - **Observação-alvo:** o momento-síntese do curso — **o agente respondendo com o estado de segundos atrás**, sem saber que Kafka existe. O dado do INSERT no Neon (C3), enriquecido pelo Flink (C4), respondendo em linguagem natural.

4. **A prova da frescura** — mudar um pedido na origem (o UPDATE no Neon da demo do Curso 3) e **perguntar de novo**: a resposta mudou.
   - **Observação-alvo:** o ciclo completo banco → CDC → Flink → RTCE → agente, em segundos, na frente da turma. A cena mais forte do bootcamp inteiro — ensaiá-la.

5. **[Instrutor/conceitual] Streaming Agent** — o desenho do agente de fraude (seção 2.3) sobre telas/doc: a Agent Definition, o disparo pelo evento, a decisão publicada como evento.
   - **Observação-alvo:** a diferença reativo × event-driven materializada — e o replay como superpoder de teste de agentes.

6. **Teardown do curso** — desabilitar RTCE, revogar credenciais de demo, parar tudo; billing check.

**Fechamento na tela** *(~1 min)*: "agentes enxergando o agora, e agentes vivendo no fluxo. O pipeline está completo — do banco legado à IA. Falta o que separa projeto de produção: operar isso tudo com disciplina. Curso 7: observabilidade, segurança, custos e confiabilidade. E nos vemos na Live 3 antes."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"RTCE é um banco para o agente consultar."** É a **serving layer do fluxo** — materializada continuamente do tópico, governada, sem ETL. Banco responde o que foi escrito nele; o RTCE responde **o que está acontecendo**.
- **"Preciso dar ao agente acesso ao Kafka/banco."** Não — e não deve: o MCP gerenciado é exatamente a camada que evita agente com credencial de infraestrutura. Escopo mínimo (Curso 2) vale dobrado para agentes.
- **"Streaming Agent é um chatbot escalado."** É outra categoria: **event-driven** — age sem pergunta, sobre eventos, com replay e recuperação. Chatbot é interface; Streaming Agent é automação.
- **"Agente com dado fresco = agente confiável."** Fresco é necessário, não suficiente — governança (quem pode ver o quê: RBAC/audit do RTCE) e observabilidade (o que o agente fez e por quê) completam a confiança. Live 3 e Curso 7.
- **A área mais volátil do bootcamp:** status, APIs e nomes evoluem por trimestre — o markdown marca a data; a doc é a verdade do dia. Ensinar padrões, não telas.

**Ganchos com a trilha:**

- `cleanup.policy` decidindo append × upsert → **Curso 1, Aula 3** (a decisão antiga com consequência nova).
- A anomalia que dispara o agente → **Aula 2**; os casos parecidos que ele consulta → **Aula 3**.
- RBAC, audit e a governança do acesso de agentes → **Live 3** e **Curso 7, Aula 3**.
- Na trilha de IA do **Desafio Final**, o RTCE + um agente MCP é o fecho de maior impacto para o vídeo de apresentação do aluno.

**Perguntas para checar entendimento:**

1. Um tópico com `cleanup.policy=compact` e outro `delete` são habilitados no RTCE. O que cada agente consegue perguntar sobre eles — e qual serve para "o saldo atual do cliente"?
2. Desenhe o agente de fraude do bootcamp nomeando qual peça de cada aula do curso ele usa (2, 3 e 4) — e onde a decisão dele vai parar.

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Agente (AI agent)** | LLM + ferramentas + objetivo: raciocina, consulta e age. |
| **MCP (Model Context Protocol)** | O protocolo aberto que padroniza como agentes descobrem e consultam contexto/ferramentas. |
| **Real-Time Context Engine (RTCE)** | Serviço gerenciado que materializa tópicos numa serving layer de baixa latência e os serve a agentes via MCP. |
| **Modo append / upsert (RTCE)** | Histórico completo consultável × último estado por chave — decidido pelo cleanup.policy do tópico. |
| **Serving layer** | A camada otimizada para lookups rápidos que o RTCE mantém a partir do fluxo. |
| **Streaming Agents** | Agentes event-driven definidos no Flink: disparam por eventos, usam tools via MCP e publicam decisões como eventos. |
| **Agent Definition** | A declaração do agente (modelo, instruções, ferramentas) na plataforma. |
| **Tool calling** | O mecanismo pelo qual o agente invoca ferramentas/sistemas externos durante o raciocínio. |
| **Replay de agentes** | Testar/reprocessar o agente sobre eventos históricos — o superpoder herdado do log (Curso 1). |

---

## 6. Referências

**Documentação e artigos oficiais:**

- Confluent Cloud — *Real-Time Context Engine (overview, modos append/upsert, MCP)* — https://docs.confluent.io/cloud/current/ai/real-time-context-engine/overview.html
- Confluent — *Introducing Real-Time Context Engine for AI* — https://www.confluent.io/blog/introducing-real-time-context-engine-ai/
- Confluent — *Streaming Agents (atualizações e Agent Definition)* — https://www.confluent.io/blog/2025-q4-streaming-agents-update/
- Confluent — *Real-Time Context Engine (produto e casos)* — https://www.confluent.io/product/real-time-context-engine/
- Anthropic — *Model Context Protocol (a especificação do MCP)* — https://modelcontextprotocol.io/
