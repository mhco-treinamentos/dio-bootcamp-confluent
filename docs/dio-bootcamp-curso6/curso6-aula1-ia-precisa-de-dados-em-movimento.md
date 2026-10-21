# Curso 6 · Aula 1 — Por que IA precisa de dados em movimento: RAG, contexto fresco e o limite do batch

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Aula 100% conceitual (o hands-on começa na Aula 2)
> **Pré-requisitos:** Cursos 1–5 (o pipeline completo de pé)

---

## 1. Objetivo da Aula

Abrir o curso com a tese: **modelos são cada vez mais intercambiáveis; o diferencial competitivo é o contexto** — e contexto envelhece em minutos. Entender por que o padrão **RAG** existe, onde o **batch quebra a IA** (o dado de ontem decidindo o agora), e o mapa do **Confluent Intelligence** — as três peças que o curso percorre: **AI/ML Functions, Embeddings/Vector Search e o Real-Time Context Engine + Streaming Agents**.

---

## 2. Conceitos Simplificados

### 2.1 O problema dos LLMs: inteligentes e desatualizados *(~4 min)*

Um LLM sabe o que estava nos dados de treino — um **passado congelado**:

- **Knowledge cutoff** — o modelo não sabe o que aconteceu depois do treino (nem nunca saberá, sem ajuda)
- **Não sabe NADA do seu negócio** — o estoque atual, o pedido do cliente, a política vigente: nada disso estava no treino
- **Alucinação** — pior que não saber é responder mesmo assim, com confiança

E a consequência de negócio: o chatbot que promete entrega de um produto **sem estoque**, o agente que oferece desconto sobre uma política **revogada ontem**. Não é burrice do modelo — é **contexto velho**.

### 2.2 RAG: dar ao modelo o que ele não tem *(~4 min)*

O padrão **Retrieval-Augmented Generation** resolve por arquitetura, não por retreino:

```
 pergunta do usuário
    │
    ▼
 ① RETRIEVAL — buscar os dados relevantes AGORA
    (documentos, estado do pedido, cadastro, política vigente)
    │
    ▼
 ② AUGMENT — montar o prompt: pergunta + contexto recuperado
    │
    ▼
 ③ GENERATION — o LLM responde ANCORADO no contexto
    ("segundo o pedido #4812, status: em transporte...")
```

- O modelo continua congelado — **o contexto é que fica fresco**: a inteligência é do LLM, a verdade é do retrieval
- RAG reduz alucinação porque muda a tarefa: de "lembre-se" para "**leia e responda com base nisto**"
- A pergunta que decide a qualidade de TODO sistema RAG: **quão fresco e confiável é o que o retrieval encontra?** — e é aqui que a arquitetura de dados entra na história da IA

### 2.3 O limite do batch: a IA decidindo com o ontem *(~4 min)*

O jeito tradicional de alimentar o retrieval: pipelines batch — o ETL noturno do Curso 5, agora servindo IA:

```
 O CICLO BATCH:            evento → banco → ETL noturno → índice/feature store → IA
 idade do contexto:                        até 24h+ ─────────────────────────►

 fraude:        o cartão clonado às 14h é bloqueado... amanhã
 suporte:       "seu pedido consta como não enviado" (saiu há 2 horas)
 recomendação:  oferece o que o cliente ACABOU de comprar
```

- Para análise retrospectiva, batch serve; para **decisão no momento**, o atraso é o defeito do produto
- E o agravante dos agentes (Aula 4): um agente **age** — agir sobre dado velho não é responder errado, é **fazer errado**
- A síntese da tese, cobrando os Cursos 1–5: nós **já construímos** o dado fresco, estruturado, com contrato, enriquecido — `pedidos_enriquecidos` existe, com segundos de idade. O que falta é **colocá-lo dentro da IA** — e é exatamente isso que o resto do curso faz

### 2.4 O mapa do Confluent Intelligence *(~5 min)*

O guarda-chuva de IA da plataforma — e o roteiro das próximas aulas:

```
                    CONFLUENT INTELLIGENCE
 ┌─────────────────────┬──────────────────────┬─────────────────────────┐
 │ AI/ML NO FLINK SQL  │ EMBEDDINGS &         │ REAL-TIME CONTEXT       │
 │ (Aula 2)            │ VECTOR SEARCH (Aula 3)│ ENGINE + STREAMING      │
 │                     │                      │ AGENTS (Aula 4)         │
 │ CREATE MODEL +      │ eventos → vetores    │ contexto vivo servido   │
 │ ML_PREDICT (LLMs    │ em tempo real; o     │ via MCP a QUALQUER      │
 │ remotos: OpenAI,    │ semantic layer que   │ agente; e agentes       │
 │ Bedrock, Vertex...) │ alimenta o RAG       │ nativos rodando no Flink│
 │ + funções embutidas │                      │                         │
 │ (anomalia, forecast,│                      │                         │
 │  sentimento)        │                      │                         │
 └─────────────────────┴──────────────────────┴─────────────────────────┘
                 tudo sobre o MESMO pipeline dos Cursos 1–5
        (mesmos tópicos, mesmo Schema Registry, mesma governança)
```

- **A ideia unificadora:** IA deixa de ser um sistema paralelo alimentado por cópias — ela **entra no fluxo**: inferência como transformação (Aula 2), embeddings como derivação contínua (Aula 3), contexto como serviço vivo (Aula 4)
- **Modelo-agnóstico por design:** a plataforma não vende LLM — você traz o seu (OpenAI, Bedrock, Vertex, Anthropic...), com suas chaves e seus limites; a plataforma entrega o que os modelos não têm: **o contexto fresco e governado**
- Nota de maturidade honesta: é a área que mais evolui na plataforma — recursos transitam de Early Access → Open Preview → GA em ritmo trimestral; o curso marca o status de cada peça e ensina a **estrutura**, que permanece

**Fechamento** *(~1 min)*: "modelos comoditizam; contexto diferencia — e contexto fresco é o que passamos cinco cursos construindo. Na próxima aula, a primeira peça: chamar um modelo de IA de dentro de um SELECT."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Resolvo com fine-tuning."** Retreinar não acompanha dado operacional (estoque, pedidos, fraude mudam por segundo) — fine-tuning ensina **comportamento**; RAG entrega **fatos frescos**. São ferramentas diferentes.
- **"RAG é sobre vetores."** RAG é sobre **retrieval** — vetores são UMA técnica (Aula 3); consulta estruturada ao estado atual (Aula 4) é outra, tão RAG quanto. Não reduzir o padrão à ferramenta.
- **"IA em tempo real = treinar modelo em tempo real."** Quase nunca — o modelo fica parado; o que corre é o **contexto** (inferência sobre dados frescos). Essa inversão é a tese do curso.
- **"Meu chatbot já usa o banco, está resolvido."** Consultar o banco operacional direto acopla a IA ao legado (o problema do Curso 1!) e perde o dado derivado/enriquecido. O fluxo curado é o contexto certo — o shift-left (Curso 5) servindo à IA.
- **Expectativa de maturidade:** deixar claro à turma que features desta área mudam de status rápido — o markdown de apoio marca o status na data de gravação; a doc é a verdade do dia.

**Ganchos com a trilha:**

- Inferência no fluxo (CREATE MODEL / ML_PREDICT + funções embutidas) → **Aula 2**.
- O semantic layer (embeddings + vector search) → **Aula 3**.
- Contexto servido a agentes via MCP + Streaming Agents → **Aula 4**.
- A trilha opcional de IA do **Desafio Final** nasce deste curso — o aluno que a escolher enriquece o pipeline com uma das três peças.

**Perguntas para checar entendimento:**

1. O chatbot de uma loja responde com base num índice atualizado à meia-noite. Descreva dois incidentes de negócio que essa arquitetura produz — e qual peça do pipeline dos Cursos 1–5 os resolveria.
2. Por que "modelos comoditizam, contexto diferencia" é um argumento de **arquitetura de dados**, e não de escolha de LLM?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **LLM (Large Language Model)** | Modelo de linguagem treinado sobre um corpus — congelado no knowledge cutoff. |
| **Knowledge cutoff** | A data-limite do conhecimento do modelo. |
| **Alucinação** | Resposta confiante e incorreta — agravada por falta de contexto. |
| **RAG (Retrieval-Augmented Generation)** | Padrão: recuperar contexto relevante e gerar a resposta ancorada nele. |
| **Retrieval** | A etapa de busca do contexto — estruturada ou semântica. |
| **Grounding (ancoragem)** | Responder com base no contexto fornecido, não na memória do modelo. |
| **Contexto fresco** | Dados com idade de segundos refletindo o estado atual do negócio. |
| **Confluent Intelligence** | O guarda-chuva de IA da plataforma: AI/ML no Flink, Embeddings/Vector Search, RTCE e Streaming Agents. |
| **Feature store / índice batch** | As camadas tradicionais de contexto — atualizadas em ciclos, o limite que o curso ataca. |

---

## 5. Referências

**Documentação e artigos oficiais:**

- Confluent — *Confluent Intelligence (visão geral do guarda-chuva)* — https://www.confluent.io/product/confluent-intelligence/
- Confluent Cloud — *AI & ML no Confluent Cloud (docs)* — https://docs.confluent.io/cloud/current/ai/overview.html
- Confluent — *Introducing Real-Time Context Engine for AI* (a tese do contexto fresco, na fonte) — https://www.confluent.io/blog/introducing-real-time-context-engine-ai/
- Lewis et al. — *Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks* (o paper original do RAG) — https://arxiv.org/abs/2005.11401
- Confluent — *Shift Left* (a base de dado curado que a IA consome) — https://www.confluent.io/blog/shift-left-bad-data-in-event-streams-part-1/
