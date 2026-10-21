# Curso 6 · Aula 3 — Embeddings em Tempo Real e Vector Search: a Create Embeddings Action e o semantic layer para RAG

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 2 do Curso 6 (CREATE MODEL/connection) · Demo de embeddings requer chave de modelo de embedding (a do aluno)
> ⚠️ **Vector Search: open preview** na data desta gravação — status na doc

---

## 1. Objetivo da Aula

Construir a metade semântica do RAG: o que é um **embedding** (o significado virando vetor), por que **o índice vetorial envelhece como qualquer contexto** — e como a plataforma o mantém fresco: a **Create Embeddings Action** (eventos virando vetores continuamente) e o **Vector Search** no Flink, fechando o ciclo do retrieval semântico **dentro do fluxo**.

---

## 2. Conceitos Simplificados

### 2.1 Embedding: o significado como coordenada *(~4 min)*

Um **embedding** é um vetor (centenas/milhares de números) que representa o **significado** de um texto — produzido por um modelo de embedding:

```
 "pedido atrasado na entrega"      → [0.12, -0.83, 0.44, ...]
 "encomenda não chegou no prazo"   → [0.11, -0.79, 0.47, ...]  ◄─ PERTO! (significado ~igual)
 "aprovação de crédito consignado" → [-0.91, 0.22, -0.35, ...] ◄─ LONGE  (outro assunto)
```

- **Proximidade no espaço = semelhança de significado** — buscar deixa de ser "contém a palavra" e vira "**significa parecido**" (similaridade de cosseno entre vetores)
- É o motor do retrieval semântico do RAG (Aula 1): a pergunta do usuário vira vetor, a busca encontra os trechos **mais próximos** — mesmo sem nenhuma palavra em comum
- **Embedding é inferência** — um modelo remoto que devolve vetor em vez de texto: o CREATE MODEL da Aula 2 com `task = embedding`. Nada novo na mecânica; novo é o uso

### 2.2 O problema: o índice vetorial também envelhece *(~4 min)*

O RAG tradicional monta o índice em batch — e herda o defeito da Aula 1:

```
 BATCH:  documentos ──(job noturno)──► embeddings ──► vector store
         ▲ a política mudou às 10h; o RAG responde a política velha até amanhã

 STREAMING (esta aula):
         evento novo/alterado ──► embedding NA HORA ──► índice fresco
         ▲ o RAG responde o vigente, com segundos de idade
```

- O contexto semântico é contexto: FAQ atualizada, descrição de produto alterada, chamado novo — **cada mudança deveria reindexar na hora**, não no ciclo
- E o pipeline do bootcamp já entrega a matéria-prima: os eventos com contrato passando pelo Flink — falta o passo "texto → vetor → índice", **contínuo**

### 2.3 Anatomia do embedding no fluxo *(~4 min)*

As duas formas de gerar vetores continuamente na plataforma:

```
 ① O CAMINHO SQL (a mecânica da Aula 2):
    CREATE MODEL meu_embedding (task=embedding, provider=...)
    + ML_PREDICT sobre o campo de texto
    → o vetor vira COLUNA do evento → CTAS → tópico de embeddings

 ② A CREATE EMBEDDINGS ACTION (o atalho gerenciado):
    uma AÇÃO declarada sobre o tópico: "gere embeddings do campo X
    com o modelo Y" → a plataforma mantém o tópico de vetores
    → menos SQL, mesma essência
```

- Nos dois caminhos, o resultado é o mesmo padrão do bootcamp: **o embedding é evento** — num tópico, com schema (o vetor é um `ARRAY<FLOAT>` tipado!), replayável (recriar o índice = replay, Curso 1)
- A escolha: a **Action** para o caso padrão (menos código, gerenciado); o **SQL** quando há transformação junto (limpar/concatenar campos antes de vetorizar — o shift-left aplicado ao semântico)
- Detalhe de arquiteto: **o modelo de embedding é parte do contrato** — trocar de modelo muda o espaço vetorial inteiro (reindexar tudo); versionar essa decisão importa

### 2.4 Vector Search: o retrieval fechando o ciclo *(~4 min)*

Com os vetores fluindo, falta buscá-los — o **Vector Search** no Flink (⚠️ open preview):

```
 pergunta → embedding da pergunta → VECTOR_SEARCH(índice, vetor, top_k)
                                       │
                                       ▼
                        os K eventos mais PRÓXIMOS em significado
                        → viram o CONTEXTO do prompt (o RAG completo)
```

- A busca integra com **índices vetoriais** (a plataforma conecta a vector stores — ex.: MongoDB Atlas, Pinecone, Elastic — conferir a lista atual na doc) alimentados pelos tópicos de embeddings
- O desenho completo do **semantic layer em streaming**: eventos → embeddings contínuos → índice fresco → busca semântica → prompt — **todas as peças dentro da plataforma governada**, nenhum pipeline batch no caminho
- Status honesto: open preview — a API pode mudar; o **padrão arquitetural** (índice alimentado por stream) é o que o aluno leva, e ele vale mesmo com outro vector store na ponta

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Hands-on: geração de embeddings (aluno com chave de embedding reproduz). Vector Search: **demonstração do instrutor** (open preview + vector store externo — transparência de sempre). Tempo de tela: ~7 min.

**O que a demo deve mostrar, nesta ordem:**

1. **O modelo de embedding no catálogo** — CREATE MODEL com `task=embedding` (a connection da Aula 2 reaproveitada ou uma nova para o modelo de embedding).
   - **Observação-alvo:** é a mesma anatomia da Aula 2 — embedding É inferência; só muda o tipo do OUTPUT (`ARRAY<FLOAT>`).

2. **Texto virando vetor ao vivo** — ML_PREDICT sobre um campo textual do dataset (placeholder — ex.: descrição/observação do pedido); o vetor aparecendo como coluna.
   - **Observação-alvo:** o momento de "ver o significado virar número" — mostrar 2 textos parecidos e apontar (numa visualização simples ou nos primeiros componentes) que os vetores se parecem.

3. **O tópico de embeddings** — CTAS materializando `pedidos_embeddings`; o schema com o array de floats no Registry.
   - **Observação-alvo:** o embedding como **evento com contrato** — replayável, consumível, governado. O semantic layer nasce como cidadão do pipeline.

4. **[Instrutor] O ciclo completo com Vector Search** — a busca semântica sobre o índice alimentado pelo tópico: uma pergunta em linguagem natural retornando os K eventos mais próximos; um evento novo entrando e sendo **encontrável segundos depois**.
   - **Observação-alvo:** o índice que não envelhece — o contraste direto com o job noturno da seção 2.2. (Narrar o status preview e o papel do vector store externo.)

5. **Teardown** — parar statements (o custo aqui é triplo: statement + chamadas de embedding + o índice externo, se houver).

**Fechamento na tela** *(~1 min)*: "significado virou vetor, vetor virou evento, e a busca semântica ficou fresca. Temos inferência (Aula 2) e retrieval (Aula 3) — falta quem USA isso tudo para agir: contexto servido a agentes, e agentes vivendo no fluxo. A última aula do curso."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Embedding é coisa de IA avançada, outro mundo."** É o ML_PREDICT da Aula 2 devolvendo array — mesma anatomia, mesma connection, mesmo padrão "resultado vira evento". Desmistificar pela continuidade.
- **"Monto o índice uma vez e pronto."** Índice vetorial é contexto — envelhece como qualquer contexto (a tese da Aula 1 aplicada ao semântico). RAG de qualidade se mede pela **idade do índice**.
- **"Trocar o modelo de embedding é upgrade transparente."** Muda o espaço vetorial inteiro: pergunta em espaço novo não encontra documentos em espaço velho. É breaking change de contrato — tratar como tal (a disciplina do Curso 2, no semântico).
- **"Busca semântica substitui busca estruturada."** São complementares: "pedidos parecidos com esta reclamação" é semântica; "o status do pedido #4812" é estruturada (Aula 4). O RAG maduro usa as duas.
- **Preview é preview:** API do Vector Search pode mudar; ensinar o padrão, marcar o status, apontar a doc.

**Ganchos com a trilha:**

- A mecânica CREATE MODEL/connection → **Aula 2** (o par direto).
- O retrieval **estruturado** do estado atual (a outra metade do contexto) → **Aula 4** (RTCE).
- Replay do tópico de embeddings para reconstruir índices → **Curso 1** (retention) fechando mais um ciclo.
- Na trilha de IA do **Desafio Final**, o semantic layer é a opção mais impressionante de documentar no README.

**Perguntas para checar entendimento:**

1. A FAQ da empresa mudou às 10h. Compare a resposta do RAG às 10h05 nas duas arquiteturas da seção 2.2 — e diga qual peça desta aula elimina a diferença.
2. Por que trocar o modelo de embedding exige reindexar tudo — e o que isso tem a ver com a disciplina de evolução de schema do Curso 2?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Embedding** | Vetor numérico que representa o significado de um conteúdo, produzido por um modelo de embedding. |
| **Espaço vetorial / similaridade de cosseno** | A geometria do significado: proximidade entre vetores = semelhança semântica. |
| **Modelo de embedding** | O modelo (remoto) cuja saída é o vetor — declarado via CREATE MODEL com task de embedding. |
| **Create Embeddings Action** | O recurso gerenciado que mantém um tópico de embeddings a partir de um tópico de origem. |
| **Semantic layer** | A camada de vetores + índice que habilita retrieval por significado. |
| **Vector store / índice vetorial** | O sistema que armazena vetores e responde busca por proximidade (Atlas, Pinecone, Elastic...). |
| **Vector Search (Flink)** | A busca semântica integrada ao Flink SQL (open preview na gravação). |
| **top_k** | Quantos vizinhos mais próximos o retrieval retorna para o prompt. |
| **Reindexação** | Reconstruir o índice — obrigatória ao trocar o modelo de embedding; barata via replay do tópico. |

---

## 6. Referências

**Documentação e artigos oficiais:**

- Confluent Cloud — *Vector Embeddings no Flink (Create Embeddings, modelos)* — https://docs.confluent.io/cloud/current/ai/embeddings/overview.html
- Confluent Cloud — *Vector Search (Flink SQL — status e integrações)* — https://docs.confluent.io/cloud/current/ai/ai-features/vector-search.html
- Confluent — *Confluent Intelligence (o semantic layer no guarda-chuva)* — https://www.confluent.io/product/confluent-intelligence/
- Lewis et al. — *Retrieval-Augmented Generation...* (o paper do RAG — a Aula 1 na fonte) — https://arxiv.org/abs/2005.11401
- Confluent Developer — *GenAI e RAG com streaming (cursos e padrões)* — https://developer.confluent.io/courses/
