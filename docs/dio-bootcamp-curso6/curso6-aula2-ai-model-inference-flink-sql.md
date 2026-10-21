# Curso 6 · Aula 2 — AI Model Inference no Flink SQL: CREATE MODEL e ML_PREDICT para enriquecer streams

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Curso 4 (Flink SQL) · Para a demo com LLM remoto: uma API key de provedor (OpenAI/Bedrock/Vertex/Anthropic — a do aluno; custo próprio, centavos)

---

## 1. Objetivo da Aula

Colocar o modelo **dentro do fluxo**: a anatomia do **CREATE MODEL** (o modelo remoto como recurso de catálogo) e do **ML_PREDICT** (inferência como função SQL), os casos que pagam a conta — **fraude, sentimento, classificação** — e as **funções de ML embutidas** (anomalia, forecast) que rodam sem chave externa. A inferência vira o que ela sempre deveria ser num pipeline: **mais uma transformação**.

---

## 2. Conceitos Simplificados

### 2.1 A inversão: em vez de levar o dado à IA, trazer a IA ao dado *(~3 min)*

O desenho tradicional — cada aplicação chamando o modelo por conta própria:

- N serviços × N integrações com o provedor: chave espalhada, retry reinventado, custo invisível, zero governança
- O dado sai do pipeline, visita a IA, e o resultado... vai para onde? (geralmente, para lugar nenhum reutilizável)

A inversão do Flink: **a inferência é um operador do pipeline** — o evento passa, o modelo pontua, o resultado **vira evento também** (num tópico, com schema, consumível por todos — a dualidade do Curso 4 valendo para scores de IA).

### 2.2 Anatomia do CREATE MODEL *(~5 min)*

O modelo remoto entra no catálogo do Flink como um recurso declarado:

```sql
CREATE MODEL sentimento_reviews
INPUT  (texto STRING)
OUTPUT (sentimento STRING)
WITH (
  'provider'          = 'openai',            -- ou bedrock, vertexai, azureopenai...
  'task'              = 'classification',
  'openai.connection' = 'minha-conexao',     -- a credencial, como RECURSO separado
  'openai.system_prompt' = 'Classifique o sentimento: POSITIVO, NEGATIVO ou NEUTRO.'
);
```

Dissecando as decisões de design:

- **O modelo é um recurso de catálogo** — como uma tabela: nomeado, versionável, com INPUT/OUTPUT **tipados** (o contrato do Curso 2 se estendendo à IA — a resposta do modelo tem schema!)
- **A credencial é uma *connection*, separada do modelo** — a API key do provedor vive num recurso próprio (criado via CLI/UI), não no SQL: quem escreve query não vê segredo (o princípio das API keys do Curso 2, aplicado a chaves de LLM)
- **BYO-modelo:** OpenAI, AWS Bedrock, Vertex AI, Azure OpenAI, Anthropic... — a plataforma orquestra a chamada; **a conta e o controle (rate limits, custos) são seus**
- **O system prompt é configuração** — a "instrução fixa" do caso de uso mora no modelo declarado, não repetida em cada query

### 2.3 ML_PREDICT: inferência como função de tabela *(~4 min)*

```sql
-- o modelo pontuando um stream, linha a linha:
SELECT r.*, p.sentimento
FROM reviews r, LATERAL TABLE(ML_PREDICT('sentimento_reviews', r.texto)) p;
```

- **É um join com o modelo**: cada evento entra, a predição sai anexada — e o resultado pode virar tópico via CTAS (`reviews_classificadas`), com schema, consumível por sinks, Tableflow, agentes...
- **A latência agora inclui uma chamada remota** — inferência é a transformação mais cara do pipeline: pensar em **onde** ela entra (depois do filtro, não antes: pontuar só o que importa) e no **custo por evento** (um stream de 1M eventos/dia × preço por chamada = a conta que o arquiteto faz ANTES)
- Tratamento de erro incluído no desenho: timeout/falha do provedor não pode derrubar o pipeline — o padrão DLQ (Curso 3) e retries valem para a IA também

### 2.4 As funções embutidas: ML sem chave externa *(~4 min)*

Nem todo caso precisa de LLM — a plataforma embute modelos de ML clássicos como funções do Flink SQL:

| Função | O que faz | Modelo por trás | Caso do bootcamp |
|---|---|---|---|
| `ML_FORECAST` | Previsão de série temporal | TimesFM (Google Research) | Prever o volume de pedidos da próxima hora |
| `AI_DETECT_ANOMALIES` | Anomalia em série temporal | TimesFM | O pico estranho de pedidos às 3h da manhã |
| Sentimento / PII detection | Classificação e redação embutidas | Funções gerenciadas (conferir status — EA) | Mascarar PII **antes** de mandar texto pro LLM |

- **Zero chave externa, zero conta em provedor** — rodam na plataforma: é o caminho da demo reproduzível pelo aluno
- A régua de escolha: **tarefa estatística sobre números → função embutida; entendimento de linguagem/raciocínio → LLM remoto**. Usar LLM para detectar anomalia numérica é pagar caro pelo martelo errado
- E a composição elegante: PII redaction embutida **antes** do ML_PREDICT remoto — o dado sensível não sai de casa (gancho direto da Live 3)

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Duas partes: a **embutida** (todo aluno reproduz — sem chave externa) e a **remota** (aluno com chave própria reproduz; sem chave, assiste). Tempo de tela: ~7 min. **Statements parados ao final.**

**O que a demo deve mostrar, nesta ordem:**

1. **[Embutida] Anomalia no fluxo de pedidos** — `AI_DETECT_ANOMALIES` sobre a contagem de pedidos por minuto (a agregação do Curso 4 alimentando a função); injetar um pico artificial via Datagen.
   - **Observação-alvo:** o pico marcado como anomalia **em segundos, sem nenhum modelo configurado** — ML de série temporal como função SQL nativa. (E a fraude do capstone ganhou seu primeiro detector.)

2. **[Remota] Declarar a connection e o CREATE MODEL** — criar a connection com a API key do provedor (o segredo entrando como recurso, fora do SQL), e o modelo de classificação (ex.: sentimento sobre um campo textual do dataset — placeholder até a modelagem fechar; alternativa: classificar a descrição do pedido em categorias).
   - **Observação-alvo:** a anatomia da seção 2.2 viva — modelo tipado no catálogo, credencial separada, system prompt como config.

3. **[Remota] ML_PREDICT ao vivo** — a query pontuando eventos; o score/classe anexado a cada linha na tela.
   - **Observação-alvo:** o momento-síntese — **um LLM respondendo dentro de um SELECT contínuo**: cada evento novo chega já classificado.

4. **[Remota] O resultado vira evento** — CTAS materializando `pedidos_classificados` (ou equivalente); mostrar o tópico novo com o campo de IA no schema.
   - **Observação-alvo:** a predição não morreu numa resposta de API — virou **dado de primeira classe**: com contrato, no fluxo, pronta pra sinks, Tableflow e para as Aulas 3–4.

5. **Teardown** — parar statements; **destacar o custo duplo**: statement Flink rodando + chamadas ao provedor por evento — a IA no fluxo é o recurso mais caro de esquecer ligado no bootcamp inteiro.

**Fechamento na tela** *(~1 min)*: "o modelo entrou no pipeline e o score virou evento. Mas classificar é só metade do RAG — falta o retrieval semântico: transformar eventos em vetores e buscá-los por significado. Embeddings e Vector Search, na próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"A plataforma fornece o modelo de LLM."** Não — é BYO: sua conta, sua chave, seu rate limit, seu custo. A plataforma fornece a **orquestração + o contexto governado**. (As funções embutidas de série temporal são a exceção que confirma: modelos gerenciados para tarefas estatísticas.)
- **"ML_PREDICT em tudo."** Inferência é a transformação mais cara — filtrar antes, pontuar o necessário, e escolher função embutida quando a tarefa é numérica. Arquiteto de streaming agora faz conta de token.
- **"A chave do provedor vai no WITH do modelo."** Vai na **connection** — recurso separado, mesmo tratamento das API keys do Curso 2. Chave em SQL versionado é a nova senha no Git.
- **"O resultado da IA é a resposta da API."** No fluxo, o resultado é **evento com schema** — reutilizável, auditável, replayável. Essa é a diferença entre "usar IA" e "ter IA na arquitetura".
- **Status e provedores evoluem rápido** (funções EA, novos providers) — o markdown marca o status na gravação; conferir a doc no uso.

**Ganchos com a trilha:**

- Embeddings como caso especial de inferência (modelo que devolve vetor) → **Aula 3** (na sequência).
- Scores alimentando agentes que **agem** → **Aula 4**.
- PII redaction antes do LLM remoto → **Live 3** (LGPD em pipelines).
- O custo por evento e o monitoramento de statements de IA → **Curso 7**.

**Perguntas para checar entendimento:**

1. Detectar pico anômalo no volume de transações × classificar a intenção do texto de um chamado: qual mecanismo para cada, e por que a resposta errada custa caro (em dinheiro) na primeira?
2. Por que a predição materializada num tópico (cena 4) vale mais que a mesma predição devolvida numa resposta de API síncrona? Cite dois consumidores possíveis.

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Model inference (inferência)** | Executar um modelo treinado sobre dados novos — no Flink, como operador do pipeline. |
| **CREATE MODEL** | Declara um modelo remoto como recurso tipado do catálogo Flink (provider, task, I/O). |
| **Connection** | O recurso que guarda a credencial do provedor — separado do modelo e das queries. |
| **ML_PREDICT** | A função de tabela que aplica o modelo a cada linha do stream. |
| **Provider (BYO-model)** | O provedor do modelo remoto (OpenAI, Bedrock, Vertex, Azure, Anthropic) — conta e chave suas. |
| **ML_FORECAST / AI_DETECT_ANOMALIES** | Funções embutidas de série temporal (TimesFM) — sem chave externa. |
| **TimesFM** | Foundation model do Google Research para séries temporais, motor das funções embutidas. |
| **System prompt (no modelo)** | A instrução fixa do caso de uso, declarada na configuração do modelo. |
| **Score/predição como evento** | O resultado da inferência materializado em tópico com schema — dado de primeira classe. |

---

## 6. Referências

**Documentação e artigos oficiais:**

- Confluent Cloud — *AI Model Inference no Flink SQL (CREATE MODEL, ML_PREDICT)* — https://docs.confluent.io/cloud/current/flink/reference/statements/create-model.html
- Confluent Cloud — *Run an AI Model (guia dos providers)* — https://docs.confluent.io/cloud/current/flink/how-to-guides/run-ai-model.html
- Confluent Cloud — *Built-in ML Functions (forecast, anomalias)* — https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html
- Confluent — *New in Confluent Intelligence (funções TimesFM, PII, sentimento — o post de status)* — https://www.confluent.io/blog/2026-q2-confluent-intelligence-update/
- Google Research — *TimesFM (o foundation model de séries temporais)* — https://github.com/google-research/timesfm
