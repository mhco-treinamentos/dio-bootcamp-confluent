# Curso 3 · Aula 4 — Resiliência e Tratamento de Erros: Single Message Transforms (SMT) e o padrão Dead Letter Topic (DLT/DLQ)

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aulas 1–3 do Curso 3

---

## 1. Objetivo da Aula

Fechar o curso com o que separa pipeline de demo de pipeline de produção: **SMTs** — transformações leves na passagem, sem processador no meio — e o padrão **Dead Letter Topic (DLT/DLQ)**: o destino digno da mensagem venenosa, que mantém o pipeline vivo sem perder dado. E elevar o DLT ao seu devido lugar: **padrão geral de resiliência em EDA**, não um recurso exclusivo do Connect.

---

## 2. Conceitos Simplificados

### 2.1 Anatomia de uma SMT chain *(~4 min)*

A **Single Message Transform** é uma transformação aplicada **mensagem a mensagem, na passagem pelo conector** — sem estado, sem janela, sem processador externo:

```
 SOURCE:  fonte ─► [SMT 1] ─► [SMT 2] ─► ... ─► topic
 SINK:    topic ─► [SMT 1] ─► [SMT 2] ─► ... ─► destino
              (a cadeia roda DENTRO do conector, na ordem declarada)
```

As SMTs do dia a dia:

| SMT | O que faz | Caso típico |
|---|---|---|
| `ExtractNewRecordState` (Debezium) | **Achata o envelope CDC** → só o `after`, limpo | O consumidor de negócio que não quer `before/op/source` (a promessa da Aula 2, cumprida) |
| `ExtractField` | Extrai um campo (ex.: da key) | Key estruturada → key simples |
| `MaskField` | Mascara valores sensíveis | CPF/e-mail não vazando pro destino (LGPD — gancho da Live 3) |
| `InsertField` | Injeta metadado (timestamp, origem) | Rastreabilidade barata |
| `RegexRouter` | Reescreve o nome do tópico destino | `server1.public.pedidos` → `pedidos` |
| `Filter` (+ predicates) | Descarta mensagens por condição | Ignorar heartbeats/ruído |

A régua de decisão — **SMT × Flink**:

> SMT é para transformação **1-para-1, sem estado, cosmética**: renomear, mascarar, achatar, rotear. No momento em que a necessidade envolve **juntar, agregar, janelar ou cruzar fluxos** — é Flink (Curso 4). SMT com lógica de negócio é gambiarra com prazo de validade.

### 2.2 O problema da mensagem venenosa *(~3 min)*

Pipeline rodando, e chega **uma** mensagem que o conector não consegue processar — payload corrompido, schema inesperado, valor que o destino rejeita. As três respostas possíveis:

```
 errors.tolerance=none  → conector FALHA          (1 veneno para o pipeline inteiro)
 "ignora e segue"       → mensagem DESCARTADA      (perda silenciosa — o pior erro)
 errors.tolerance=all
   + DLQ configurada    → veneno DESVIADO pro DLT  (pipeline vivo, dado preservado) ✓
```

- **Falhar tudo** é o default a evitar: uma mensagem ruim para a ingestão da empresa — o anti-padrão "connector sem DLQ"
- **Descartar em silêncio** é pior: o dado some e ninguém sabe — viola o "não perder nunca" do Curso 1
- **Desviar** é o padrão maduro: o fluxo bom segue, o veneno fica **guardado, identificado e reprocessável**

### 2.3 Anatomia do fluxo DLT *(~4 min)*

```
 topic ─► SINK CONNECTOR ─────────────────► destino
              │ falhou (parse/conversão/escrita)
              │ retry esgotado
              ▼
        DEAD LETTER TOPIC (ex.: dlq-pedidos-sink)
          a mensagem ORIGINAL, intacta, + HEADERS de contexto:
          ├─ __connect.errors.topic          (de onde veio)
          ├─ __connect.errors.partition/offset (posição exata)
          ├─ __connect.errors.exception.class + message (o PORQUÊ)
          └─ __connect.errors.stage           (onde no pipeline quebrou)
```

- **A mensagem vai intacta** — o DLT preserva o dado original; os **headers** carregam o diagnóstico (o quê, onde, por quê)
- **DLT é um tópico normal** — tem retention, pode ter consumidor, alerta e dashboard. **DLT crescendo = sintoma**; monitorá-lo é operação básica (Curso 7)
- **O ciclo de vida do veneno:** desviado → diagnosticado (headers) → corrigido (o produtor, o schema ou o dado) → **reprocessado** (um consumidor lê o DLT e reinjeta) ou expirado conscientemente

### 2.4 DLT/DLQ como padrão geral de EDA *(~3 min)*

O nome vem das filas (*Dead Letter Queue* — a carta que não pôde ser entregue), e no ecossistema Kafka **DLT e DLQ são sinônimos** — usaremos os dois. O ponto arquitetural da aula:

- O padrão **não é do Connect** — ele já apareceu duas vezes no bootcamp: a **DLQ action das Data Quality Rules** (Curso 2, Aula 3 — o contrato desviando o valor inválido) e o gancho do **Curso 1, Aula 5** ("o que fazer com a falha de verdade")
- **Qualquer consumidor** pode (e em produção, deve) implementar o mesmo desenho: try → retry com critério → desviou pro DLT com contexto → segue o fluxo
- A pergunta de design que o padrão responde: **"qual é o destino digno da mensagem que não consigo processar agora?"** — e a resposta nunca pode ser "trava tudo" nem "some com ela"

**Configurações de referência — tratamento de erros (sink):**

| Config | O que faz | Ponto didático |
|---|---|---|
| `errors.tolerance` | `none` (falha) × `all` (tolera e desvia) | A escolha da seção 2.2 |
| `errors.deadletterqueue.topic.name` | O tópico DLT | Convenção de nome: `dlq-{conector}` |
| `errors.deadletterqueue.context.headers.enable` | Anexa os headers de diagnóstico | **Sempre true** — DLT sem contexto é cemitério |
| `errors.retry.timeout` / `errors.retry.delay.max.ms` | Retry antes de desviar | Erro transitório (rede) ≠ veneno; retry filtra |
| `errors.log.enable` | Loga cada erro tolerado | O rastro para o alerta |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: o fluxo sink da Aula 3 recriado, agora com SMT + DLQ. Tempo de tela: ~7 min. **Teardown completo ao final (fecha o curso).**

**O que a demo deve mostrar, nesta ordem:**

1. **Recriar o sink com uma SMT na cadeia** — o Postgres Sink da Aula 3 + `ExtractNewRecordState` (ou equivalente de achatamento no conector CDC source, conforme o desenho do projeto de demos): o destino recebendo o registro **limpo**, sem envelope.
   - **Observação-alvo:** antes/depois da SMT lado a lado — o envelope rico da Aula 2 virando a linha achatada que o negócio consome. Transformação por **configuração**.

2. **Configurar a DLQ no sink** — `errors.tolerance=all`, DLT nomeado (`dlq-pedidos-sink`), headers de contexto habilitados, retry configurado.
   - **Observação-alvo:** o formulário materializa a tabela de configs da seção 2.4 — três campos separam o pipeline frágil do resiliente.

3. **Envenenar de propósito** — injetar no tópico uma mensagem que o sink não consegue processar (ex.: payload fora do schema esperado pelo destino / tipo incompatível — o mecanismo exato fica a cargo do projeto de demos).
   - **Observação-alvo:** o **pipeline não cai** — status RUNNING, mensagens boas continuando a fluir pro Neon.

4. **Abrir o DLT** — a mensagem venenosa lá, intacta; inspecionar os **headers** (`exception.message`, tópico/partição/offset de origem, stage).
   - **Observação-alvo:** o momento-síntese — o diagnóstico completo viajou com o dado: dá pra saber o quê, onde e por quê **sem procurar em log nenhum**.

5. **Esboçar o reprocesso** — mostrar (conceitualmente na tela) o caminho de volta: corrigir a causa → consumidor lê o DLT → reinjeta no fluxo. Não precisa executar o reprocesso completo; o desenho na tela basta.
   - **Observação-alvo:** DLT não é lixeira — é fila de recuperação. O dado tem ciclo de vida.

6. **Teardown completo** — deletar conector(es) e tópicos de exercício; conferência de billing. Fim do curso com a casa limpa.

**Fechamento na tela** *(~1 min)*: "ingestão sem código, CDC do legado, entrega idempotente, transformação na passagem e veneno com destino digno — o encanamento de produção está completo. No Curso 4, os dados param de só *passar* e começam a ser **transformados de verdade**: Apache Flink. Antes, nos vemos na Live 2 — Event Sourcing, CQRS e Saga."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"SMT resolve transformação, então não preciso de Flink."** SMT é 1-para-1, sem estado, cosmética. Join, agregação, janela, cruzamento de fluxos = Flink (Curso 4). Lógica de negócio em SMT é dívida técnica declarada.
- **"errors.tolerance=all sem DLQ é tolerância."** É **perda silenciosa** — a mensagem some sem rastro. Tolerância de verdade = desvio com contexto. (E DLT sem headers é cemitério sem lápide.)
- **"DLT é coisa do Connect."** É padrão de EDA — Data Quality Rules (Curso 2), consumidores de aplicação, Flink: todos usam o mesmo desenho. O Connect só o entrega por configuração.
- **"Mensagem no DLT = incidente resolvido."** DLT crescendo é sintoma ativo: sem monitoramento (Curso 7) e sem processo de reprocesso, o desvio vira aterro sanitário.
- **DLQ nativa do Connect é do lado sink** — no source, o tratamento de erro tem outras nuances (o erro acontece antes de existir mensagem no Kafka); citar sem aprofundar, e notar que conectores fully managed específicos documentam seu comportamento.

**Ganchos com a trilha:**

- Transformação com estado (o que a SMT não faz) → **Curso 4** (Flink), já na próxima parada.
- DLQ action nas Data Quality Rules → **Curso 2, Aula 3** (o mesmo padrão, camada de contrato).
- Alertas sobre DLT e lag → **Curso 7, Aula 2**.
- No **Desafio Final**, tratamento de erros com DLT é critério de avaliação do README (padrões EDA aplicados).

**Perguntas para checar entendimento:**

1. Um lote com 999 mensagens boas e 1 corrompida chega ao sink. Descreva o destino de cada uma nas três configurações da seção 2.2 — e o custo de negócio de cada escolha.
2. Sua equipe quer mascarar o CPF antes de entregar ao Elasticsearch e também calcular o total por cliente por hora. Qual das duas tarefas é SMT, qual é Flink, e por quê?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **SMT (Single Message Transform)** | Transformação 1-para-1, sem estado, aplicada na passagem pelo conector. |
| **SMT chain (`transforms`)** | Cadeia ordenada de SMTs declarada na configuração. |
| **ExtractNewRecordState** | SMT do Debezium que achata o envelope CDC para o estado `after`. |
| **Predicate** | Condição que liga/desliga uma SMT por mensagem. |
| **Mensagem venenosa (poison pill)** | Mensagem que o processamento não consegue tratar e que, sem desvio, trava ou derruba o fluxo. |
| **Dead Letter Topic (DLT) / Dead Letter Queue (DLQ)** | Tópico de desvio para mensagens não processáveis — sinônimos no ecossistema Kafka. |
| **errors.tolerance** | Política do conector diante do erro: falhar (`none`) ou tolerar e desviar (`all`). |
| **Context headers** | Metadados de diagnóstico anexados à mensagem desviada (origem, exceção, estágio). |
| **Reprocessamento (replay do DLT)** | Ciclo de corrigir a causa e reinjetar as mensagens desviadas no fluxo. |

---

## 6. Referências

**Livros:**

- Adam Bellemare — *Building Event-Driven Microservices* (O'Reilly) — tratamento de falhas e deadletter em pipelines orientados a eventos
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 9 (confiabilidade em pipelines). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/

**Documentação e artigos oficiais:**

- Confluent — *Single Message Transforms for Managed Connectors* — https://docs.confluent.io/cloud/current/connectors/single-message-transforms.html
- Apache Kafka — *Connect Transforms (referência de SMTs)* — https://kafka.apache.org/documentation/#connect_transforms
- Debezium — *New Record State Extraction (achatamento do envelope)* — https://debezium.io/documentation/reference/stable/transformations/event-flattening.html
- Confluent — *Kafka Connect Deep Dive: Error Handling and Dead Letter Queues* (Robin Moffatt) — https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/
- KIP-298: Error Handling in Connect — https://cwiki.apache.org/confluence/display/KAFKA/KIP-298%3A+Error+Handling+in+Connect
