# Curso 1 · Aula 2 — Message Queue vs Data Streaming: modelos de mensageria e quando usar cada um

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~15 min · Conceito + demo (sandbox Docker — implementação no projeto de demos)

---

## 1. Objetivo da Aula

Diferenciar os dois grandes modelos de distribuição de mensagens — **Message Queue** e **Data Streaming** — entender o que cada um resolve, e descobrir que o Kafka atende aos dois mundos: consumer groups para distribuir trabalho, log retido para streaming, e os **share groups (Queues for Kafka)** trazendo semântica de fila nativa.

---

## 2. Conceitos Simplificados

### 2.1 Message Queue: distribuir trabalho *(~3 min)*

No modelo de fila, a mensagem é uma **tarefa a ser executada uma vez**: um produtor enfileira, **um** consumidor pega, processa e a mensagem **some da fila**.

- **Consumo destrutivo** — processou, apagou; a fila tende a ficar vazia
- **Competição** — vários workers competem; cada mensagem vai para **um** deles
- **Caso de uso clássico** — processamento de jobs: redimensionar imagens, enviar e-mails, cobrar cartão

**Analogia:** a senha do cartório. Cada senha é atendida por **um** guichê e morre ali — ninguém "reatende" a senha 42.

### 2.2 Data Streaming: publicar fatos *(~3 min)*

No modelo de streaming, a mensagem é um **fato registrado** (a Aula 1 inteira mora aqui): o produtor escreve num **log durável**, e **N consumidores independentes** leem, cada um controlando sua própria posição de leitura.

- **Consumo não-destrutivo** — ler não apaga; o dado fica retido pelo tempo configurado
- **Independência** — o time de antifraude e o de relatórios leem o mesmo fluxo sem se atrapalhar
- **Replay** — um consumidor novo (ou corrigido) pode voltar no tempo e reler tudo
- **Caso de uso clássico** — propagação de fatos entre domínios, analytics em tempo real, auditoria

**Analogia:** a fila é a senha do cartório; o streaming é o **jornal com acervo** — todo mundo lê a mesma edição, cada um no seu ritmo, e dá pra consultar edições antigas.

### 2.3 O equívoco central: "fila e tópico são a mesma coisa" *(~2 min)*

É o erro mais comum de quem chega ao Kafka vindo de RabbitMQ/SQS/ActiveMQ:

| | **Message Queue** | **Data Streaming (log)** |
|---|---|---|
| O que é a mensagem | Tarefa a executar | Fato registrado |
| Após o consumo | Apagada | Retida (retention) |
| Consumidores | Competem (1 mensagem → 1 worker) | Independentes (1 mensagem → N leitores) |
| Reler o passado | Impossível | Replay nativo |
| Pergunta que responde | "quem faz esse trabalho?" | "o que aconteceu?" |

### 2.4 E o Kafka? Os dois mundos na mesma plataforma *(~2 min)*

O Kafka nasceu como **log de streaming**, mas cobre a semântica de fila:

- **Consumer group** — dentro de um grupo, cada mensagem é entregue a **um** consumidor: distribuição de trabalho, como numa fila (mecânica detalhada na Aula 4)
- **Vários grupos** — cada grupo recebe **tudo**: o comportamento pub/sub de streaming
- **Share groups (Queues for Kafka)** — evolução recente (KIP-932): semântica de fila nativa, com consumidores além do número de partições e confirmação mensagem a mensagem — sem abrir mão do log retido

Mensagem-chave da aula: **não é "Kafka vs fila" — é entender qual semântica o seu problema pede.** Trabalho a distribuir → semântica de fila. Fato a propagar → streaming.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Sandbox: Kafka single-broker em KRaft via Docker. Tempo de tela: ~5 min.

**O que a demo deve mostrar, nesta ordem:**

1. **Setup mínimo visível** — o sandbox subindo com um comando só (o aluno vê que é um `docker compose up`; detalhes do ambiente ficam para a Aula 0 do Bloco 2). Um tópico `pedidos` criado.

2. **Cena 1 — semântica de fila (1 grupo, 2 consumidores):** um producer publica ~10 mensagens numeradas; dois consumidores no **mesmo** consumer group.
   - **Observação-alvo:** as mensagens se **dividem** entre os dois consumidores — nenhuma aparece nos dois. É a distribuição de trabalho da fila.

3. **Cena 2 — semântica de streaming (2 grupos):** os mesmos consumidores, agora em **grupos diferentes**, e o producer publica mais ~10 mensagens.
   - **Observação-alvo:** **os dois recebem tudo.** Mesma plataforma, mesmo tópico — mudou só o group — e a semântica virou pub/sub.

4. **Cena 3 — replay (o diferencial do log):** um consumidor novo, em um terceiro grupo, lendo **desde o início** (`from beginning`).
   - **Observação-alvo:** todas as mensagens das cenas anteriores reaparecem — nada foi apagado pelo consumo. Numa fila tradicional, esse consumidor chegaria e encontraria o vazio.

**Fechamento na tela** *(~1 min)*: recapitular apontando — "cena 1 foi fila, cena 2 foi streaming, cena 3 é o que só o log permite. Na próxima aula, abrimos o capô desse log: tópicos, partições e offsets."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Fila = tópico."** Fila apaga ao consumir; o log do Kafka retém e permite replay. É a tabela da seção 2.3 — vale gastar tempo nela.
- **"Kafka substitui o RabbitMQ em qualquer cenário."** Nem sempre foi verdade (roteamento complexo, prioridade por mensagem eram pontos fortes das filas dedicadas); os share groups encurtam essa distância, mas a resposta certa continua sendo "depende da semântica que o problema pede".
- **"Consumer group é um recurso de fila, então Kafka é uma fila."** O group dá a **distribuição** da fila, mas o dado continua no log — os outros grupos seguem enxergando tudo. É híbrido por design.
- **Cuidado com o tempo:** share groups merecem menção e contexto (é diferencial de conteúdo atualizado), mas não demo — a mecânica de consumo profunda é assunto da Aula 4.

**Ganchos com a trilha:**

- A mecânica que faz a Cena 1 funcionar (partições, rebalance, commit) é a **Aula 4** inteira.
- O "voltar no tempo" da Cena 3 se materializa em **offsets** na **Aula 3**.
- A escolha fila × streaming reaparece nos padrões da **Live 2** (Saga reage a eventos; comandos pedem semântica dirigida).

**Perguntas para checar entendimento:**

1. Um sistema de geração de boletos precisa que cada boleto seja gerado exatamente por um worker. Qual semântica? E o feed de "pedidos criados" que alimenta antifraude + relatórios + CRM?
2. Por que um consumidor novo consegue ler mensagens "antigas" no Kafka, mas não numa fila tradicional?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Message Queue** | Modelo em que mensagens são tarefas consumidas por um único worker e removidas após o processamento. |
| **Data Streaming** | Modelo em que mensagens são fatos gravados num log durável, lidos por N consumidores independentes. |
| **Pub/Sub (publish/subscribe)** | Padrão em que publicadores emitem mensagens e todos os assinantes interessados as recebem. |
| **Log** | Estrutura de dados append-only e imutável onde o Kafka armazena as mensagens (anatomia na Aula 3). |
| **Retention (retenção)** | Tempo (ou tamanho) pelo qual o Kafka mantém as mensagens no log, independentemente de consumo. |
| **Replay** | Capacidade de um consumidor reposicionar sua leitura e reprocessar mensagens passadas. |
| **Consumer group** | Conjunto de consumidores que dividem entre si as mensagens de um tópico (semântica de distribuição). |
| **Share group (Queues for Kafka)** | Recurso do Kafka (KIP-932) que traz semântica de fila nativa: mais consumidores que partições e confirmação por mensagem. |
| **Worker** | Consumidor que compete por tarefas em um modelo de fila. |

---

## 6. Referências

**Livros:**

- Gwen Shapira, Todd Palino, Rajini Sivaram & Krit Petty — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 1 ("Meet Kafka") posiciona o Kafka frente a sistemas de mensageria tradicionais. E-book gratuito via Confluent: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Gregor Hohpe & Bobby Woolf — *Enterprise Integration Patterns* (Addison-Wesley) — padrões Point-to-Point Channel (fila) e Publish-Subscribe Channel
- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — o log compartilhado como alternativa aos brokers de mensagens

**Documentação e artigos oficiais:**

- Apache Kafka — *Introduction* (Kafka como plataforma de streaming vs sistemas de mensageria) — https://kafka.apache.org/intro
- KIP-932: Queues for Kafka (share groups) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
- Confluent — *Kafka vs. Message Queue Systems* — https://www.confluent.io/learn/message-queue/
- Apache Kafka — documentação de consumer groups — https://kafka.apache.org/documentation/#intro_consumers
