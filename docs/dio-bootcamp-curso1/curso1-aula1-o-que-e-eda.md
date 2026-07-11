# Curso 1 · Aula 1 — O que é EDA? Evento, Mensagem e Comando

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~15 min · Aula 100% conceitual (a prática começa na Aula 2)

---

## 1. Objetivo da Aula

Construir o modelo mental de **Event-Driven Architecture (EDA)** e fixar o vocabulário que sustenta todo o bootcamp: **evento**, **mensagem** e **comando** — e por que essa distinção muda a forma de projetar sistemas.

---

## 2. Conceitos Simplificados

### 2.1 O ponto de partida: o mundo request/response *(~3 min)*

Sistemas tradicionais conversam por **chamadas diretas e síncronas**: o Checkout chama o Pagamento, que chama o Estoque, que chama a Notificação — e cada um **espera** a resposta do próximo para continuar.

O problema aparece na narrativa do e-commerce:

> A Notificação caiu. O Estoque, que a esperava, trava. O Pagamento, que esperava o Estoque, trava. O pedido inteiro **falha por causa de um e-mail**.

Três dores nascem daí:

- **Acoplamento** — cada serviço precisa conhecer o endereço, o contrato e a disponibilidade do próximo
- **Fragilidade em cascata** — a falha de um derruba a cadeia inteira
- **Rigidez** — incluir um novo interessado (ex.: antifraude) exige alterar quem já existia

### 2.2 A virada: comunicar por fatos *(~3 min)*

EDA inverte a lógica: em vez de **pedir** que outros façam algo, o sistema **anuncia o que aconteceu** — e quem tiver interesse, reage.

> O Checkout publica o fato `PedidoCriado` e segue a vida. Pagamento, Estoque, Notificação e o antifraude (que chegou depois, sem ninguém alterar nada) reagem cada um no seu ritmo. A Notificação caiu? Quando voltar, processa o que ficou pendente — o pedido nunca soube da falha.

**Analogia do jornal:** o jornalista publica a notícia; não liga para cada leitor. Quem tem interesse assina e lê no seu ritmo. Request/response é o telefonema — se o outro lado não atende, a comunicação falha.

O que se ganha:

- **Desacoplamento** — quem publica não conhece quem consome
- **Resiliência** — a falha de um consumidor não contamina os demais
- **Extensibilidade** — novos consumidores entram sem tocar no produtor
- **Tempo real** — a informação flui no instante em que o fato ocorre

### 2.3 O vocabulário: evento, comando e mensagem *(~6 min)*

**Evento** — um **fato que já aconteceu**, imutável, nomeado no **passado**. Não tem destinatário: 0, 1 ou N interessados podem reagir.

```json
{
  "event_type": "PedidoCriado",
  "occurred_at": "2026-07-11T14:32:07Z",
  "order_id": "ord-98123",
  "customer_id": "cli-4471",
  "total": 289.90
}
```

*Analogia:* o lançamento no extrato bancário — registra o que ocorreu, ninguém edita uma linha do extrato. Se houve erro, entra um **novo** lançamento de estorno. (Guarde essa ideia de "registro imutável em sequência": ela é a essência do log do Kafka, Aula 3.)

**Comando** — uma **intenção**: alguém pede que **um** serviço específico faça algo. Nomeado no **imperativo**, tem um destinatário e **pode ser rejeitado** (estoque insuficiente, cartão negado).

```json
{
  "command_type": "CriarPedido",
  "customer_id": "cli-4471",
  "items": [ { "sku": "ABC-123", "qty": 2 } ]
}
```

**Mensagem** — o **envelope genérico** que transporta o conteúdo entre sistemas. Todo evento e todo comando viajam dentro de uma mensagem — mas a mensagem em si é só transporte.

```
┌─────────────────── MENSAGEM ───────────────────┐
│  headers   → metadados (origem, trace, tipo)   │
│  key       → identidade (ex.: order_id)        │
│  payload   → o conteúdo: evento OU comando     │
│  timestamp → quando entrou no sistema          │
└────────────────────────────────────────────────┘
```

*Analogia:* a carta e o envelope. O evento/comando é a carta; a mensagem é o envelope com remetente e carimbo. O correio (a plataforma de streaming) transporta envelopes — **ele não abre a carta**.

### 2.4 A régua de comparação *(~2 min)*

| | **Evento** | **Comando** |
|---|---|---|
| Tempo verbal | Passado (`PedidoCriado`) | Imperativo (`CriarPedido`) |
| Intenção | Informar um fato | Pedir uma ação |
| Destinatário | Ninguém em específico (0..N) | Um serviço específico |
| Pode ser rejeitado? | Não — já aconteceu | Sim — é só uma intenção |
| Acoplamento | Baixo | Mais alto (conhece o destino) |

**Fechamento da aula** *(~1 min)*: "todo o bootcamp acontece dentro desse modelo — sistemas publicando fatos e reagindo a fatos. Na próxima aula, colocamos a mão na massa: os dois jeitos clássicos de distribuir mensagens, **Message Queue vs Data Streaming**, rodando num Kafka local."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Evento e mensagem são a mesma coisa."** A mensagem é o envelope; o evento é o conteúdo. Essa distinção volta a cobrar caro no Schema Registry (Curso 2), onde o contrato é sobre o **payload**.
- **"EDA elimina o síncrono."** Não elimina — convivem. Consulta de saldo continua request/response; o que muda é a comunicação **entre domínios** e a propagação de fatos.
- **"É só trocar REST por fila que virou EDA."** EDA é modelo de pensamento (fatos imutáveis, reação, desacoplamento), não troca de transporte. Fica evidente na Aula 6 (Do Monólito ao EDA).
- **Nomenclatura no passado não é frescura** — força o time a modelar **fatos**, e fatos imutáveis são o que permite replay e auditoria (a Aula 3 mostra isso materializado no log).

**Ganchos com a trilha:**

- Evento × comando volta com força na **Live 2** (Event Sourcing, CQRS e Saga).
- A anatomia da mensagem (key, payload, headers) é o que o aluno produz de verdade no **Curso 2, Aula 4**.
- Imutabilidade + fatos no passado preparam o **log append-only** (Aula 3).

**Perguntas para checar entendimento:**

1. `EnviarEmailDeBoasVindas` é um evento ou um comando? Como você o transformaria no outro?
2. Por que um evento não pode ser "rejeitado" pelo consumidor?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **EDA (Event-Driven Architecture)** | Arquitetura em que sistemas se comunicam publicando e reagindo a eventos, em vez de chamadas diretas. |
| **Event (evento)** | Fato imutável que já aconteceu, nomeado no passado. Não tem destinatário específico. |
| **Command (comando)** | Intenção dirigida a um serviço específico, no imperativo. Pode ser rejeitada. |
| **Message (mensagem)** | Envelope que transporta o payload (evento ou comando), com key, headers e timestamp. |
| **Producer (produtor)** | Sistema que publica mensagens. |
| **Consumer (consumidor)** | Sistema que lê e reage às mensagens publicadas. |
| **Payload** | O conteúdo útil da mensagem — o evento ou comando em si. |
| **Decoupling (desacoplamento)** | Produtor e consumidor não se conhecem e evoluem de forma independente. |
| **Request/Response** | Modelo síncrono tradicional: quem chama espera a resposta para continuar. |

---

## 5. Referências

**Livros:**

- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — os capítulos iniciais fundamentam evento como fato e o desacoplamento por log. E-book gratuito: https://www.confluent.io/designing-event-driven-systems/
- Adam Bellemare — *Building Event-Driven Microservices* (O'Reilly) — cap. 1: eventos como fonte da verdade na comunicação entre domínios
- Gregor Hohpe & Bobby Woolf — *Enterprise Integration Patterns* (Addison-Wesley) — a taxonomia clássica Message / Command Message / Event Message (base da distinção desta aula)

**Documentação e artigos oficiais:**

- Martin Fowler — *What do you mean by "Event-Driven"?* — https://martinfowler.com/articles/201701-event-driven.html (a distinção event notification × event-carried state × command)
- Confluent — *What is Event-Driven Architecture?* — https://www.confluent.io/learn/event-driven-architecture/
- Apache Kafka — *Introduction* — https://kafka.apache.org/intro
