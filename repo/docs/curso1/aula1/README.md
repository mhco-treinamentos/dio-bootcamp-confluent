# Curso 1 · Aula 1 — O que é EDA? Evento, mensagem e comando

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~20 min · 📄 Exercício em papel — **sem Docker, sem terminal**

Esta aula é 100% conceitual: você não roda nada. O que se constrói aqui é o **vocabulário** que todas as próximas aulas assumem que você tem — e que, mal entendido, cobra caro lá na frente (o contrato do Schema Registry, no Curso 2, é sobre exatamente uma das três palavras deste guia).

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter assistido à aula | ~15 min |
| Papel e caneta, ou um arquivo de texto | Você vai escrever ~15 linhas |
| Instalar alguma coisa | **Nada.** Sem Python, sem Docker |

> 💡 A partir da **Aula 2** você vai precisar do sandbox local (`make up`).
> Se quiser adiantar o setup, veja [PYCHARM.md](../../../PYCHARM.md).

> Os exemplos aqui usam **payments** (transações de cartão e PIX), o mesmo domínio
> das demos das Aulas 2 a 5. A aula gravada usa exemplos de e-commerce — o conceito
> é idêntico, só muda o domínio.

---

## O que você vai saber fazer ao final

- Classificar uma mensagem de payments como **evento** ou **comando**, e justificar em uma frase
- Converter um comando no evento correspondente (e vice-versa)
- Identificar as quatro partes do envelope de uma mensagem, num exemplo real
- Explicar por que um evento **não pode ser rejeitado** por quem o lê

---

## Os conceitos em 90 segundos

EDA troca *"peço que você faça"* por *"anuncio o que aconteceu"*. Quem publica não sabe quem escuta; quem escuta reage no seu ritmo. Disso vem o desacoplamento — e também a regra que confunde todo mundo no começo: um fato consumado não se recusa.

**A régua** — consulte durante os exercícios:

| | **Evento** | **Comando** |
|---|---|---|
| Tempo verbal | Passado (`TransacaoAutorizada`) | Imperativo (`AutorizarTransacao`) |
| Intenção | Informar um fato | Pedir uma ação |
| Destinatário | Ninguém em específico (0..N) | Um serviço específico |
| Pode ser rejeitado? | Não — já aconteceu | Sim — é só uma intenção |
| Acoplamento | Baixo | Mais alto (conhece o destino) |

**O envelope** — o que a plataforma transporta:

```
┌─────────────────── MENSAGEM ───────────────────┐
│  headers   → metadados (origem, trace, tipo)   │
│  key       → identidade (ex.: transaction_id)  │
│  payload   → o conteúdo: evento OU comando     │
│  timestamp → quando entrou no sistema          │
└────────────────────────────────────────────────┘
```

> 📖 As analogias (o jornal, o extrato bancário, a carta e o envelope) e a narrativa
> das três dores do request/response estão **na aula gravada**. Este guia assume que
> você já assistiu e vai direto ao exercício.

---

## Passo a passo

### Passo 1 — Classifique 8 mensagens *(~6 min)*

**Objetivo**: separar fato consumado de intenção.

**O que fazer**: copie a tabela e, para cada nome, escreva `E` (evento) ou `C` (comando) e uma justificativa de até 8 palavras.

| # | Nome | E ou C? | Por quê? |
|---|---|---|---|
| 1 | `AutorizarTransacao` | | |
| 2 | `TransacaoAutorizada` | | |
| 3 | `BloquearCartao` | | |
| 4 | `CartaoBloqueado` | | |
| 5 | `PagamentoRecusado` | | |
| 6 | `EnviarNotificacaoDeFraude` | | |
| 7 | `LimiteDeContaAtualizado` | | |
| 8 | `EstornarTransacao` | | |

**Entregável**: 8 linhas classificadas e justificadas.

**Como saber que acertou**: o teste de bolso é perguntar *"posso recusar isso?"*. Se sim, é comando. Gabarito mais abaixo — responda as 8 antes de abrir.

---

### Passo 2 — Converta comando em evento (e vice-versa) *(~4 min)*

**Objetivo**: perceber que a conversão é sobre **quem decide**, não sobre trocar o nome.

**Exemplo resolvido** (o par 3/4 do passo anterior):

```
Comando: BloquearCartao     → o antifraude PEDE ao serviço de cartões
Evento:  CartaoBloqueado    → o serviço de cartões ANUNCIA que bloqueou
Quem decide: no comando, o antifraude propõe; no evento, cartões já decidiu.
```

**O que fazer**: faça o mesmo para os itens **1**, **6** e **8**. Para cada par, responda: *quem decide?*

**Entregável**: 3 pares, cada um com a frase "quem decide".

**Como saber que acertou**: se o seu evento ainda puder ser recusado por quem lê, ele continua sendo um comando disfarçado.

---

### Passo 3 — Preencha o envelope *(~5 min)*

**Objetivo**: separar o conteúdo (o evento) do transporte (a mensagem).

Dado este payload:

```json
{
  "event_type": "TransacaoAutorizada",
  "occurred_at": "2026-07-11T14:32:07Z",
  "transaction_id": "txn-1001",
  "account_id": "acc-4471",
  "channel": "CREDIT",
  "amount": "150.00",
  "status": "AUTHORIZED"
}
```

> 💡 Este payload não é ilustração. `txn-1001`, `150.00` e o ciclo
> `CREATED → AUTHORIZED → SETTLED` são exatamente os dados que você vai ver rodando
> no terminal na **Aula 5** ([`aula5_ordem_e_idempotencia.py`](../../../demos/curso1/aula5_ordem_e_idempotencia.py)).

**O que fazer**: preencha as quatro caixas.

```
headers   → ______________________________
key       → ______________________________
payload   → ______________________________
timestamp → ______________________________
```

E responda:

- **(a)** Qual campo do JSON você usaria como `key`? Por quê?
- **(b)** `occurred_at` e o `timestamp` da mensagem são a mesma coisa?
- **(c)** Se o antifraude e o extrato leem esta mensagem, quantas cópias do payload existem?

**Entregável**: envelope preenchido + 3 respostas.

**Como saber que acertou**: gabarito abaixo. A pergunta (a) não é detalhe — é literalmente a decisão que determina a ordem das mensagens na Aula 5.

---

### Passo 4 — As duas perguntas de checagem *(~5 min)*

Escreva 2 a 3 linhas para cada:

1. `EnviarNotificacaoDeFraude` é evento ou comando? Como você o transformaria no outro?
2. Por que um evento **não pode** ser "rejeitado" pelo consumidor?

> ⚠️ Escreva antes de abrir o gabarito. Reconhecer a resposta certa não é o mesmo
> que produzi-la — e a pergunta 2 é a que mais separa quem entendeu de quem achou
> que entendeu.

---

## Gabarito

<details>
<summary><b>Passo 1 — classificação (clique para abrir)</b></summary>

| # | Nome | Resposta | Por quê |
|---|---|---|---|
| 1 | `AutorizarTransacao` | **C** | Imperativo; o emissor pode recusar (saldo insuficiente) |
| 2 | `TransacaoAutorizada` | **E** | Passado; já aconteceu, ninguém desfaz |
| 3 | `BloquearCartao` | **C** | Pedido dirigido ao serviço de cartões |
| 4 | `CartaoBloqueado` | **E** | Fato consumado |
| 5 | `PagamentoRecusado` | **E** | A recusa **já ocorreu** — é um fato |
| 6 | `EnviarNotificacaoDeFraude` | **C** | Imperativo, destinatário único |
| 7 | `LimiteDeContaAtualizado` | **E** | Passado |
| 8 | `EstornarTransacao` | **C** | Intenção; o evento é `TransacaoEstornada` |

**O item 5 é o mais errado de todos.** Muita gente marca `C` porque "recusado" soa
negativo. Mas negativo não é o mesmo que rejeitável: `PagamentoRecusado` descreve
uma decisão **que já foi tomada**. Ninguém pode "recusar a recusa" — só reagir a ela.

</details>

<details>
<summary><b>Passo 2 — conversão</b></summary>

```
1. Comando: AutorizarTransacao        → o adquirente PEDE autorização ao emissor
   Evento:  TransacaoAutorizada       → o emissor ANUNCIA que autorizou
   Quem decide: o emissor. No comando ele ainda pode negar; no evento, já decidiu.

6. Comando: EnviarNotificacaoDeFraude → o antifraude PEDE que notifiquem o cliente
   Evento:  FraudeDetectada           → o antifraude ANUNCIA o que encontrou
   Quem decide: quem publica FraudeDetectada não decide se haverá notificação —
   isso vira problema de quem escuta. É aqui que o desacoplamento aparece:
   amanhã entram SMS, push e bloqueio automático, sem tocar no antifraude.

8. Comando: EstornarTransacao         → o atendimento PEDE o estorno
   Evento:  TransacaoEstornada        → o serviço de pagamentos ANUNCIA que estornou
   Quem decide: pagamentos. O pedido pode ser negado; o estorno consumado, não.
```

O par 6 é o mais instrutivo: repare que o bom evento (`FraudeDetectada`) **não é**
a tradução literal do comando (`NotificacaoDeFraudeEnviada` também existiria, mas
descreve outro fato — o envio, não a detecção). Converter bem exige perguntar
*qual fato de negócio aconteceu*, não apenas passar o verbo para o particípio.

</details>

<details>
<summary><b>Passo 3 — envelope</b></summary>

```
headers   → origem (servico-autorizacao), trace id, tipo do evento, versão do schema
key       → "txn-1001"  (o transaction_id)
payload   → o JSON inteiro
timestamp → quando a mensagem entrou no broker
```

**(a) Qual campo como `key`?** `transaction_id`. A key é a **identidade da entidade
cujo ciclo de vida a mensagem descreve**. Com ela, todos os eventos de `txn-1001`
(`CREATED → AUTHORIZED → SETTLED`) caem na mesma partição e chegam **em ordem** —
exatamente o que a Aula 5 demonstra na tela.

Se a sua resposta foi `account_id`, não está errada: é uma escolha de design
diferente, que garante ordem por **conta** em vez de por transação. A pergunta certa
é sempre *"o que precisa estar em ordem?"*.

**(b) `occurred_at` e `timestamp` são a mesma coisa?** Não. `occurred_at` é **tempo
de negócio** (quando a transação foi autorizada); o `timestamp` da mensagem é
**tempo de ingestão** (quando chegou ao broker). Normalmente diferem por
milissegundos — mas podem diferir por horas, se o sistema de origem ficou offline.
Essa diferença tem nome (*lateness*) e é o assunto das janelas de Flink, no Curso 4.

**(c) Quantas cópias do payload?** **Uma.** O log é lido por N consumidores sem ser
copiado nem consumido — é a diferença entre fila e streaming, que a Aula 2 mostra
rodando.

</details>

<details>
<summary><b>Passo 4 — perguntas de checagem</b></summary>

**1. `EnviarNotificacaoDeFraude` é evento ou comando?**

Comando: verbo no imperativo, destinatário específico (o serviço de notificações) e
pode ser recusado (cliente sem canal cadastrado, política de silêncio noturno).

Para virar evento, pergunte qual fato o originou: `FraudeDetectada`. A diferença
prática é grande — com o comando, o antifraude precisa saber que existe um serviço
de notificações; com o evento, ele anuncia o que viu e **qualquer número** de
interessados reage (SMS, push, bloqueio de cartão, dashboard), sem que o antifraude
seja alterado.

**2. Por que um evento não pode ser rejeitado?**

Porque não é um pedido: é um registro do que já aconteceu. Quem consome pode
**ignorar**, **falhar ao processar** ou **reagir com uma compensação**
(`TransacaoEstornada` desfaz o efeito de negócio) — mas não pode fazer o fato não
ter ocorrido.

Rejeitar pressupõe autoridade sobre a decisão, e a decisão já foi tomada por quem
publicou. É a mesma lógica do extrato bancário: você não apaga um lançamento errado,
você lança um estorno.

</details>

---

## Erros comuns

| Se você… | O que está confundindo |
|---|---|
| classificou `PagamentoRecusado` como comando | Recusa passada é um fato; "negativo" não é o mesmo que "rejeitável" |
| escreveu o payload inteiro no campo `key` | A key é identidade, não conteúdo |
| traduziu `EnviarNotificacaoDeFraude` para `NotificacaoEnviada` | Converteu o verbo, não o fato de negócio — o evento de origem é `FraudeDetectada` |
| respondeu que evento e mensagem são a mesma coisa | O envelope é transporte; o contrato do Schema Registry (Curso 2) é sobre o **payload** |
| concluiu que EDA elimina o síncrono | Consulta de saldo continua request/response — o que muda é a comunicação **entre domínios** |

---

## Glossário

| Termo | Definição |
|---|---|
| **Evento** | Fato imutável que já aconteceu, nomeado no passado. Sem destinatário específico. |
| **Comando** | Intenção dirigida a um serviço específico, no imperativo. Pode ser rejeitada. |
| **Mensagem** | Envelope que transporta o payload, com key, headers e timestamp. |
| **Payload** | O conteúdo útil da mensagem — o evento ou comando em si. |
| **Key** | Campo que identifica a entidade; determina a partição e, com isso, a ordem. |

O glossário completo da aula (EDA, producer, consumer, decoupling, request/response) está no material da aula gravada.

---

## Para ir além

**Onde isso reaparece**

| Conceito desta aula | Volta em |
|---|---|
| Imutabilidade do evento | Aula 3 — o log append-only |
| `key = transaction_id` → ordem | Aula 5 — ordem por chave |
| Contrato é sobre o payload | Curso 2, Aula 3 — Schema Registry |
| Tempo de negócio × tempo de ingestão | Curso 4 — janelas e watermarks no Flink |
| Evento × comando | Live 2 — Event Sourcing, CQRS e Saga |

**Fontes**

- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — [e-book gratuito](https://www.confluent.io/designing-event-driven-systems/)
- Adam Bellemare — *Building Event-Driven Microservices* (O'Reilly), cap. 1
- Gregor Hohpe & Bobby Woolf — *Enterprise Integration Patterns* — a taxonomia clássica Message / Command / Event
- Martin Fowler — [*What do you mean by "Event-Driven"?*](https://martinfowler.com/articles/201701-event-driven.html)
- Confluent — [*What is Event-Driven Architecture?*](https://www.confluent.io/learn/event-driven-architecture/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| — | [Curso 1](../README.md) | [Aula 2 · Message Queue vs Data Streaming](../aula2/README.md) |
