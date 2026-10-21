# Curso 1 · Aula 6 — Do monólito ao EDA: quando migrar (e quando não)

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~25 min · 📄 Exercício em papel — **sem Docker, sem terminal**

A aula que fecha o Bloco 1 não ensina mais nenhuma mecânica: ensina a **decidir**. Você vai calcular por que uma cadeia de microsserviços síncronos fica menos confiável que o monólito que ela substituiu, e vai recomendar — com critério — quando *não* migrar.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter assistido à aula | ~15 min |
| Ter feito as [Aulas 1](../aula1/README.md) a [5](../aula5/README.md) | Esta aula usa tudo o que veio antes |
| Papel e caneta | Você vai desenhar e fazer uma conta |
| Instalar alguma coisa | **Nada** |

> 💡 Se você fez as aulas práticas, já tem a intuição do log, do consumer group e das
> garantias de entrega. Aqui esses conceitos viram **argumento de arquitetura**.

---

## O que você vai saber fazer ao final

- Calcular a disponibilidade composta de uma cadeia síncrona
- Identificar um "monólito distribuído" pelos sintomas
- Explicar qual propriedade do log quebra o acoplamento temporal
- Recomendar **ficar** no monólito quando for o certo — e dizer o que mudaria sua recomendação
- Desenhar uma migração incremental em vez de um big bang

---

## Os conceitos em 90 segundos

Três arquiteturas, e o que cada salto resolve:

| | **Monólito** | **Microsserviços síncronos** | **EDA** |
|---|---|---|---|
| Deploy | Um só, tudo junto | Independente por serviço | Independente por serviço |
| Comunicação | Chamada local | REST/gRPC — **um chama o outro** | Publica fato no log |
| Acoplamento temporal | N/A (mesmo processo) | **Todos de pé ao mesmo tempo** | Quebrado — o log espera |
| Novo interessado no dado | Alterar o código | Alterar quem publica | **Ninguém é alterado** |
| Consistência | ACID imediata | Distribuída (difícil) | Eventual (exige desenho) |

> 📖 A narrativa completa — as forças do monólito, o problema do monólito distribuído
> e os critérios de migração — está **na aula gravada**.

---

## Passo 1 — A conta que ninguém faz *(~6 min)*

**Objetivo**: quantificar por que a cadeia síncrona é frágil.

Uma empresa quebrou o monólito em 4 serviços que se chamam por REST, em cadeia:

```
Checkout ──► Pagamento ──► Estoque ──► Notificação
```

Cada um tem **99,5% de disponibilidade** — um número que qualquer time consideraria bom.

**Calcule**: se o fluxo só funciona quando os quatro estão de pé, qual a disponibilidade do fluxo inteiro?

```
disponibilidade_total = 0,995 × 0,995 × 0,995 × 0,995 = ?
```

Depois responda:

- **(a)** Quantas horas de indisponibilidade por mês isso representa? (considere 730h/mês)
- **(b)** O monólito original, com 99,5%, ficava fora quantas horas por mês?
- **(c)** A migração melhorou ou piorou a confiabilidade do fluxo?

**Entregável**: o percentual e as três respostas.

---

## Passo 2 — Diagnostique o monólito distribuído *(~6 min)*

**Objetivo**: reconhecer os sintomas antes de sugerir a cura.

A mesma empresa relata os problemas abaixo. Para cada um, marque se ele **existe**,
**some** ou **muda de forma** ao adotar EDA:

| # | Sintoma | Com EDA: existe / some / muda? |
|---|---|---|
| 1 | Uma indisponibilidade da Notificação derruba o checkout | |
| 2 | Incluir um serviço de antifraude exige alterar o Checkout | |
| 3 | A latência do checkout é a soma das 4 chamadas | |
| 4 | Não há como saber o que aconteceu em que ordem, ontem às 14h | |
| 5 | O time de Estoque não consegue fazer deploy sem avisar os outros | |
| 6 | Um pedido pode ficar pago sem baixa no estoque, se algo falhar no meio | |

**Entregável**: 6 marcações com uma justificativa curta cada.

---

## Passo 3 — Desenhe a migração *(~7 min)*

**Objetivo**: sair do "vamos migrar tudo" para um plano executável.

A empresa quer adotar EDA, mas **não pode parar** o negócio. O monólito continua no ar.

Desenhe (em papel mesmo) uma migração em 3 etapas para o fluxo de **transações de pagamento**, respondendo:

1. **Qual fluxo migrar primeiro?** Por que ele, e não outro?
2. **Como os eventos começam a existir** sem reescrever o sistema legado?
3. **Qual o primeiro consumidor** a plugar, e por que ele é uma boa escolha para começar?
4. **O que acontece com o monólito** ao final da etapa 3 — ele morre?

> 💡 Duas pistas: a Aula 2 mostrou que **adicionar consumidor não afeta quem publica**.
> E existe uma técnica para gerar eventos a partir de um banco existente **sem tocar na
> aplicação** — você vai vê-la funcionando no Curso 3.

**Entregável**: um diagrama com 3 etapas e as 4 respostas.

---

## Passo 4 — As duas perguntas de checagem *(~5 min)*

Escreva 3 a 4 linhas para cada:

1. Uma empresa quebrou o monólito em 12 microsserviços que se chamam por REST. Que problema da Aula 1 ela reencontrou, e **que propriedade do log** resolve?
2. Cite um cenário em que você **recomendaria ficar** no monólito — e o sinal concreto que te faria reavaliar.

> ⚠️ A pergunta 2 é a mais importante da aula. Recomendar EDA para tudo é evangelismo,
> não engenharia.

---

## Gabarito

<details>
<summary><b>Passo 1 — a conta (clique para abrir)</b></summary>

```
0,995⁴ = 0,9801 → 98,01%
```

**(a)** `730h × (1 − 0,9801) ≈ 14,5 horas` fora do ar por mês.

**(b)** O monólito, com 99,5%: `730h × 0,005 ≈ 3,65 horas`.

**(c) Piorou — em quase 4×.** A migração quebrou o **deploy** (cada time libera
sozinho), mas manteve a **dependência**: o fluxo continua exigindo que todos estejam
de pé, agora com a rede no meio.

Este é o cálculo que raramente aparece no slide de "vantagens dos microsserviços".
Quanto mais serviços na cadeia síncrona, pior: com 10 serviços a 99,5%, o fluxo cai
para ~95% — cerca de 36h fora por mês.

**E com EDA?** A conta muda de natureza: o Checkout publica e segue. Se a Notificação
estiver fora, o evento fica no log esperando. A disponibilidade do *checkout* passa a
depender do Checkout e do Kafka — não da soma da cadeia.

</details>

<details>
<summary><b>Passo 2 — diagnóstico</b></summary>

| # | Sintoma | Resposta | Por quê |
|---|---|---|---|
| 1 | Notificação derruba o checkout | **Some** | O Checkout publica e segue; a Notificação consome quando puder |
| 2 | Antifraude exige alterar o Checkout | **Some** | O antifraude assina o tópico; quem publica não sabe quem lê (Aula 2) |
| 3 | Latência é a soma das 4 chamadas | **Some** | Só resta a latência de publicar; o resto vira assíncrono |
| 4 | Não dá para saber o que houve ontem às 14h | **Some** | O log retido é auditável e reproduzível (Aulas 2 e 3) |
| 5 | Deploy do Estoque exige avisar os outros | **Some** | Contratos são os eventos, não as APIs — e evoluem com schema (Curso 2) |
| 6 | Pedido pago sem baixa no estoque | **Muda de forma** | Continua possível, mas passa a ser tratável: *retry* + idempotência (Aula 5), e compensação via Saga |

**O item 6 é o único que não some** — e é o mais honesto da lista. EDA não elimina
inconsistência entre serviços: troca a inconsistência *silenciosa* por uma
**eventual e tratável**. O preço do desacoplamento é abrir mão da transação ACID
distribuída; o padrão para lidar com isso (Saga) é assunto da Live 2.

</details>

<details>
<summary><b>Passo 3 — a migração</b></summary>

**1. Qual fluxo primeiro?** O de **maior valor com mais interessados** — no nosso
domínio, as transações de pagamento: antifraude, extrato, BI e cobrança querem todas o
mesmo fato. Migrar um fluxo com um único consumidor não paga a complexidade.

**2. Como os eventos passam a existir?** Com **CDC** (Change Data Capture): um
conector lê o log de transações do banco legado e publica cada mudança como evento —
**sem uma linha de código novo na aplicação**. É exatamente o Curso 3.

**3. Primeiro consumidor?** Um que seja **read-only e não crítico** — BI ou um
dashboard. Se ele falhar, ninguém perde dinheiro, e o time aprende a operar o pipeline
com risco baixo. Começar pelo antifraude (que bloqueia transação) é apostar alto na
primeira mão.

**4. O monólito morre?** **Não necessariamente — e tudo bem.** No padrão *strangler*,
ele vai perdendo responsabilidades à medida que consumidores assumem funções. Muitas
migrações bem-sucedidas param no meio do caminho: o legado segue como sistema de
registro, e o streaming distribui os fatos. Migração não é corrida até o fim.

</details>

<details>
<summary><b>Passo 4 — perguntas de checagem</b></summary>

**1. O problema reencontrado e a propriedade que resolve**

O problema é o **acoplamento** da Aula 1 — a cadeia síncrona com falha em cascata,
agora distribuída pela rede. Quebrou-se o deploy, não a dependência: é o **monólito
distribuído**.

A propriedade do log que resolve é a **durabilidade com leitura independente**: o
produtor escreve e segue, e o consumidor lê **no seu ritmo**, mantendo a própria
posição (offset). Isso quebra o **acoplamento temporal** — não é mais preciso que
todos estejam de pé ao mesmo tempo. Um consumidor fora do ar volta e processa o que
ficou; foi exatamente o replay da Aula 2.

**2. Quando ficar no monólito**

Cenário legítimo: **CRUD com um consumidor por dado e time pequeno.** Um sistema
interno de RH com 8 pessoas no time, sem picos, sem múltiplos interessados nos mesmos
fatos. Aqui o monólito bem feito ganha: transação ACID de graça, deploy simples,
debug direto — e EDA cobraria consistência eventual, mais infraestrutura e um modelo
mental novo sem entregar benefício proporcional.

**O sinal para reavaliar**: quando o **mesmo dado passa a ter 3 ou mais interessados**
com necessidades diferentes, ou quando começam a aparecer jobs noturnos copiando dados
de um sistema para outro. Esse é o momento em que o custo de *não* ter um log central
supera o custo de tê-lo.

Outro sinal: quando o time começa a **agendar deploys em conjunto** — sinal de que o
acoplamento já é organizacional, não só técnico.

</details>

---

## Erros comuns

| Se você… | O que está confundindo |
|---|---|
| respondeu que a migração do Passo 1 melhorou a confiabilidade | Deploy independente ≠ dependência quebrada. Só a comunicação assíncrona resolve |
| marcou o item 6 como "some" | EDA **não** dá transação distribuída de graça — troca ACID por consistência eventual |
| desenhou a migração começando pelo fluxo crítico | Comece pelo que tem mais interessados **e** menos risco; aprenda antes de apostar alto |
| concluiu que todo monólito deve virar EDA | A dor do monólito é de *crescimento*. Sem a dor, EDA é complexidade sem retorno |

---

## Glossário

| Termo | Definição |
|---|---|
| **Monólito distribuído** | Microsserviços que se chamam sincronamente: mantém o acoplamento e adiciona rede. |
| **Acoplamento temporal** | Exigência de que produtor e consumidor estejam disponíveis ao mesmo tempo. |
| **Disponibilidade composta** | Produto das disponibilidades de uma cadeia síncrona — sempre menor que a menor delas. |
| **Consistência eventual** | O sistema converge para o estado correto, não instantaneamente. |
| **Padrão strangler** | Migração incremental: o novo sistema envolve o legado e assume funções aos poucos. |
| **CDC** | Captura de mudanças do banco como eventos, sem alterar a aplicação (Curso 3). |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| CDC como ponte legado → streaming | **Curso 3** — Kafka Connect e Debezium |
| Saga, Event Sourcing e CQRS | **Live 2** |
| Contratos entre times | **Curso 2** — Schema Registry |
| O dado como produto | **Curso 5** — Lakehouse |

**Fontes**

- Sam Newman — *Building Microservices*, 2ª ed. (O'Reilly) — cap. 4: estilos de comunicação
- Sam Newman — *Monolith to Microservices* (O'Reilly) — o padrão strangler em detalhe
- Martin Fowler — [*MonolithFirst*](https://martinfowler.com/bliki/MonolithFirst.html)
- Ben Stopford — *Designing Event-Driven Systems* — [e-book gratuito](https://www.confluent.io/designing-event-driven-systems/)

---

## Você terminou o Curso 1 🎉

O modelo mental está completo:

| Aula | O que você levou |
|---|---|
| 1 | O vocabulário: evento, comando, mensagem |
| 2 | Fila vs streaming — e que só o `group.id` os separa |
| 3 | O log: partições, offsets e segments no disco |
| 4 | Consumer groups, rebalance e LAG |
| 5 | Ordem por chave, at-least-once e idempotência |
| 6 | Quando migrar — e quando não |

**No Bloco 2**, tudo isso ganha uma plataforma gerenciada: o Confluent Cloud, com Schema Registry, contratos de dados e conectores.

Antes de seguir, limpe o ambiente:

```bash
make down
```

(derruba o sandbox e apaga os volumes)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 5 · Ordem, idempotência e entrega](../aula5/README.md) | [Curso 1](../README.md) | [Curso 2 · Confluent Cloud](../../curso2/README.md) |
