# Curso 1 · Aula 5 — Ordem por Chave, Idempotência e Semânticas de Entrega

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~20 min (teto) · Conceito + demo (sandbox Docker — implementação no projeto de demos)

---

## 1. Objetivo da Aula

Fechar o tripé de confiabilidade do Bloco 1 dissecando os mecanismos por dentro: como a **chave** garante ordem, o caminho de uma escrita **replicada** (líder, ISR e `acks`), a mecânica da **idempotência** no produtor, e as três **semânticas de entrega** — *at-most-once*, *at-least-once* e *exactly-once* — junto com os defaults `RF=3`, `min.insync.replicas=2`, `acks=all`.

---

## 2. Conceitos Simplificados

### 2.1 Ordem por chave: a anatomia do roteamento *(~3 min)*

A Aula 3 deixou a ferida aberta: ordem só existe **dentro da partição**. A solução é dirigir os records que precisam de ordem para a mesma partição — e o mecanismo é o **partitioner** (visto na anatomia do producer, Aula 4) agindo sobre a **key**:

```
record(key="ord-98123")
        │
   hash(key) % partições  ─►  SEMPRE a mesma partição
        │
partition 1 │ PedidoCriado │ PagamentoAprovado │ PedidoEnviado │ →
              ord-98123       ord-98123           ord-98123
              ▲──────── ordem de negócio garantida ────────▲
```

- Mesma chave → mesmo hash → **mesma partição** → ordem entre elas
- A pergunta de modelagem: **"ordem de quê?"** Pedido? Cliente? Dispositivo? A resposta define a chave
- Pedidos diferentes caem em partições diferentes e fluem **em paralelo**

**Analogia:** a esteira do caixa. Suas compras (mesma chave) passam juntas e em ordem numa esteira só; as dos outros clientes, em outras esteiras ao mesmo tempo. Ordem onde importa, paralelismo onde não importa.

### 2.2 Anatomia de uma escrita replicada: líder, ISR e acks *(~4 min)*

Dissecando o que acontece entre o `send()` e o "confirmado". Cada partição é **replicada** em vários brokers — uma réplica **líder** atende; as **seguidoras** copiam:

```
producer ──send──► BROKER 1 · partition 0 (LEADER)
                        │ grava no log local
                        ├──► BROKER 2 · réplica (follower, in-sync) ─┐
                        └──► BROKER 3 · réplica (follower, in-sync) ─┤
                                                                     │
   acks=0   ─► confirma sem esperar NADA (nem o líder)               │
   acks=1   ─► confirma quando o LÍDER gravou                        │
   acks=all ─► confirma quando as réplicas IN-SYNC gravaram ◄────────┘
```

Os três números do "cinto de segurança" padrão do bootcamp, agora com a mecânica visível:

- **`RF=3`** — a partição existe em 3 brokers; 2 podem falhar sem perda de dados
- **`min.insync.replicas=2`** — a escrita só é aceita se ao menos 2 réplicas estiverem em dia (o **ISR**); se o ISR encolher abaixo disso, o broker **rejeita** a escrita em vez de aceitar sem garantia
- **`acks=all`** — o produtor só considera enviado quando todo o ISR confirmou

Juntos: o record confirmado está garantido em **pelo menos 2 máquinas**. E o ponto crítico: **RF=3 sozinho não garante nada** — com `acks=1`, a confirmação pode vir antes da replicação, e a queda do líder naquele instante perde o dado. É o **trio** que protege, não o RF isolado.

*(No sandbox de 1 broker essa anatomia não se materializa — são defaults de cluster real, que o Confluent Cloud traz de fábrica, como veremos no Curso 2.)*

### 2.3 O dilema da entrega e as três semânticas *(~3 min)*

Entre produtor, broker e consumidor existe rede — e rede falha. O produtor enviou e **não recebeu o ack**: a mensagem chegou ou não? Ele não sabe. As três respostas possíveis são as três semânticas:

| | **At-most-once** | **At-least-once** | **Exactly-once** |
|---|---|---|---|
| Estratégia | Não reenvia | Reenvia até confirmar | Reenvia + deduplica |
| Pode perder? | **Sim** | Não | Não |
| Pode duplicar? | Não | **Sim** | Não |
| Custo | Mínimo | Baixo | Mais alto (idempotência + transações) |
| Exemplo de uso | Telemetria tolerante a perda | O padrão de mercado | Movimentação financeira |

O lado do consumidor tem o mesmo dilema em espelho — o cenário da Aula 4: **processou e morreu antes de commitar → releitura → duplicata**. At-least-once nasce nos dois lados.

### 2.4 Anatomia da idempotência: PID + sequência *(~4 min)*

**Idempotente** = executar N vezes tem o mesmo efeito de 1 vez. No Kafka, ela existe em duas camadas:

**No produtor** (`enable.idempotence=true` — **default** nas versões atuais), a deduplicação é mecânica:

```
producer (PID=7)
   ├─ envia record  [PID=7, seq=0] ─► broker grava  ✔
   ├─ envia record  [PID=7, seq=1] ─► broker grava  ✔  (mas o ACK se perde na rede!)
   └─ REENVIA       [PID=7, seq=1] ─► broker: "seq=1 do PID=7 eu JÁ tenho" → descarta ✔
```

- Cada produtor recebe um **PID** (producer ID) e numera os records por partição com um **sequence number**
- O broker guarda a última sequência gravada por (PID, partição): reenvio com sequência repetida é **descartado silenciosamente**
- Resultado: **retry sem duplicata na escrita** — o motivo de `retries` alto ser seguro na tabela da Aula 4

**No consumidor**, idempotência é decisão de **design seu** — o broker não pode adivinhar o efeito de negócio:

- Técnicas: chave natural + upsert; verificação de "já processei este `event_id`?" antes de aplicar o efeito
- A síntese pragmática do mercado: **at-least-once na entrega + consumidor idempotente ≈ efeito de exactly-once**, com muito menos complexidade

**Exactly-once completo (EOS)** adiciona **transações** (gravações atômicas em múltiplas partições + commit de offset transacional). Existe, funciona, tem custo e escopo — no bootcamp ele reaparece dimensionado no Curso 7 e no Flink (Curso 4).

**Configurações de referência — Confiabilidade:**

| Config | Lado | O que faz | Ponto didático |
|---|---|---|---|
| `acks` | Producer | `0` (fire-and-forget), `1` (líder), `all` (ISR completo) | O seletor de semântica na escrita; default moderno = `all` |
| `enable.idempotence` | Producer | Liga PID + sequence number | Default `true`; retry sem duplicata |
| `retries` / `delivery.timeout.ms` | Producer | Reenvios e prazo total de entrega | Com idempotência, reenviar é seguro |
| `max.in.flight.requests.per.connection` | Producer | Lotes simultâneos sem ack | ≤5 com idempotência preserva a ordem mesmo com retry |
| `replication.factor` (RF) | Tópico | Cópias de cada partição | 3 é o padrão de produção |
| `min.insync.replicas` | Tópico/Broker | Mínimo de réplicas em dia para aceitar escrita | 2 com RF=3: tolera 1 falha sem parar nem perder |
| `enable.auto.commit` | Consumer | Commit automático × manual | A fronteira reprocessar × perder (Aula 4) |
| `isolation.level` | Consumer | `read_committed` ignora transações abortadas | Só citar: par do EOS transacional |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Sandbox: o mesmo Kafka single-broker KRaft, tópico de 3 partições. Tempo de tela: ~5–6 min. Foco: **ordem por chave** e **duplicata do at-least-once** — replicação/acks ficam no conceitual (1 broker não demonstra ISR).

**O que a demo deve mostrar, nesta ordem:**

1. **Produzir eventos COM chave** — ciclo de vida de 3 pedidos intercalados (ex.: `ord-1: criado`, `ord-2: criado`, `ord-1: pago`, `ord-3: criado`, `ord-2: pago`, `ord-1: enviado`, ...), com `key = order_id`.
   - **Observação-alvo:** ao consumir exibindo chave + partição + offset, **todos os eventos de um mesmo pedido estão na mesma partição, na ordem de negócio** — pedidos diferentes se espalham. Contrastar com a Cena 2 da Aula 3 (sem chave = espalhado) e ligar ao diagrama do hash (seção 2.1).

2. **Simular o cenário at-least-once no consumo** — consumidor com commit manual processa um lote e é derrubado **após processar e antes de commitar**; ao religar, o lote é entregue de novo.
   - **Observação-alvo:** a **duplicata aparece na tela** — o mesmo evento processado duas vezes. Não é bug: é a semântica cumprindo o contrato "não perder nunca, ao custo de repetir às vezes".

3. **Resolver com consumidor idempotente** — repetir a cena 2, agora com o consumidor checando `event_id` já processado (dedup visível, ex.: set/tabela de IDs) antes de aplicar o efeito.
   - **Observação-alvo:** a releitura acontece igual, mas o **efeito** não se repete — "duplicata inofensiva". A síntese *at-least-once + idempotência* materializada — e o espelho, no consumo, do que o PID+seq faz na escrita.

**Fechamento na tela** *(~1 min)*: "ordem onde o negócio pede, escrita protegida pelo trio RF/ISR/acks, retry sem duplicar, consumo que repete sem doer. Esse é o pipeline confiável. O que fazer com a mensagem que falha de verdade — o veneno no lote — tem padrão próprio: Dead Letter Topic, no Curso 3. E com isso fechamos a mecânica; a próxima aula sobe pro nível da arquitetura: do monólito ao EDA."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"RF=3 garante zero perda."** Depende de `acks` + ISR. Com `acks=1`, a confirmação pode preceder a replicação — a anatomia da seção 2.2 mostra exatamente onde o dado escapa. Apresentar sempre como **trio**.
- **"Idempotência do produtor resolve duplicata no consumidor."** São camadas diferentes: PID+seq dedup **na escrita**; a duplicata de **releitura** (cena 2 da demo) só o design do consumidor resolve.
- **"Exactly-once é mágica / é de graça."** Tem custo (transações) e escopo. Em beginner, a mensagem: existe, funciona, e na maioria dos casos *at-least-once + consumidor idempotente* resolve mais barato.
- **"Escolher chave é detalhe técnico."** É decisão de **modelagem de negócio** — define ordem e distribuição de carga. Chave ruim (tudo com a mesma chave) cria **hot partition** e mata o paralelismo.
- **Disciplina de relógio:** a tabela de configs é consulta — em vídeo, narrar `acks`, `enable.idempotence` e `min.insync.replicas`; o resto fica na documentação de apoio. Transações/EOS profundos não cabem nos 20 min.

**Ganchos com a trilha:**

- Dead Letter Topic como padrão de resiliência → **Curso 3, Aula 4**.
- O trio RF/ISR/acks operacionalizado (e multi-region/DR) → **Curso 7, Aulas 4–5**.
- Garantias de entrega no processamento stateful → **Curso 4, Aula 4** (checkpoints do Flink).
- `isolation.level` e transações → contexto no **Curso 7** (consolidação das garantias).

**Perguntas para checar entendimento:**

1. Um cluster tem RF=3 e o produtor usa `acks=1`. O líder confirma e morre antes de replicar. O dado existe? O que mudaria com `acks=all` + `min.insync.replicas=2`?
2. Seu consumidor de cobranças roda com at-least-once. O que precisa ser verdade sobre ele para nenhum cliente ser cobrado duas vezes — e por que a idempotência do produtor não basta?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Key (chave)** | Atributo do record que o partitioner usa para determinar a partição; mesma chave → mesma partição → ordem. |
| **Leader / Follower** | Réplica que atende leituras/escritas de uma partição / réplicas que a copiam. |
| **ISR (in-sync replicas)** | Conjunto de réplicas em dia com a líder; o `acks=all` espera por elas. |
| **acks** | Nível de confirmação exigido pelo produtor: `0`, `1` ou `all`. |
| **Replication factor (RF)** | Número de cópias de cada partição distribuídas entre brokers. |
| **min.insync.replicas** | Mínimo de réplicas in-sync para uma escrita ser aceita. |
| **At-most-once** | Semântica em que mensagens podem se perder, mas nunca duplicar. |
| **At-least-once** | Semântica em que nada se perde, mas reenvios/releituras podem duplicar. |
| **Exactly-once (EOS)** | Cada mensagem produz efeito exatamente uma vez (idempotência + transações). |
| **PID (producer ID) + sequence number** | Mecanismo da idempotência: o broker descarta reenvios com sequência repetida. |
| **Idempotence (idempotência)** | Propriedade de uma operação cujo efeito é o mesmo executada 1 ou N vezes. |
| **Hot partition (partição quente)** | Partição sobrecarregada por má distribuição de chaves. |
| **Transaction (transação)** | Escritas atômicas em múltiplas partições + commit de offset; base do EOS completo. |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 7 ("Reliable Data Delivery") e cap. 8 ("Exactly-Once Semantics") são o núcleo desta aula. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — caps. 5 (replicação) e 11 (garantias em stream processing): o fundamento teórico de líder/seguidor e entrega

**Documentação e artigos oficiais:**

- Apache Kafka — *Message Delivery Semantics* — https://kafka.apache.org/documentation/#semantics
- Apache Kafka — *Replication* (líder, ISR, high watermark) — https://kafka.apache.org/documentation/#replication
- Confluent — *Exactly-Once Semantics Are Possible: Here's How Kafka Does It* (Neha Narkhede) — https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
- Confluent — *Idempotent Producer* e *Delivery Guarantees* — https://docs.confluent.io/kafka/design/delivery-semantics.html
- KIP-98: Exactly Once Delivery and Transactional Messaging — https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
