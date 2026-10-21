# Curso 3 · Aula 3 — Sink Connectors: entregando para Data Warehouses, NoSQL e Object Storage

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo (Confluent Cloud → Neon Postgres)
> **Pré-requisitos:** Aula 2 (tópico CDC populado) + Setup Neon

---

## 1. Objetivo da Aula

Fechar a segunda metade do encanamento: os **Sink Connectors**, que entregam o que vive nos tópicos para os sistemas de destino. Dissecar a anatomia da entrega (**consumer group interno, batching, semântica**), entender o pulo do gato — **idempotência via upsert no destino** — e mapear o panorama dos destinos: Data Warehouses, NoSQL e Object Storage.

---

## 2. Conceitos Simplificados

### 2.1 Anatomia de um sink: um consumidor de luxo *(~4 min)*

Um sink connector é, por baixo, **um consumer group** (Curso 1, Aula 4) com superpoderes de entrega:

```
 TOPIC ──► SINK CONNECTOR
              │ consumer group interno (offsets, rebalance, lag — tudo vale!)
              │ converter: Avro + Schema Registry → estrutura
              │ batching: acumula N eventos ou T ms
              ▼
           DESTINO (banco, DW, NoSQL, storage)
              │ escreve em lote
              ▼
           commit de offset  ◄── SÓ depois da escrita confirmada
```

- **Consumer group interno** — o sink tem `group.id`, commita offsets e aparece na tela de consumer lag (Curso 2, Aula 4): **monitorar sink = monitorar lag**
- **Batching** — escrever evento a evento mataria o destino; o sink acumula e grava em lote (throughput × latência, o mesmo trade-off do `linger.ms` — Curso 1, Aula 4)
- **A ordem sagrada:** processa → escreve no destino → **então** commita. Se o sink morrer entre a escrita e o commit... o lote é **relido e reescrito**. Bem-vindo de volta, **at-least-once** (Curso 1, Aula 5)

### 2.2 O pulo do gato: idempotência no destino via upsert *(~4 min)*

O at-least-once do sink significa: **o destino verá duplicatas** eventualmente. A solução não é evitar a releitura — é torná-la **inofensiva**, exatamente como no Curso 1, Aula 5. No mundo dos sinks, a técnica tem nome: **upsert**.

```
 modo INSERT:  evento {id:42, status:PAGO} chega 2x
               → 2 linhas (ou erro de PK duplicada)   ✗ duplicata dói

 modo UPSERT:  evento {id:42, status:PAGO} chega 2x
               → "INSERT ... ON CONFLICT (id) UPDATE"
               → 1 linha, estado correto              ✓ duplicata inofensiva
```

- **`insert.mode=upsert` + chave primária** — a releitura vira um update idempotente: N entregas, 1 efeito
- A chave do upsert vem da **key do evento** — que no nosso fluxo é a **primary key da linha** (o CDC da Aula 2 escolheu certo lá atrás): a cadeia *PK → key → partição → ordem → upsert* atravessa o pipeline inteiro
- **Deletes:** o evento `op: "d"` / tombstone pode virar DELETE no destino (`delete.enabled`) — o ciclo completo da linha, replicado
- Síntese: **at-least-once na entrega + upsert no destino ≈ efeito exactly-once** — a mesma equação pragmática do Curso 1, agora em configuração de conector

### 2.3 O panorama dos destinos *(~4 min)*

Cada família de destino tem sua física — e o sink respeita:

| Família | Exemplos | O que muda na entrega | Pergunta que responde |
|---|---|---|---|
| **Banco relacional** | Postgres, MySQL | Linha a linha em lote; upsert por PK; schema mapeado de Avro para DDL | "qual o estado atual?" (operacional) |
| **Data Warehouse** | Snowflake, BigQuery, Redshift | Micro-batches otimizados para colunar; append-first (dedup analítico) | "o que aconteceu no agregado?" (analítico) |
| **NoSQL / busca** | MongoDB, Elasticsearch | Documento por chave (upsert natural); índice de busca | "acha esse documento/texto" (consulta rápida) |
| **Object Storage** | S3, GCS, Azure Blob | Arquivos particionados por tempo/chave (Parquet/Avro); append imutável | "guarda tudo barato" (data lake, arquivo) |

- O mesmo tópico pode alimentar **vários sinks ao mesmo tempo** — cada um é um consumer group independente (Curso 1, Aula 2: streaming, não fila). Um evento `PedidoPago` → Postgres (operação) + Snowflake (análise) + Elasticsearch (busca), sem coordenação
- **Spoiler estratégico:** para o caminho analítico, o Curso 5 apresenta uma alternativa ao sink tradicional — o **Tableflow**, que materializa o tópico como tabela Iceberg sem conector no meio. Sinks continuam reinando para destinos operacionais e sistemas específicos

**Configurações de referência — Sink (padrão JDBC/Postgres):**

| Config | O que faz | Ponto didático |
|---|---|---|
| `topics` | De quais tópicos consumir | Um sink pode drenar vários |
| `connection.host/.user/...` | O destino | Credenciais = tratamento de API key |
| `insert.mode` | `insert` × `upsert` | O pulo do gato da seção 2.2 |
| `pk.mode` / `pk.fields` | De onde vem a chave do upsert | `record_key` = a key do evento (nossa cadeia) |
| `delete.enabled` | Tombstone vira DELETE | O ciclo completo da linha |
| `auto.create` / `auto.evolve` | Criar/evoluir a tabela destino pelo schema | Schema Registry dirigindo DDL — poderoso e perigoso (ver Pontos de atenção) |
| `batch.size` | Tamanho do lote de escrita | O `linger.ms` do sink |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Fluxo: tópico CDC da Aula 2 → **Postgres Sink fully managed** → Neon (schema/tabela de destino separada — simulando "outro sistema", ex.: `analytics.pedidos_espelho`). Dataset placeholder até a modelagem fechar. Tempo de tela: ~7 min. **Conector deletado ao final.**

**O que a demo deve mostrar, nesta ordem:**

1. **Criar o Postgres Sink** — consumindo o tópico da Aula 2, destino Neon (schema `analytics`), `insert.mode=upsert`, `pk.mode=record_key`, `auto.create=true`.
   - **Observação-alvo:** o formulário é a tabela de configs da seção 2.3; e o `auto.create` vai materializar a tabela sozinho a partir do schema Avro — o Registry dirigindo DDL.

2. **Ver a carga inicial chegar** — a tabela destino criada e populada com o que estava no tópico (o snapshot + mudanças da Aula 2); `SELECT` no Neon confirmando.
   - **Observação-alvo:** o ciclo fechou — **banco → Kafka → banco** — e o "outro sistema" recebeu a história completa sem integração ponto a ponto.

3. **Mudança fluindo ponta a ponta** — um UPDATE na tabela de origem (Aula 2 ainda de pé conceitualmente / reproduzir insert manual no tópico) refletindo na tabela destino segundos depois.
   - **Observação-alvo:** a latência fim a fim origem → destino em segundos, atravessando o pipeline inteiro.

4. **Provar o upsert** — reprocessar eventos (resetar o offset do consumer group do sink ou reenviar um lote) e mostrar que a tabela destino **não duplica** — mesma contagem, estado correto.
   - **Observação-alvo:** o momento-síntese — a duplicata do at-least-once chegou E foi inofensiva. A cena 3 da Aula 5 do Curso 1, agora sem escrever código.

5. **Ver o sink como consumer group** — a tela de consumer lag mostrando o grupo interno do conector.
   - **Observação-alvo:** sink se monitora como qualquer consumidor — lag alto = destino não acompanha (semente do Curso 7).

6. **Teardown** — deletar o sink; tabela destino fica como evidência.

**Fechamento na tela** *(~1 min)*: "ingestão (Aula 2) e entrega (Aula 3): o encanamento está completo. Falta a parte que separa pipeline de brinquedo de pipeline de produção: o que fazer quando a mensagem vem **errada**. SMT e Dead Letter Topic, na próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Sink entrega exactly-once por padrão."** O padrão é at-least-once — o "efeito exactly-once" é conquistado com upsert/idempotência no destino (ou suporte transacional específico do conector). Perguntar sempre: *o que acontece quando o mesmo lote chega duas vezes?*
- **"auto.create/auto.evolve é só conveniência."** Em aula, é mágico; em produção, é o schema do tópico ditando DDL no destino — governança do Curso 2 (quem pode evoluir o quê) na veia. Usar com contrato BACKWARD sólido.
- **"Um sink por tópico."** O mesmo tópico alimenta N destinos independentes — é o argumento econômico do streaming (capturou uma vez, entrega para todos).
- **"Object storage sink = Lakehouse."** Arquivos no S3 são o começo da conversa analítica, não o fim — o Curso 5 mostra o degrau acima (tabelas Iceberg com Tableflow).

**Ganchos com a trilha:**

- Mensagem malformada quebrando o sink → **Aula 4** (DLQ é a resposta).
- O caminho analítico sem sink → **Curso 5** (Tableflow).
- Consumer lag do sink como alerta → **Curso 7, Aula 2**.
- No **Desafio Final**, o sink é a camada de entrega operacional do pipeline do aluno.

**Perguntas para checar entendimento:**

1. O sink morreu depois de gravar um lote no Postgres e antes de commitar o offset. O que acontece quando ele volta — e por que a tabela não duplica?
2. Sua empresa quer o mesmo fluxo de pedidos no Snowflake (análise) e no Elasticsearch (busca). Quantos conectores, e o que os torna independentes entre si?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Sink connector** | Conector que consome tópicos e escreve em sistemas externos. |
| **Consumer group interno** | O grupo que o sink usa por baixo — offsets, rebalance e lag valem normalmente. |
| **Batching (sink)** | Acúmulo de eventos para escrita em lote no destino. |
| **Upsert** | Escrita idempotente: insere se não existe, atualiza se existe (por chave). |
| **insert.mode / pk.mode** | Configs que definem o modo de escrita e a origem da chave do upsert. |
| **auto.create / auto.evolve** | Criação/evolução automática da tabela destino a partir do schema do tópico. |
| **Tombstone → DELETE** | Tradução do evento de remoção em delete no destino (`delete.enabled`). |
| **Fan-out de destinos** | O mesmo tópico alimentando múltiplos sinks independentes. |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 9 (pipelines: semântica de entrega em sinks). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11 (derivação de estado a partir de logs — o fundamento do "banco → Kafka → banco")

**Documentação oficial:**

- Confluent Cloud — *Postgres Sink (JDBC) Connector* — https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html
- Confluent — *JDBC Sink Deep Dive (insert.mode, pk.mode, deletes)* — https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html
- Confluent Cloud — *Catálogo de Sink Connectors (DW, NoSQL, Object Storage)* — https://docs.confluent.io/cloud/current/connectors/index.html
- Confluent — *Kafka Connect: Delivery Semantics* — https://docs.confluent.io/platform/current/connect/index.html#delivery-guarantees
