# Curso 3 · Aula 1 — Introdução ao Kafka Connect e aos conectores gerenciados no Confluent Cloud

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Curso 2 concluído (cluster, SA, keys, Schema Registry)

---

## 1. Objetivo da Aula

Apresentar o **Kafka Connect** — o framework que liga o mundo externo ao streaming **por configuração, não por código** — dissecando sua anatomia (**connector → tasks → workers → converters**), o catálogo de conectores fully managed do Confluent Cloud, e criar o primeiro conector da vida do aluno.

---

## 2. Conceitos Simplificados

### 2.1 O problema: o código de integração que ninguém quer escrever *(~3 min)*

No Curso 2, Aula 4, o aluno produziu mensagens "na mão". No mundo real, os dados já vivem em algum lugar — bancos, SaaS, filas, storage — e a primeira tentação é escrever um serviço para cada integração:

- 10 fontes + 5 destinos = um zoológico de micro-integrações, cada uma reinventando **retry, escala, entrega, schema e monitoramento**
- É código de baixo valor (move dado de A pra B) com custo alto de manutenção — o clássico "80% do esforço de dados é encanamento"

O **Kafka Connect** padroniza o encanamento: integração vira **configuração declarativa** sobre um framework que já resolve escala, tolerância a falha e entrega. A regra da aula:

> **Integração de dados não é lugar de código criativo. É lugar de configuração sobre plataforma testada.**

### 2.2 Anatomia do Kafka Connect *(~5 min)*

```
 MUNDO EXTERNO                  KAFKA CONNECT                        KAFKA
┌────────────┐      ┌────────────────────────────────┐      ┌──────────────┐
│  Postgres  │      │ CONNECTOR (a configuração:     │      │              │
│  S3        │ ───► │  o QUE integrar, de ONDE)      │ ───► │   topics     │
│  MongoDB   │      │   └─ TASKS (as unidades que    │      │              │
│  SaaS ...  │ ◄─── │      trabalham — paralelismo)  │ ◄─── │              │
└────────────┘      │        └─ WORKERS (onde tasks  │      └──────────────┘
                    │           rodam — a infra)     │
                    │ CONVERTERS (formato: bytes ↔   │
                    │  Avro/JSON + Schema Registry)  │
                    └────────────────────────────────┘
      SOURCE: externo → Kafka          SINK: Kafka → externo
```

- **Connector** — a peça lógica: um plugin (Postgres, S3, MongoDB...) + a configuração declarativa. **Source** traz para o Kafka; **Sink** leva do Kafka
- **Tasks** — as unidades de trabalho reais: um conector se divide em N tasks para paralelizar (`tasks.max`). É o consumer group do mundo Connect
- **Workers** — os processos onde as tasks rodam. **Aqui mora a fronteira de gestão**: no self-managed, o cluster de workers é seu problema (dimensionar, escalar, dar patch); no **fully managed do Confluent Cloud, workers não existem para você** — só a configuração
- **Converters** — a ponte de formato entre o mundo externo e os bytes do Kafka: `AvroConverter` integra direto com o **Schema Registry** (Curso 2, Aula 3) — conector também respeita contrato

### 2.3 Fully managed × self-managed *(~3 min)*

| | **Self-managed Connect** | **Fully managed (Confluent Cloud)** |
|---|---|---|
| Workers | Você dimensiona, opera e escala | Invisíveis — a plataforma opera |
| Conectores | Qualquer plugin (inclusive próprios) | Catálogo de 90+ prontos |
| Quando faz sentido | Plugin proprietário, rede fechada, requisito de perímetro | O caso comum: integrar rápido sem operar infraestrutura |
| Custo | Infraestrutura + operação | Por conector/tarefa + throughput — **e é o recurso que queima créditos ligado à toa** |

A regra prática do bootcamp: fully managed sempre — e **conector é o vilão nº 1 do teardown** (Aula 0): diferente do cluster Basic ocioso, conector ligado cobra por existir.

**Configurações de referência — comuns a todo conector:**

| Config | O que faz | Ponto didático |
|---|---|---|
| `connector.class` | Qual plugin usar (Datagen, PostgresCdcSource, ...) | O "tipo" do conector |
| `tasks.max` | Teto de tasks paralelas | O `partitions` do mundo Connect — teto, não garantia |
| `kafka.topic` / `topics` | Destino (source) ou origem (sink) | Source escreve em 1+; sink lê de 1+ |
| `key.converter` / `value.converter` | Formato + Schema Registry | `AvroConverter` = contrato desde a ingestão |
| `transforms` | Cadeia de SMTs | Aula 4 |
| `errors.tolerance` / `errors.deadletterqueue.*` | O que fazer com mensagem problemática | Aula 4 — a fundação da resiliência |

### 2.4 O catálogo e o critério de escolha *(~2 min)*

O catálogo fully managed cobre as categorias que o bootcamp percorre: **bancos relacionais/CDC** (Postgres, MySQL, SQL Server — Aula 2), **Data Warehouses** (Snowflake, BigQuery, Redshift), **NoSQL** (MongoDB, Elasticsearch), **Object Storage** (S3, GCS, Azure Blob) e **SaaS** (Salesforce etc.). Critério ao avaliar um conector: existe fully managed? Suporta o modo de entrega que preciso (Aula 3)? Como trata erro (Aula 4)?

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — UI + CLI do Confluent Cloud. Tempo de tela: ~6–7 min. **Termina com o conector DELETADO** — o teardown é parte da lição.

**O que a demo deve mostrar, nesta ordem:**

1. **Passear pelo catálogo** — a galeria de conectores fully managed na UI; filtrar por source/sink; abrir a página do **Datagen Source**.
   - **Observação-alvo:** a dimensão do catálogo (categorias, não decoreba) e que cada conector é um formulário de configuração — não um projeto de código.

2. **Criar o Datagen Source** — dados sintéticos de domínio próximo ao dataset do bootcamp (ex.: `orders`), tópico novo, formato **Avro** (Schema Registry entra automaticamente), `tasks.max=1`, autenticando com a service account do Curso 2.
   - **Observação-alvo:** o formulário inteiro é a tabela de configs da seção 2.3, viva — e a SA/API key reaparecendo (identidade de aplicação, sempre).

3. **Ver o conector trabalhar** — status RUNNING, a task ativa, throughput; o tópico enchendo; o schema **registrado automaticamente** no subject.
   - **Observação-alvo:** ninguém escreveu producer — e mesmo assim há contrato no Registry e eventos fluindo. Configuração sobre plataforma.

4. **Inspecionar no Stream Lineage** — o fluxo conector → tópico desenhado.
   - **Observação-alvo:** a governança (Curso 2) enxergando o Connect sem esforço extra.

5. **TEARDOWN dramatizado** — pausar, mostrar o custo por hora na UI, **deletar o conector**; conferir que o tópico e o schema ficam (dados são baratos; conector ligado não).
   - **Observação-alvo:** a regra da Aula 0 com números na tela: *conector é cobrado por existir* — deletar é o normal, recriar custa um formulário.

**Fechamento na tela** *(~1 min)*: "primeiro conector: criado, observado, destruído. Dados sintéticos servem pra aprender — na próxima aula, conectamos no banco de verdade: o Postgres legado, via CDC com Debezium."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Connect é um ETL visual."** É um framework de **movimentação** com garantias de entrega — transformação pesada não mora aqui (SMT é ajuste fino, Aula 4; transformação de verdade é Flink, Curso 4). O slogan: *Connect move, Flink transforma*.
- **"tasks.max define quantas tasks rodam."** É teto — o paralelismo real depende do conector e da fonte (um Datagen simples roda 1; um CDC de várias tabelas divide melhor).
- **"Conector parado não custa."** No fully managed, conector **existente** cobra. Pausar ≠ deletar. Repetir até virar reflexo.
- **"Converter é detalhe."** É onde o contrato nasce na ingestão — JSON sem schema na entrada cobra o preço lá na frente (Curso 5 mostra isso doendo no Lakehouse).

**Ganchos com a trilha:**

- CDC e o envelope Debezium → **Aula 2** (na sequência).
- Entrega e idempotência no destino → **Aula 3**.
- SMT e DLQ → **Aula 4**.
- O conector como fonte do capstone: a **camada de ingestão do Desafio Final** nasce neste curso.

**Perguntas para checar entendimento:**

1. Sua empresa precisa levar dados do MongoDB para o Kafka e do Kafka para o Snowflake. Quantos conectores, de que tipo, e o que você avalia antes de escolher cada um?
2. Por que "pausei o conector" não encerra a preocupação com créditos?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Kafka Connect** | Framework de integração de dados por configuração, com escala e tolerância a falha embutidas. |
| **Source connector** | Conector que traz dados do mundo externo para tópicos Kafka. |
| **Sink connector** | Conector que leva dados de tópicos Kafka para sistemas externos. |
| **Task** | Unidade de trabalho paralela de um conector (`tasks.max` é o teto). |
| **Worker** | Processo que executa tasks — invisível no fully managed. |
| **Converter** | Componente de formato (bytes ↔ Avro/JSON/Protobuf), integrado ao Schema Registry. |
| **Fully managed connector** | Conector operado pela plataforma no Confluent Cloud — configuração sem infraestrutura. |
| **Datagen** | Conector source de dados sintéticos, para desenvolvimento e ensino. |
| **SMT (Single Message Transform)** | Transformação leve aplicada mensagem a mensagem na passagem (Aula 4). |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 9 ("Building Data Pipelines"): o Connect e os critérios de pipeline. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Adam Bellemare — *Building Event-Driven Microservices* (O'Reilly) — cap. sobre liberation de dados legados via conectores

**Documentação oficial:**

- Apache Kafka — *Kafka Connect* — https://kafka.apache.org/documentation/#connect
- Confluent Cloud — *Fully Managed Connectors (catálogo)* — https://docs.confluent.io/cloud/current/connectors/index.html
- Confluent Cloud — *Datagen Source Connector* — https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html
- Confluent — *Kafka Connect Concepts (connectors, tasks, workers, converters)* — https://docs.confluent.io/platform/current/connect/index.html
