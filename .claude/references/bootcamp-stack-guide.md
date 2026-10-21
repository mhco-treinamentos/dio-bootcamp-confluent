# Bootcamp Stack Guide — Confluent Cloud

> **O "porquê" de cada escolha de stack do Bootcamp Confluent Cloud 2026 (DIO): quais componentes gerenciados cada módulo usa, como eles se conectam no pipeline de referência de pagamentos/fraude, e quando (e com que limites) cair para o fallback local.**

O bootcamp é **cloud-first**: todo lab aponta para o Confluent Cloud (créditos gratuitos de US$ 400/30 dias). O Docker local existe apenas como fallback enxuto para demos offline e gravação sem gastar créditos. O programa completo está em [`docs/Bootcamp_Confluent_Cloud_2026_v3.pdf`](../../docs/Bootcamp_Confluent_Cloud_2026_v3.pdf).

---

## Arquitetura de referência (Desafio Final — build-as-you-go)

```
Postgres 16 ──CDC──►  Managed Connect  ──►  Kafka + Schema  ──►  Flink SQL     ──►  Tableflow/Iceberg
(payments,            (Debezium             Registry             (enrichment +       │
 Docker/cloud)         PostgresCdcSourceV2)  (Confluent Cloud)    fraude + IA)        └─► Catálogo
                                                 │                                        (Glue / Snowflake
                                                 │                                         Open Catalog / Unity)
                                                 └──► Observabilidade (Metrics API, consumer lag, custos)
```

Cada camada é construída em um curso e reaproveitada nos seguintes — o Desafio Final é a soma das camadas, não um projeto novo.

---

## Componentes Confluent Cloud por módulo

| Módulo (curso) | Componentes usados | O que o aluno provisiona |
|----------------|--------------------|--------------------------|
| **1 — Fundamentos EDA/Kafka** | Nenhum recurso cloud obrigatório | Conceitos (log, partições, consumer groups, semânticas); demos podem usar o fallback local |
| **2 — Plataforma Confluent** | Environment, cluster Basic, service accounts, API keys, **Schema Registry / Data Contracts** (Avro, BACKWARD), CLI `confluent`, Cluster Linking (conceito de migração) | Aula 0: conta + environment + cluster + keys; primeira mensagem real |
| **3 — Connect e CDC** | **Managed Connect**: Debezium Postgres source (`PostgresCdcSourceV2`), sink connectors (DW/NoSQL/Object Storage), SMT, **Dead Letter Topic** | Postgres 16 em Docker como fonte + conector gerenciado; ver [cdc-connect-patterns.md](cdc-connect-patterns.md) |
| **4 — Flink** | **Managed Flink**: compute pools, statements, Flink SQL (janelas tumbling/hopping/session, joins, `MATCH_RECOGNIZE`), checkpoints gerenciados | Compute pool + statements de enriquecimento e detecção de fraude |
| **5 — Lakehouse** | **Tableflow** (topic → tabela Apache Iceberg), evolução de schema via SR, catálogos: AWS Glue, Snowflake Open Catalog, Databricks Unity | Habilitar Tableflow no topic enriquecido; conectar um catálogo externo |
| **6 — Real-Time AI** | AI Model Inference no Flink SQL (`CREATE MODEL` + `ML_PREDICT` com modelos remotos — OpenAI, Bedrock, Vertex AI), embeddings em tempo real, **Vector Search** (open preview) | Modelo remoto registrado + statement de classificação de fraude |
| **7 — Produção** | **Metrics API**, consumer lag, alertas (Prometheus/Grafana), RBAC/ACLs/service accounts, custos (retenção, compactação, Tiered Storage, clusters **Freight**, sizing), multi-region/DR | Dashboards + política de acesso least-privilege por lab |

Conceitos-âncora transversais (aparecem em todos os módulos): **RF=3, `min.insync.replicas=2`, `acks=all`** e as três semânticas de entrega (at-most-once, at-least-once, exactly-once), consolidadas no módulo 7.

---

## Como as peças se conectam (pipeline payments/fraude)

1. **Fonte** — Postgres 16 (`payments`) com `accounts`, `cards`, `transactions`; geradores Python (`repo/dataset/generators/`) alimentam o banco e/ou produzem eventos sintéticos direto nos topics `dev.payments.gen.*`.
2. **CDC** — conector Debezium gerenciado publica em `dev.payments.pg-payments.{entity}.cdc.v1` (RegexRouter aplica a convenção); registros ruins caem no Dead Letter Topic (Curso 3, Aula 4).
3. **Schemas** — tudo Avro no Schema Registry, namespace `com.bootcamp.payments`, compatibilidade BACKWARD; Data Contracts com tags PII/PCI ([rules/schemas.md](../rules/schemas.md)).
4. **Transformação** — Flink SQL lê os topics CDC + sintéticos, faz unwrap/enriquecimento/joins e publica `dev.payments.flink.transactions.enriched.v1` e `dev.payments.flink.fraud.detected.v1` (`MATCH_RECOGNIZE` para regras determinísticas).
5. **IA** — `ML_PREDICT` adiciona score de fraude por modelo remoto; embeddings + Vector Search dão contexto fresco (RAG) para o agente de análise.
6. **Lakehouse** — Tableflow materializa os topics enriquecidos como tabelas Iceberg (**shift-left**: o Flink limpa/enriquece **antes** de materializar); catálogo externo expõe para o mundo analítico.
7. **Observabilidade** — Metrics API alimenta Prometheus/Grafana: consumer lag, throughput do conector, profundidade do DLQ, custo por cluster.

---

## Decisões de arquitetura

### 1. Cloud-first, não cloud-only
**O quê**: todos os labs apontam para o Confluent Cloud; o local é fallback, nunca o caminho principal.
**Por quê**: o objeto de estudo do bootcamp É a plataforma gerenciada (Connect, Flink, Tableflow, AI não existem no compose local). Ensinar self-managed seria ensinar outro produto.
**Custo**: dependência de créditos → disciplina de teardown (abaixo).

### 2. Envelope Debezium preservado no topic cru + unwrap no Flink (shift-left)
**O quê**: o topic CDC mantém `{before, after, op, ts_ms}`; o achatamento acontece no Flink (o SMT `ExtractNewRecordState` é ensinado como alternativa no módulo 3).
**Por quê**: preserva semântica de deletes e event-time precisos para o desafio final; o aluno vê os dois padrões e o trade-off de cada um.

### 3. Avro + Schema Registry (não JSON)
**O quê**: todos os topics usam Avro via Schema Registry, com Data Contracts e tags.
**Por quê**: tipado, compacto, evoluível e governável — pré-requisito para Tableflow schematizar topics como tabelas Iceberg sem adivinhação de tipos.

### 4. Postgres 16 como fonte CDC (não Oracle)
**O quê**: banco fonte dos labs é Postgres em Docker (`wal_level=logical`).
**Por quê**: logical decoding nativo, imagem de ~150 MB, zero licenciamento — o padrão CDC (snapshot, envelope, slots) é o mesmo que o aluno aplicará em qualquer banco legado.

### 5. Tableflow em vez de jobs de ingestão para o lakehouse
**O quê**: nenhum job Spark/Flink escrevendo Iceberg; o Tableflow materializa o topic diretamente.
**Por quê**: elimina a camada de "pipeline de cópia" — o topic **é** a tabela. A energia do módulo 5 vai para modelagem, evolução de schema e catálogos, não para infraestrutura de escrita.

---

## Fallback local (demos offline / gravação)

Compose enxuto — **1 broker KRaft + Schema Registry**, nada mais (ver [skills/confluent/local-dev-fallback.md](../skills/confluent/local-dev-fallback.md)):

| Componente | Imagem | Nota de compatibilidade |
|------------|--------|-------------------------|
| Kafka (KRaft, single node) | `confluentinc/cp-kafka:7.9.x` | Sem ZooKeeper; manter broker e SR na **mesma** linha de versão |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.x` | Par alinhado com o broker — não misturar versões |

Seleção de perfil via `KAFKA_PROFILE=cloud|local` — um único ponto de configuração, nunca código duplicado ([rules.md](../rules.md)).

### O que o fallback cobre — e o que não cobre

| Funciona local | NÃO existe local (só cloud) |
|----------------|------------------------------|
| Producers/consumers dos módulos 1–2 | Managed Connect / conectores gerenciados (módulo 3) |
| Registro e evolução de schemas Avro | Managed Flink / compute pools (módulo 4) |
| Demos de topics, partições, consumer groups | Tableflow / Iceberg (módulo 5) |
| Testes com testcontainers | `CREATE MODEL` / `ML_PREDICT` / Vector Search (módulo 6) |
| — | Metrics API, RBAC, Data Contracts avançados (módulo 7) |

**Limite conceitual importante**: com 1 broker, RF=1 — os defaults de confiabilidade (RF=3, `min.insync.replicas=2`) **não podem ser demonstrados** localmente. Ao gravar demos locais, dizer isso explicitamente para não ensinar o default errado.

---

## Créditos e disciplina de teardown

Os créditos gratuitos (US$ 400/30 dias) acabam rápido com recursos ociosos. Regras do bootcamp ([rules/labs.md](../rules/labs.md)):

- **Todo `setup.sh` tem um `teardown.sh` par** — deletar conectores, statements, compute pools e clusters ao fim do lab
- Cluster **Basic** para tudo (Standard/Dedicated apenas se um recurso exigir); Freight é conteúdo do módulo 7, não default de lab
- Compute pools do Flink: `MAX_CFU` mínimo; deletar statements parados (statement parado ≠ custo zero)
- Conectores gerenciados cobram por task/hora — pausar não basta entre sessões de gravação, deletar e recriar por script
- CDC: ao deletar o conector, **dropar o replication slot** no Postgres (senão o WAL cresce — ver [cdc-connect-patterns.md](cdc-connect-patterns.md))
- Retenção curta nos topics de lab (1 dia) e um service account por lab com least privilege

---

## O que o `.claude/` cobre para este stack

| Camada | Agent / Skill |
|--------|---------------|
| Environments, clusters, keys, CLI | [confluent-cloud-expert](../agents/confluent-cloud-expert.md), [skills/confluent/cloud-setup.md](../skills/confluent/cloud-setup.md) |
| Connect + CDC + DLQ | [connect-cdc-expert](../agents/connect-cdc-expert.md), [skills/confluent/managed-connector.md](../skills/confluent/managed-connector.md) |
| Producer/consumer, semânticas de entrega | [kafka-streaming-expert](../agents/kafka-streaming-expert.md), [kafka-producer-guide.md](kafka-producer-guide.md) |
| Flink SQL gerenciado | [flink-confluent-expert](../agents/flink-confluent-expert.md) |
| Tableflow / Iceberg / catálogos | [tableflow-lakehouse-expert](../agents/tableflow-lakehouse-expert.md), [skills/confluent/tableflow.md](../skills/confluent/tableflow.md) |
| Real-Time AI | [realtime-ai-expert](../agents/realtime-ai-expert.md) |
| Observabilidade e custos | [observability-cost-expert](../agents/observability-cost-expert.md) |
| Segurança (RBAC, ACLs, LGPD) | [kafka-security-expert](../agents/kafka-security-expert.md) |
| Estrutura de labs / desafio final | [lab-architect](../agents/lab-architect.md), [rules/labs.md](../rules/labs.md) |
| Fallback local | [skills/confluent/local-dev-fallback.md](../skills/confluent/local-dev-fallback.md) |

---

## Quando consultar este guia

- Antes de desenhar um lab novo — confirmar quais componentes o módulo usa e o que precisa de teardown
- Ao decidir se uma demo pode ser gravada no fallback local (checar a tabela de limites)
- Ao debater trade-offs cloud vs. local ou envelope vs. unwrap
- Ao estimar consumo de créditos de um lab novo

---

**Última atualização**: 2026-07-11
