# Agent Fleet — Bootcamp Confluent Cloud 2026

> **Agentes especializados para construir os labs e o Desafio Final do bootcamp (Confluent Cloud: Kafka + Connect/CDC + Flink SQL + Tableflow + Real-Time AI) com domínio de pagamentos/fraude.**

## Agentes de Plataforma & Stack

| Agent | Domínio | Expertise |
|-------|---------|-----------|
| `confluent-cloud-expert` | Confluent Cloud | CLI `confluent`, environments, clusters (Basic/Standard/Dedicated/Freight), service accounts, API keys, Cluster Linking, teardown |
| `connect-cdc-expert` | Connect & CDC | Conectores gerenciados, Debezium source (Postgres), sinks DW/NoSQL/Object Storage, SMTs, Dead Letter Topic |
| `kafka-streaming-expert` | Kafka Streaming | Producers, topics, schemas, partitioning, semânticas de entrega, idempotência |
| `kafka-security-expert` | Segurança | RBAC, ACLs, service accounts, API keys, mascaramento PII/PCI, LGPD |
| `flink-confluent-expert` | Flink gerenciado | Compute pools, statements, watermarks, janelas (tumbling/hopping/session), joins, MATCH_RECOGNIZE |
| `tableflow-lakehouse-expert` | Lakehouse | Tableflow, Iceberg, evolução de schema, catálogos (Glue/Snowflake Open Catalog/Unity), consumo analítico |
| `realtime-ai-expert` | Real-Time AI | CREATE MODEL, ML_PREDICT, embeddings, Vector Search, streaming agents |
| `observability-cost-expert` | Produção | Metrics API, consumer lag, alertas Prometheus/Grafana, custos (Tiered Storage, Freight), multi-region/DR |

## Agentes de Dados & Engenharia

| Agent | Domínio | Expertise |
|-------|---------|-----------|
| `data-modeler` | Modelagem | Design de entidades, evolução de schema (BACKWARD), relacionamentos, distribuições |
| `data-quality-expert` | Qualidade | Validação, realismo, distribuições estatísticas em dados sintéticos |
| `lab-architect` | Labs | Estrutura de labs por módulo, time-boxing, camadas build-as-you-go do Desafio Final |
| `software-engineer` | Engenharia de Software | Padrões Python, arquitetura, testing, performance |
| `logging-expert` | Logging | Logging estruturado, níveis, formato JSON |

## Como usar

### Invocação direta
```
"Usando confluent-cloud-expert, crie o script da Aula 0: environment, cluster, API keys e teardown"
"Usando connect-cdc-expert, configure o source connector Debezium para a tabela transactions"
"Usando flink-confluent-expert, escreva o statement MATCH_RECOGNIZE de fraude por velocidade"
"Usando tableflow-lakehouse-expert, materialize o topic enriquecido como tabela Iceberg"
"Usando realtime-ai-expert, adicione ML_PREDICT de classificação de fraude ao pipeline"
"Usando observability-cost-expert, monte o dashboard de consumer lag com a Metrics API"
```

### Multi-agent
```
"Primeiro use connect-cdc-expert para configurar o source connector Debezium do Postgres,
depois use flink-confluent-expert para enriquecer as transações,
depois use tableflow-lakehouse-expert para materializar o resultado como Iceberg"
```

## Guia de seleção

### Confluent Cloud (setup e operação)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Environment, cluster, API keys, teardown | `confluent-cloud-expert` |
| Configurar topics, partições, retention | `kafka-streaming-expert` |
| Producer idempotente (cloud ou fallback local) | `kafka-streaming-expert` |
| RBAC, ACLs e mascaramento de PII | `kafka-security-expert` |
| Migração/legado com Cluster Linking | `confluent-cloud-expert` |

### Connect & CDC (módulo 3)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Preparar Postgres fonte (wal_level, publication) | `connect-cdc-expert` |
| Configurar source connector Debezium gerenciado | `connect-cdc-expert` |
| Sink connectors (DW, NoSQL, S3) | `connect-cdc-expert` |
| SMT + Dead Letter Topic (poison pills) | `connect-cdc-expert` |
| Mapear tipos Postgres → Avro | `connect-cdc-expert` + `data-modeler` |

### Flink SQL (módulo 4)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Compute pool e statements | `flink-confluent-expert` |
| Watermarks, janelas, joins | `flink-confluent-expert` |
| CEP / MATCH_RECOGNIZE para fraude | `flink-confluent-expert` + `lab-architect` |
| Quando usar Flink vs ksqlDB vs Kafka Streams | `flink-confluent-expert` |

### Lakehouse (módulo 5)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Habilitar Tableflow em um topic | `tableflow-lakehouse-expert` |
| Evolução de schema na materialização | `tableflow-lakehouse-expert` + `data-modeler` |
| Integração com Glue / Snowflake Open Catalog / Unity | `tableflow-lakehouse-expert` |

### Real-Time AI (módulo 6)
| Tarefa | Agent recomendado |
|--------|-------------------|
| CREATE MODEL + ML_PREDICT no Flink SQL | `realtime-ai-expert` |
| Embeddings em tempo real e Vector Search | `realtime-ai-expert` |
| Streaming agents com contexto operacional | `realtime-ai-expert` |

### Produção (módulo 7)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Metrics API, consumer lag, alertas | `observability-cost-expert` |
| Otimização de custos (retenção, Tiered Storage, Freight) | `observability-cost-expert` |
| Multi-region e DR | `observability-cost-expert` |
| RBAC/ACLs em produção | `kafka-security-expert` |

### Dados & qualidade
| Tarefa | Agent recomendado |
|--------|-------------------|
| Design Avro schema para CDC | `data-modeler` |
| Estratégia de evolução de schema | `data-modeler` |
| Validar qualidade de dados sintéticos | `data-quality-expert` |
| Arquitetura Python dos geradores | `software-engineer` |
| Estruturar um novo lab ou camada do desafio | `lab-architect` |
| Formatos brasileiros (CPF, CNPJ, MCC) | `data-modeler` + ver [references/brazilian-data-formats.md](../references/brazilian-data-formats.md) |

---

**Última atualização**: 2026-07-11
