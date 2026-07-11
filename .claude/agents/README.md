# Agent Fleet — data-streaming-tour

> **Agentes especializados para construir e manter o workshop pt-BR de streaming (Kafka + Oracle CDC + Flink + Iceberg/Trino) com domínio de pagamentos.**

## Agentes de Plataforma & Stack

| Agent | Domínio | Expertise |
|-------|---------|-----------|
| `confluent-platform-expert` | Confluent Platform (Docker) | Docker Compose, Kafka broker, Schema Registry, Control Center, community edition |
| `oracle-database-expert` | Oracle DB | Arquitetura, tipos de dado, supplemental logging, LogMiner, Oracle XE em Docker |
| `oracle-cdc-expert` | Oracle CDC | Debezium Oracle connector, Confluent Oracle CDC Source, snapshot strategies, SMTs |
| `kafka-streaming-expert` | Kafka Streaming | Producers, topics, schemas, partitioning, throughput, idempotência |
| `kafka-security-expert` | Segurança Kafka | SASL/SSL, ACLs, API keys (em laboratório), mascaramento PII/PCI, LGPD |
| `flink-self-managed-expert` | Flink self-managed | Flink 1.19 em Docker, JAR classloader, S3 plugin, SQL Client, checkpointing |
| `iceberg-expert` | Iceberg | Partitioning, catalogs (Nessie/Glue/Hive), schema evolution, compaction, manutenção |
| `trino-expert` | Trino | Iceberg connector, otimização de query, time travel, integração com Superset/BI |

## Agentes de Dados & Engenharia

| Agent | Domínio | Expertise |
|-------|---------|-----------|
| `data-modeler` | Modelagem | Design de entidades, evolução de schema, relacionamentos, distribuições |
| `data-quality-expert` | Qualidade | Validação, realismo, distribuições estatísticas em dados sintéticos |
| `scenario-architect` | Cenários | Orquestração de cenas, máquinas de estado, poison pills, fluxos comportamentais |
| `software-engineer` | Engenharia de Software | Padrões Python, arquitetura, testing, performance |
| `logging-expert` | Logging | Logging estruturado, níveis, formato JSON |

## Como usar

### Invocação direta
```
"Usando confluent-platform-expert, monte o docker-compose do a cena de Kafka fundamentals com Kafka + Schema Registry"
"Usando oracle-cdc-expert, configure Debezium para CDC da tabela TRANSACOES em Oracle XE"
"Usando flink-self-managed-expert, escreva uma cena com MATCH_RECOGNIZE para detectar fraude por velocidade"
"Usando iceberg-expert + trino-expert, monte o sink Flink→Iceberg do a cena de lakehouse e queries Trino"
```

### Multi-agent
```
"Primeiro use oracle-database-expert para preparar supplemental logging,
depois use oracle-cdc-expert para configurar o connector Debezium,
depois use kafka-streaming-expert para validar os topics CDC gerados"
```

## Guia de seleção

### Kafka & Confluent Platform (workshop local)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Subir Kafka + SR via Docker Compose | `confluent-platform-expert` |
| Configurar topics, partições, retention | `kafka-streaming-expert` |
| Producer idempotente | `kafka-streaming-expert` |
| ACL e mascaramento de PII | `kafka-security-expert` |

### Oracle CDC
| Tarefa | Agent recomendado |
|--------|-------------------|
| Preparar Oracle XE para CDC | `oracle-database-expert` |
| Habilitar supplemental logging | `oracle-database-expert` |
| Configurar Debezium / Oracle CDC Source | `oracle-cdc-expert` |
| Mapear tipos Oracle → Avro | `oracle-database-expert` + `data-modeler` |
| Troubleshoot CDC lag | `oracle-cdc-expert` |

### Flink SQL (self-managed)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Build da imagem Flink com JARs + S3 plugin | `flink-self-managed-expert` |
| Debug de checkpoint / classloader | `flink-self-managed-expert` |
| Watermarks, windows, joins | `flink-self-managed-expert` |
| CEP / MATCH_RECOGNIZE para fraude | `flink-self-managed-expert` + `scenario-architect` |

### Lakehouse (Iceberg + Trino)
| Tarefa | Agent recomendado |
|--------|-------------------|
| Design de tabelas Iceberg (PK, partition, catalog) | `iceberg-expert` |
| Manutenção (compaction, snapshot expiry) | `iceberg-expert` |
| Configurar Trino com Nessie + MinIO | `trino-expert` |
| Otimizar queries Trino em Iceberg | `trino-expert` |
| Wire Trino → Superset | `trino-expert` |

### Dados & qualidade
| Tarefa | Agent recomendado |
|--------|-------------------|
| Design Avro schema para CDC | `data-modeler` |
| Estratégia de evolução de schema | `data-modeler` |
| Validar qualidade de dados sintéticos | `data-quality-expert` |
| Arquitetura Python dos geradores | `software-engineer` |
| Orquestrar cena de demonstração | `scenario-architect` |
| Formatos brasileiros (CPF, CNPJ, MCC) | `data-modeler` + ver [references/brazilian-data-formats.md](../references/brazilian-data-formats.md) |
