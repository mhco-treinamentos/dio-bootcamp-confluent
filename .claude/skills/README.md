# Skills Registry — data-streaming-tour

> **Capacidades executáveis para construir o workshop de streaming (Kafka + Oracle CDC + Flink + Iceberg/Trino).**

## Confluent Platform Skills (local Docker)

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `oracle-cdc-connector` | Deploy de Oracle CDC Source / Debezium em Kafka Connect | "Deploy connector Oracle CDC" |
| `connect` | Gerenciamento geral de Kafka Connect (JDBC, Debezium, S3, etc.) | "Deploy um connector" |
| `docker-compose` | Compose para a stack local (Kafka, SR, Connect, Oracle) | "Gerar docker-compose.yml" |
| `schema-registry` | Registro de schemas (Avro/JSON/Protobuf) | "Registrar um schema" |
| `topics` | Criação e gerenciamento de topics | "Criar um topic Kafka" |
| `troubleshoot` | Debug de issues comuns (connector failure, schema incompat) | "Investigar falha de connector" |

## Workflow Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `topic-designer` | Design de topic com naming, partitioning, retention | "Desenhar topic para..." |
| `connector-configurator` | Configurar connector com SMTs | "Configurar connector para..." |
| `flink-job-builder` | Construir job Flink SQL | "Criar job Flink para..." |
| `data-contract-designer` | Criar data contract (YAML) | "Criar contrato de dados para..." |
| `compliance-checker` | Validar conformidade (LGPD, PCI-DSS) | "Verificar compliance de..." |
| `performance-analyzer` | Analisar performance Kafka/Flink | "Analisar performance de..." |
| `governance-reporter` | Gerar relatórios de governança | "Gerar relatório de governança" |

## Common Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `code-reviewer` | Review de qualidade de código | "Review this code" |
| `doc-generator` | Geração de documentação | "Document this component" |
| `validation` | Validar configurações | "Validate this config" |

## Generator Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `schema-designer` | Design de schemas Avro/JSON | "Desenhar um schema para..." |
| `generator-builder` | Construir gerador baseado em Faker | "Criar gerador para..." |
| `kafka-producer` | Configurar producer Kafka | "Configurar producer para..." |
| `data-profiler` | Profile e validação de dados gerados | "Profile this generated data" |
| `poison-pill-injector` | Injetar problemas de qualidade de dado | "Injetar poison pills" |
| `scenario-builder` | Construir cenas E2E | "Construir um cenário" |

## Exemplos de uso

### Confluent Platform (local)
```
"Use docker-compose para gerar a stack da cena de Kafka fundamentals (Kafka + SR + Control Center)"
"Use oracle-cdc-connector para deploy do Debezium na tabela TRANSACOES"
"Use schema-registry para registrar o schema Avro de Transacao"
"Use topics para criar dev.payments.gen.transacoes.created.v1 com 6 partições"
```

### Workflow
```
"Use topic-designer para desenhar os topics CDC de payments (contas, cartões, transações)"
"Use connector-configurator para configurar Oracle CDC com mascaramento de CPF/PAN"
"Use compliance-checker para validar LGPD + PCI-DSS nos schemas"
"Use data-contract-designer para criar contrato de transacao.v1"
```
