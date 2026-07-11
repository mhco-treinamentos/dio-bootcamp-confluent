# Skills Registry — Bootcamp Confluent Cloud 2026

> **Capacidades executáveis para construir os labs e o Desafio Final do bootcamp (Confluent Cloud: Kafka + SR + Connect/CDC + Flink + Tableflow). Domínio: pagamentos/detecção de fraude.**

## Confluent Cloud Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `cloud-setup` | Aula 0: login, environment, cluster, API keys, service accounts, teardown | "Setup do Confluent Cloud" |
| `topics` | Criar/gerenciar topics via `confluent kafka topic` | "Criar um topic Kafka" |
| `schema-registry` | Schemas Avro/JSON/Protobuf, compatibilidade BACKWARD | "Registrar um schema" |
| `connect` | Plugins e ciclo de vida de conectores gerenciados | "Deploy um connector" |
| `managed-connector` | Source Debezium Postgres (CDC), sinks, SMT, DLQ — módulo 3 | "Configurar CDC / DLQ" |
| `tableflow` | Materializar topics como tabelas Iceberg — módulo 5 | "Habilitar Tableflow" |
| `troubleshoot` | Debug: auth, conectores, SR, lag, Flink, billing | "Investigar falha" |
| `local-dev-fallback` | Compose enxuto (KRaft + SR) para demos offline sem gastar créditos | "Stack local / gravar offline" |

## Workflow Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `topic-designer` | Design de topic: naming (rules.md), particionamento, retenção | "Desenhar topic para..." |
| `connector-configurator` | Design de config de conector gerenciado com SMTs e DLQ | "Configurar connector para..." |
| `flink-job-builder` | Statements Flink SQL gerenciados (janelas, MATCH_RECOGNIZE) | "Criar statement Flink para..." |
| `data-contract-designer` | Data contract (YAML/Avro) com BACKWARD e tags PII/PCI | "Criar contrato de dados para..." |
| `compliance-checker` | LGPD + PCI-DSS via Stream Governance (Live 3) | "Verificar compliance de..." |
| `performance-analyzer` | Performance via Metrics API (lag, CFUs, custos) | "Analisar performance de..." |
| `governance-reporter` | Relatórios de governança (catálogo, PII audit, acesso) | "Gerar relatório de governança" |

## Generator Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `schema-designer` | Design de schemas Avro/JSON (namespace `com.bootcamp.payments`) | "Desenhar um schema para..." |
| `generator-builder` | Geradores Faker (Kafka-direct e Postgres-direct p/ CDC) | "Criar gerador para..." |
| `kafka-producer` | Producer com perfil `KAFKA_PROFILE=cloud\|local` | "Configurar producer para..." |
| `data-profiler` | Profile e validação de dados gerados | "Profile this generated data" |
| `poison-pill-injector` | Injetar problemas de qualidade (lab de DLQ) | "Injetar poison pills" |
| `scenario-builder` | Cenários E2E para os labs por módulo | "Construir um cenário / lab" |

## Common Skills

| Skill | Descrição | Trigger |
|-------|-----------|---------|
| `code-reviewer` | Review de qualidade de código | "Review this code" |
| `doc-generator` | Geração de documentação | "Document this component" |
| `validation` | Validar configurações | "Validate this config" |
| `excalidraw-diagram-builder` | Diagramas de arquitetura (.excalidraw) | "Criar diagrama de..." |

## Exemplos de uso

### Confluent Cloud
```
"Use cloud-setup para criar o environment da Aula 0 com teardown.sh"
"Use topics para criar dev.payments.gen.transactions.created.v1 com 6 partições"
"Use managed-connector para o Debezium Postgres capturando public.transactions"
"Use schema-registry para registrar o Avro de TransactionCreated com BACKWARD"
"Use tableflow para materializar dev.payments.flink.transactions.enriched.v1 como Iceberg"
```

### Workflow
```
"Use topic-designer para desenhar os topics CDC de payments (accounts, cards, transactions)"
"Use flink-job-builder para o MATCH_RECOGNIZE de RULE_VELOCITY no módulo 4"
"Use compliance-checker para validar LGPD + PCI nos schemas antes da Live 3"
"Use data-contract-designer para criar o contrato de transactions.cdc.v1"
```

---

**Última atualização**: 2026-07-11
