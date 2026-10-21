# Claude Code Configuration — Bootcamp Confluent Cloud 2026

> **Labs hands-on e implementação de referência do Desafio Final do Bootcamp Confluent Cloud 2026 (DIO). Stack 100% Confluent Cloud gerenciado: Kafka + Schema Registry + Connect/CDC + Flink SQL + Tableflow/Iceberg + Real-Time AI. Domínio: pagamentos/detecção de fraude.**

Esta pasta contém toda a configuração do Claude Code para construir e manter o código do bootcamp. O programa completo (blocos, cursos e aulas) está em [`docs/Bootcamp_Confluent_Cloud_2026_v3.pdf`](../docs/Bootcamp_Confluent_Cloud_2026_v3.pdf).

---

## Visão geral

| Componente | Detalhe |
|------------|---------|
| **Plataforma** | Confluent Cloud (environments, clusters, service accounts, API keys via CLI `confluent`) |
| **Ingestão/CDC** | Conectores gerenciados no Confluent Cloud (Debezium source, sinks DW/NoSQL/Object Storage) |
| **Schemas** | Schema Registry + Data Contracts (Avro, compatibilidade BACKWARD) |
| **Processamento** | Flink SQL gerenciado (compute pools, statements) |
| **Lakehouse** | Tableflow → Apache Iceberg (catálogos: AWS Glue, Snowflake Open Catalog, Databricks Unity) |
| **Real-Time AI** | `CREATE MODEL` + `ML_PREDICT` no Flink SQL, embeddings e Vector Search |
| **Operação** | Metrics API, consumer lag, RBAC/ACLs, Tiered Storage, DR |
| **Fallback local** | Docker Compose enxuto (1 broker KRaft + Schema Registry) para demos offline sem gastar créditos |
| **Linguagem** | Python 3.11+ (geradores/producers), SQL (Flink) |
| **Idioma** | Prosa em pt-BR · Código em inglês (identificadores, logs, tabelas) |
| **Licença** | MIT |

### Fluxo de dados (Desafio Final — build-as-you-go)

```
Banco fonte ──CDC──►  Managed Connect  ──►  Kafka + SR   ──►  Flink SQL    ──►  Tableflow/Iceberg
(pagamentos)          (Debezium)            (Confluent        (enrichment +      │
                                             Cloud)            fraude + IA)      └──► Catálogo (Glue/Snowflake/Unity)
                                                │
                                                └──► Observabilidade (Metrics API, lag, custos)
```

Cada camada corresponde a um curso do bootcamp: ingestão (Curso 2), CDC (Curso 3), transformação (Curso 4), lakehouse (Curso 5), IA (Curso 6), observabilidade (Curso 7).

---

## Estrutura de diretórios

```
.claude/
├── README.md                    # Este arquivo
├── rules.md                     # Padrões cross-lab (Kafka, dados, CHANGELOG)
├── settings.json                # Hooks (lint, format, SQL, Avro PII)
├── settings.local.json          # Permissões locais (não commitada com segredos)
├── versions.lock                # Versões fixadas (Python, CLI, ferramentas)
│
├── hooks/                       # Scripts de lifecycle
│   ├── lint-python.sh          # ruff check + format em *.py
│   ├── format-check.sh         # black --line-length 100 em *.py
│   ├── validate-sql.sh         # avisa keywords minúsculas em *.sql
│   ├── terraform-fmt.sh        # terraform fmt + validate (provider confluent p/ IaC opcional)
│   ├── validate-avro-pii.sh    # bloqueia *.avsc com campos sensíveis sem confluent:tags
│   └── pre-commit-changelog.sh # bloqueia commit sem entrada no CHANGELOG
│
├── rules/                       # Regras escopadas por path
│   ├── python.md               # **/*.py — Decimal BRL, Enums, logging, LGPD
│   ├── sql.md                  # **/*.sql — Flink SQL no Confluent Cloud
│   ├── testing.md              # **/tests/** — pytest, Hypothesis, testcontainers
│   ├── schemas.md              # **/*.avsc — namespace, tags PII/PCI, envelope Debezium
│   └── labs.md                 # **/labs/**, **/desafio-final/** — time-boxing, verificabilidade, teardown
│
├── workflows/                   # RESEARCH → PLAN → ACT → VALIDATE
│   ├── README.md
│   ├── research.md
│   ├── plan.md
│   ├── act.md
│   ├── validate.md
│   └── quick-tasks.md
│
├── agents/                      # Agents especialistas
│   ├── README.md
│   ├── confluent-cloud-expert.md        # CLI, environments, clusters, service accounts, Cluster Linking
│   ├── connect-cdc-expert.md            # Conectores gerenciados, Debezium, SMT, DLQ
│   ├── kafka-streaming-expert.md        # Producer/consumer patterns, semânticas de entrega
│   ├── kafka-security-expert.md         # RBAC, ACLs, API keys, mascaramento PII/LGPD
│   ├── flink-confluent-expert.md        # Flink SQL gerenciado, janelas, joins, MATCH_RECOGNIZE
│   ├── tableflow-lakehouse-expert.md    # Tableflow, Iceberg, catálogos, consumo analítico
│   ├── realtime-ai-expert.md            # CREATE MODEL, ML_PREDICT, Vector Search, streaming agents
│   ├── observability-cost-expert.md     # Metrics API, lag, alertas, custos, DR
│   ├── data-modeler.md                  # Modelagem de entidades, evolução de schema
│   ├── data-quality-expert.md           # Validação e qualidade de dados sintéticos
│   ├── lab-architect.md                 # Estrutura de labs por módulo, camadas do desafio
│   ├── software-engineer.md             # Padrões Python e arquitetura
│   └── logging-expert.md                # Logging estruturado
│
├── skills/                      # Capacidades executáveis
│   ├── README.md
│   ├── common/                  # code-reviewer, doc-generator, validation
│   ├── confluent/               # cloud-setup, topics, schema-registry, managed-connector, tableflow, troubleshoot, local-dev-fallback
│   ├── generators/              # schema-designer, generator-builder, kafka-producer, ...
│   └── workflow/                # topic-designer, connector-configurator, flink-job-builder, ...
│
├── references/                  # Documentação técnica
│   ├── README.md
│   ├── bootcamp-stack-guide.md
│   ├── cdc-connect-patterns.md
│   ├── kafka-producer-guide.md
│   ├── data-modeling.md
│   ├── faker-patterns.md
│   ├── brazilian-data-formats.md
│   ├── behavioral-patterns-guide.md
│   └── poison-pill-patterns.md
│
└── instructions/                # Playbooks passo a passo
    ├── README.md
    ├── cloud-setup_v1.md
    ├── new-generator_v1.md
    ├── new-lab_v1.md
    ├── domain-extension_v1.md
    └── desafio-final_v1.md
```

---

## Hooks ([settings.json](settings.json))

| Evento | Trigger | Ação |
|--------|---------|------|
| PostToolUse | `Edit\|Write(*.py)` | [lint-python.sh](hooks/lint-python.sh) → `ruff check --fix` + `ruff format` |
| PostToolUse | `Edit\|Write(*.py)` | [format-check.sh](hooks/format-check.sh) → `black --line-length 100` |
| PostToolUse | `Edit\|Write(*.sql)` | [validate-sql.sh](hooks/validate-sql.sh) → avisa keywords minúsculas |
| PostToolUse | `Edit\|Write(*.tf\|*.tfvars)` | [terraform-fmt.sh](hooks/terraform-fmt.sh) → `terraform fmt` + `validate` (provider confluent, opcional) |
| PostToolUse | `Edit\|Write(*.avsc\|*.avro)` | [validate-avro-pii.sh](hooks/validate-avro-pii.sh) → **bloqueia** Avro com campos sensíveis sem `confluent:tags` |
| PreToolUse | `Bash(git commit*)` | [pre-commit-changelog.sh](hooks/pre-commit-changelog.sh) → **bloqueia** commit sem entrada no CHANGELOG |

Hooks falham *open* (não bloqueantes) quando ferramentas externas não estão instaladas — exceto o guardião de CHANGELOG e o validador de Avro PII, que bloqueiam por design.

---

## Regras (path-scoped)

Carregadas automaticamente por glob:

| Arquivo | Glob | Escopo |
|---------|------|--------|
| [python.md](rules/python.md) | `**/*.py` | Black/Ruff/mypy, `Decimal` para BRL, Enums, logging lazy, LGPD |
| [sql.md](rules/sql.md) | `**/*.sql` | Flink SQL no Confluent Cloud (watermark, janelas, MATCH_RECOGNIZE, ML_PREDICT) |
| [testing.md](rules/testing.md) | `**/tests/**/*.py` | pytest, Hypothesis, testcontainers para Kafka local |
| [schemas.md](rules/schemas.md) | `**/*.avsc,**/schemas/**` | Namespace `com.bootcamp.payments`, tags PII/PCI, envelope Debezium |
| [labs.md](rules/labs.md) | `**/labs/**,**/desafio-final/**` | Labs time-boxed, comandos verificáveis, teardown de recursos cloud |

---

## Quick start (para o autor do bootcamp)

O código entregável vive em [`repo/`](../repo/): `dataset/` + `schemas/` (geradores e contratos), `docs/cursoN/` (guias por aula) e `desafio-final/`.

```
"Usando confluent-cloud-expert, crie o script de setup da Aula 0: environment, cluster básico, API keys e teardown"
```
```
"Usando connect-cdc-expert, configure o source connector Debezium gerenciado para a tabela transactions no lab do módulo 3"
```
```
"Usando flink-confluent-expert, escreva o statement com MATCH_RECOGNIZE para detectar 3 transações em <60s no módulo 4"
```
```
"Usando tableflow-lakehouse-expert, materialize o topic de transações enriquecidas como tabela Iceberg via Tableflow"
```
```
"Usando realtime-ai-expert, adicione ML_PREDICT de classificação de fraude ao pipeline do desafio final"
```

---

**Última atualização**: 2026-07-11
**Mantido por**: Mateus Oliveira / Intelium Consulting
