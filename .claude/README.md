# Claude Code Configuration — data-streaming-tour

> **Workshop pt-BR de streaming de dados ponta a ponta. Stack 100% Docker Compose: Kafka + Oracle CDC + Flink SQL + Iceberg/Trino. Domínio: pagamentos/financeiro.**

Esta pasta contém toda a configuração do Claude Code para construir e manter o material do workshop. As regras, hooks, agents e skills foram adaptados a partir de uma configuração de plataforma de produção e enxutos para o contexto pedagógico.

---

## Visão geral

| Componente | Detalhe |
|------------|---------|
| **Banco fonte** | Oracle XE 21c (em Docker, com supplemental logging para CDC) |
| **CDC** | Debezium Oracle Connector (Kafka Connect) |
| **Transporte** | Apache Kafka + Schema Registry (Confluent Platform Docker) |
| **Processamento** | Apache Flink 1.19 (self-managed, SQL Client) |
| **Lakehouse** | Apache Iceberg + Nessie catalog + MinIO + Trino |
| **Linguagem** | Python 3.11+ (geradores), SQL (Flink + Trino) |
| **Idioma** | Prosa em pt-BR · Código em inglês (identificadores, logs, tabelas) |
| **Licença** | MIT |

### Fluxo de dados

```
Oracle XE  ──CDC──►  Kafka Connect  ──►  Kafka + SR  ──►  Flink SQL  ──►  Iceberg (MinIO/Nessie)
(Pagamentos)         (Debezium)         (local)         (enrichment)     │
                                                                          └──► Trino (analytics)
```

---

## Estrutura de diretórios

```
.claude/
├── README.md                    # Este arquivo
├── rules.md                     # Padrões cross-workshop (Kafka, dados, CHANGELOG)
├── settings.json                # Hooks (lint, format, SQL, Avro PII)
├── settings.local.json          # Permissões locais (não commitada com segredos)
├── versions.lock                # Versões fixadas (Python, ferramentas)
│
├── hooks/                       # Scripts de lifecycle
│   ├── lint-python.sh          # ruff check + format em *.py
│   ├── format-check.sh         # black --line-length 100 em *.py
│   ├── validate-sql.sh         # avisa keywords minúsculas em *.sql
│   ├── terraform-fmt.sh        # terraform fmt + validate (caso opte por IaC futuramente)
│   ├── validate-avro-pii.sh    # bloqueia *.avsc com campos sensíveis sem confluent:tags
│   └── pre-commit-changelog.sh # bloqueia commit sem entrada no CHANGELOG
│
├── rules/                       # Regras escopadas por path
│   ├── python.md               # **/*.py — Decimal BRL, Enums, logging, LGPD
│   ├── sql.md                  # **/*.sql — Flink SQL, Oracle CDC, Trino
│   ├── testing.md              # **/tests/** — pytest, Hypothesis, testcontainers
│   ├── schemas.md              # **/*.avsc — namespace, tags PII/PCI, Debezium envelope
│   └── workshop.md             # **/ws-*/** — cenas, time-boxing, verifiabilidade
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
│   ├── confluent-platform-expert.md     # Confluent Platform Docker
│   ├── oracle-database-expert.md        # Oracle XE config & supplemental logging
│   ├── oracle-cdc-expert.md             # Debezium Oracle, LogMiner
│   ├── kafka-streaming-expert.md        # Producer/consumer patterns
│   ├── kafka-security-expert.md         # ACLs, SASL, segurança local
│   ├── flink-self-managed-expert.md     # Flink 1.19 Docker, SQL Client, checkpointing
│   ├── iceberg-expert.md                # Iceberg, Nessie, particionamento
│   ├── trino-expert.md                  # Trino + Iceberg connector
│   ├── data-modeler.md                  # Modelagem de entidades, evolução de schema
│   ├── data-quality-expert.md           # Validação e qualidade de dados sintéticos
│   ├── scenario-architect.md            # Orquestração de cenas
│   ├── software-engineer.md             # Padrões Python e arquitetura
│   └── logging-expert.md                # Logging estruturado
│
├── skills/                      # Capacidades executáveis
│   ├── README.md
│   ├── common/                  # code-reviewer, doc-generator, validation
│   ├── confluent/               # docker-compose, schema-registry, topics, connect, troubleshoot
│   ├── generators/              # schema-designer, generator-builder, kafka-producer, ...
│   └── workflow/                # topic-designer, connector-configurator, flink-job-builder, ...
│
├── references/                  # Documentação técnica
│   ├── README.md
│   ├── oracle-cdc-patterns.md
│   ├── kafka-producer-guide.md
│   ├── data-modeling.md
│   ├── faker-patterns.md
│   ├── brazilian-data-formats.md
│   ├── behavioral-patterns-guide.md
│   ├── poison-pill-patterns.md
│   └── workshop-stack-guide.md
│
└── instructions/                # Playbooks passo a passo
    ├── README.md
    ├── new-generator_v1.md
    ├── kafka-setup_v1.md
    ├── domain-extension_v1.md
    └── new-scenario_v1.md
```

---

## Hooks ([settings.json](settings.json))

| Evento | Trigger | Ação |
|--------|---------|------|
| PostToolUse | `Edit\|Write(*.py)` | [lint-python.sh](hooks/lint-python.sh) → `ruff check --fix` + `ruff format` |
| PostToolUse | `Edit\|Write(*.py)` | [format-check.sh](hooks/format-check.sh) → `black --line-length 100` |
| PostToolUse | `Edit\|Write(*.sql)` | [validate-sql.sh](hooks/validate-sql.sh) → avisa keywords minúsculas |
| PostToolUse | `Edit\|Write(*.tf\|*.tfvars)` | [terraform-fmt.sh](hooks/terraform-fmt.sh) → `terraform fmt` + `validate` (opcional) |
| PostToolUse | `Edit\|Write(*.avsc\|*.avro)` | [validate-avro-pii.sh](hooks/validate-avro-pii.sh) → **bloqueia** Avro com campos sensíveis sem `confluent:tags` |
| PreToolUse | `Bash(git commit*)` | [pre-commit-changelog.sh](hooks/pre-commit-changelog.sh) → **bloqueia** commit sem entrada no CHANGELOG |

Hooks falham *open* (não bloqueantes) quando ferramentas externas não estão instaladas — exceto o guardião de CHANGELOG e o validador de Avro PII, que bloqueiam por design.

---

## Regras (path-scoped)

Carregadas automaticamente por glob:

| Arquivo | Glob | Escopo |
|---------|------|--------|
| [python.md](rules/python.md) | `**/*.py` | Black/Ruff/mypy, `Decimal` para BRL, Enums, logging lazy, LGPD |
| [sql.md](rules/sql.md) | `**/*.sql` | Flink SQL (watermark/TTL/MATCH_RECOGNIZE), Oracle CDC, Trino |
| [testing.md](rules/testing.md) | `**/tests/**/*.py` | pytest, Hypothesis, testcontainers para Kafka/Oracle |
| [schemas.md](rules/schemas.md) | `**/*.avsc,**/schemas/**` | Namespace `com.workshop.payments`, tags PII/PCI, envelope Debezium |
| [workshop.md](rules/workshop.md) | `**/ws-*/**,**/workshop*/**,**/samples/**` | Cenas time-boxed, comandos verificáveis, cleanup, anti-patterns pedagógicos |

A regra de Terraform foi removida — o workshop é 100% Docker Compose. Se um cenário futuro precisar de IaC, restaurar `rules/terraform.md` do histórico do git.

---

## Quick start (para o autor do workshop)

A edição em construção é [`data-streaming-tour-2026/`](../data-streaming-tour-2026/). Cenas vivem dentro dela.

```
"Usando confluent-platform-expert, monte o docker-compose da cena de Kafka fundamentals (Kafka + Schema Registry + Control Center) em data-streaming-tour-2026/"
```
```
"Usando oracle-cdc-expert, gere supplemental logging para a tabela TRANSACOES no Oracle XE da cena de CDC"
```
```
"Usando flink-self-managed-expert, escreva uma cena com MATCH_RECOGNIZE para detectar 3 transações em <60s"
```
```
"Usando iceberg-expert + trino-expert, monte a cena de lakehouse com sink Flink → Iceberg e queries Trino"
```

---

**Última atualização**: 2026-05-11
**Mantido por**: Mateus Oliveira / Intelium Consulting
