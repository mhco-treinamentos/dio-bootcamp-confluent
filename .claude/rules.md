# Platform Rules — Bootcamp Confluent Cloud 2026

> **Padrões cross-lab. Regras específicas por linguagem/path vivem em [`rules/`](rules/).**

| Escopo | Arquivo |
|--------|---------|
| Python (`**/*.py`) | [rules/python.md](rules/python.md) |
| SQL — Flink no Confluent Cloud (`**/*.sql`) | [rules/sql.md](rules/sql.md) |
| Tests (`**/tests/**/*.py`) | [rules/testing.md](rules/testing.md) |
| Avro schemas (`**/*.avsc`, `**/schemas/**`) | [rules/schemas.md](rules/schemas.md) |
| Labs e Desafio Final (`**/labs/**`, `**/desafio-final/**`) | [rules/labs.md](rules/labs.md) |

Este arquivo cobre o que se aplica entre labs: padrões Kafka/Confluent Cloud, segredos, geração de dados, restrições de domínio (pagamentos), organização de arquivos e política de CHANGELOG.

---

## Kafka Standards — Confluent Cloud

### Topic Naming

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}
```

`stage` no bootcamp é sempre `dev` ou `lab` — não há `prod`.

CDC topics (Debezium managed connector):
```
dev.payments.pg-payments.accounts.cdc.v1
dev.payments.pg-payments.cards.cdc.v1
dev.payments.pg-payments.transactions.cdc.v1
```

Topics enriquecidos (saída do Flink):
```
dev.payments.flink.transactions.enriched.v1
dev.payments.flink.fraud.detected.v1
dev.payments.flink.balance.aggregated.v1
```

Synthetic / generators:
```
dev.payments.gen.transactions.created.v1
lab.payments.gen.accounts.snapshot.v1
```

### Defaults de confiabilidade (conceitos-âncora do bootcamp)

Sempre que criar topics ou explicar entrega, usar e citar os defaults: **RF=3**, **min.insync.replicas=2**, **acks=all**. As semânticas de entrega (at-most-once, at-least-once, exactly-once) são praticadas ao longo dos labs e consolidadas no módulo 7.

### Producer Configuration — Confluent Cloud

Credenciais **sempre** via variáveis de ambiente — nunca hardcoded, nunca commitadas:

```python
import os

cloud_producer_config = {
    "bootstrap.servers": os.environ["CC_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CC_API_KEY"],
    "sasl.password": os.environ["CC_API_SECRET"],
    "client.id": "bootcamp-gen-{domain}",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.size": 16384,
    "compression.type": "snappy",
}
```

### Producer Configuration — Fallback local (Docker)

Para demos offline/gravação sem gastar créditos (ver [skills/confluent/local-dev-fallback.md](skills/confluent/local-dev-fallback.md)):

```python
local_producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "bootcamp-gen-{domain}",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "compression.type": "snappy",
}
```

O seletor cloud/local deve ser um único ponto de configuração (env var `KAFKA_PROFILE=cloud|local`), nunca código duplicado.

### Schema Registry

```python
schema_registry_config = {
    "url": os.environ["SR_URL"],
    "basic.auth.user.info": f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}",
}
# Fallback local: url=http://localhost:8081, sem auth
```

Compatibilidade default do bootcamp: **BACKWARD** (versionar eventos sem quebrar consumers — Curso 2, Aula 3). Detalhes de Avro (campos, tags, evolução) → [rules/schemas.md](rules/schemas.md).

### Segredos e créditos

- `.env` local com `CC_*`/`SR_*` — sempre no `.gitignore`; fornecer `.env.example` sem valores
- Um service account por lab, com least privilege (ACLs mínimas) — nunca usar a API key de admin nos producers
- **Teardown obrigatório** ao final de cada lab: todo script de setup tem um `teardown.sh` par — créditos gratuitos (US$ 400/30 dias) acabam rápido com clusters ociosos

---

## Data Generation Rules

### Realismo

| Aspecto | Requisito |
|---------|-----------|
| Consistência temporal | `created_at` < `authorized_at` < `settled_at` |
| Integridade referencial | `card_id` referencia `account_id` que existe |
| Distribuição estatística | Valores de transação log-normal; merchants Pareto |
| Restrições de domínio | Saldo não fica negativo (exceto cheque especial); fraude < 1% das transações |

### Formatos brasileiros

| Campo | Formato | Validação |
|-------|---------|-----------|
| CPF | `XXXXXXXXXXX` (11 dígitos no schema, formatado na apresentação) | Check digit válido |
| CNPJ | `XXXXXXXXXXXXXX` (14 dígitos) | Check digit válido |
| Telefone | `+55XXXXXXXXXXX` | DDD válido |
| CEP | `XXXXXXXX` | 8 dígitos |
| Conta bancária | `agencia` (4 dígitos) + `conta` (até 13 dígitos) + DV | Sem máscara em topic |
| Cartão (PAN) | `XXXXXXXX******XXXX` | Sempre mascarado em topic — nunca PAN cleartext |
| MCC | 4 dígitos | Tabela ISO 18245 |

### PII / PCI

- Sempre seed determinístico (`seed=42` em testes; configurável em geradores)
- PII por entidade é consistente entre runs (mesmo `account_id` → mesmo CPF)
- **Nunca** usar dados de produção em qualquer lab do bootcamp
- PAN de cartão **sempre mascarado** antes de chegar a topic (BIN + last4)
- Pseudonimização preferível à anonimização quando reversibilidade é necessária
- Anonimização on-the-fly com Flink SQL é conteúdo da Live 3 — labs de LGPD usam esses geradores

---

## Domain Rules — Payments

| Restrição | Regra |
|-----------|-------|
| Valores monetários | BRL, `Decimal` com 2 casas (ver [rules/python.md](rules/python.md)), nunca `float` |
| Distribuição de valor | Log-normal com mediana ~R$ 80, cauda longa até ~R$ 10.000 |
| Transações por conta | Pareto: 20% das contas geram 80% das transações |
| Horário | Distribuição bimodal: pico 11–13h e 18–21h (horário de Brasília) |
| Taxa de fraude | 0.5–1.5% das transações (realista para PIX/cartão) |
| Taxa de aprovação | 92–97% (resto: saldo insuficiente, suspeita, vencido) |
| MCC | Top 10 MCCs concentram ~70% (supermercado, combustível, restaurantes, e-commerce) |
| Geografia | UF da transação correlacionada com UF do cartão (≥85% match) |

### Anti-fraud patterns (para labs de Flink e Real-Time AI)

| Padrão | Janela | Indicador |
|--------|--------|-----------|
| Velocidade | 3+ transações em <60s | `RULE_VELOCITY` |
| Geolocalização impossível | 2 UFs diferentes em <10min | `RULE_GEO_IMPOSSIBLE` |
| Valor atípico | Transação > 10× média do cartão (últimos 90d) | `RULE_AMOUNT_OUTLIER` |
| Madrugada | Transação entre 02h–05h em conta sem histórico noturno | `RULE_NIGHT_OWL` |

Regras determinísticas → Flink `MATCH_RECOGNIZE` (módulo 4); classificação por modelo → `ML_PREDICT` (módulo 6).

---

## Generator Architecture

### Base interface

```python
from abc import ABC, abstractmethod
from typing import Iterator


class BaseGenerator(ABC):
    """Base class para todos os geradores de dados sintéticos."""

    @abstractmethod
    def generate(self, count: int) -> Iterator[dict]:
        """Yield `count` registros sintéticos."""

    @abstractmethod
    def get_schema(self) -> dict:
        """Retorna o schema Avro dos registros gerados."""
```

Style Python, logging, Decimal/enum → [rules/python.md](rules/python.md).
Schemas Avro (namespace, tags, evolução) → [rules/schemas.md](rules/schemas.md).
Tests → [rules/testing.md](rules/testing.md).

---

## File Organization

```
bootcamp-confluent/
├── README.md
├── CHANGELOG.md
├── docs/                               # Programa do bootcamp (PDF)
├── .claude/                            # Config Claude Code (rules, agents, skills, hooks)
└── repo/                               # Código entregável
    ├── README.md
    ├── dataset/ + schemas/             # Dados e contratos (reuso entre labs)
    │   ├── generators/
    │   │   ├── base.py                 # BaseGenerator ABC
    │   │   ├── payments/
    │   │   │   ├── accounts.py
    │   │   │   ├── cards.py
    │   │   │   └── transactions.py
    │   │   └── common/brazilian_formats.py
    │   ├── schemas/payments/*.avsc
    │   └── producers/kafka_producer.py  # Profile cloud|local
    ├── labs/
    │   ├── modulo-02-plataforma/       # Primeira mensagem, SR, Data Contracts
    │   ├── modulo-03-connect-cdc/      # Managed connectors, Debezium, SMT, DLQ
    │   ├── modulo-04-flink/            # Statements, janelas, joins, MATCH_RECOGNIZE
    │   ├── modulo-05-lakehouse/        # Tableflow, Iceberg, catálogos
    │   ├── modulo-06-realtime-ai/      # CREATE MODEL, ML_PREDICT, Vector Search
    │   └── modulo-07-producao/         # Metrics API, RBAC, custos, DR
    └── desafio-final/                  # Pipeline end-to-end de referência (padrão DIO)
```

> Cada lab corresponde a um curso do bootcamp e é construído incrementalmente (build-as-you-go) rumo ao Desafio Final. As regras em [`rules/labs.md`](rules/labs.md) (time-boxing, comandos verificáveis, teardown) aplicam-se automaticamente aos paths `**/labs/**` e `**/desafio-final/**`.

---

## CHANGELOG

### Política

Material de bootcamp evolui rápido por iteração de feedback. O hook [pre-commit-changelog.sh](hooks/pre-commit-changelog.sh) bloqueia commits que mexem em código sem entrada no `CHANGELOG.md` na raiz.

**Escopo**: vale para `.py`, `.sql`, `.tf`, `.tfvars`, `.avsc`, `.yaml`, `.yml`, `.json`. Mudanças apenas em `*.md` (READMEs, docs de lab) **não** exigem CHANGELOG — o git log já é suficiente para documentação.

### Formato

```markdown
## [Unreleased]

### Added
- modulo-03: Lab de DLQ com poison pill injetada via SMT `(@author)`

### Changed
- modulo-02: Reduz tópico inicial de 6 para 3 partições para acelerar setup `(@author)`

### Fixed
- modulo-04: Corrige watermark delay de 5s para 30s no lab de janelas tumbling `(@author)`
```

### Regras

1. Sempre adicionar sob `## [Unreleased]` no topo
2. Categorias: Added, Changed, Deprecated, Removed, Fixed, Security, Documentation
3. Imperativo: "Adiciona X" / "Add X" — não "Adicionado" / "Added"
4. Tag de autor: `(@MateusHenriqueOliveira)` ou `(@claude-code)`
5. Prefixo do módulo/lab quando aplicável: `modulo-0X:` ou `desafio-final:`
6. Específico: "modulo-05: Adiciona evolução de schema com type widening no Tableflow" — não "Adiciona evolução"

---

**Última atualização**: 2026-07-11
