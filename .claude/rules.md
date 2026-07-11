# Platform Rules — data-streaming-tour Workshop

> **Padrões cross-workshop. Regras específicas por linguagem/path vivem em [`rules/`](rules/).**

| Escopo | Arquivo |
|--------|---------|
| Python (`**/*.py`) | [rules/python.md](rules/python.md) |
| SQL — Flink, Oracle, Trino (`**/*.sql`) | [rules/sql.md](rules/sql.md) |
| Tests (`**/tests/**/*.py`) | [rules/testing.md](rules/testing.md) |
| Avro schemas (`**/*.avsc`, `**/schemas/**`) | [rules/schemas.md](rules/schemas.md) |
| Material de workshop (`**/ws-*/**`) | [rules/workshop.md](rules/workshop.md) |

Este arquivo cobre o que se aplica entre workshops: padrões Kafka, geração de dados, restrições de domínio (pagamentos), organização de arquivos e política de CHANGELOG.

---

## Kafka Standards (local Docker)

### Topic Naming

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}
```

`stage` no workshop é sempre `dev` ou `demo` — não há `prod`.

CDC topics (Debezium Oracle):
```
dev.payments.oracle-pagamentos.contas.cdc.v1
dev.payments.oracle-pagamentos.cartoes.cdc.v1
dev.payments.oracle-pagamentos.transacoes.cdc.v1
```

Topics enriquecidos (saída do Flink):
```
dev.payments.flink.transacoes.enriched.v1
dev.payments.flink.fraude.detected.v1
dev.payments.flink.saldo.aggregated.v1
```

Synthetic / generators:
```
dev.payments.gen.transacoes.created.v1
demo.payments.gen.contas.snapshot.v1
```

### Producer Configuration — Local Docker

```python
local_producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "ws-gen-{domain}",
    "acks": "all",
    "retries": 3,
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.size": 16384,
    "compression.type": "snappy",
}
```

### Schema Registry — Local

```python
schema_registry_config = {
    "url": "http://localhost:8081",
    # No auth no workshop local. Em produção: basic.auth.user.info=<key>:<secret>
}
```

Detalhes de Avro (campos, tags, evolução) → [rules/schemas.md](rules/schemas.md).

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
- **Nunca** usar dados de produção em qualquer cenário do workshop
- PAN de cartão **sempre mascarado** antes de chegar a topic (BIN + last4)
- Pseudonimização preferível à anonimização quando reversibilidade é necessária

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

### Anti-fraud patterns (para cenas de Flink CEP)

| Padrão | Janela | Indicador |
|--------|--------|-----------|
| Velocidade | 3+ transações em <60s | `RULE_VELOCITY` |
| Geolocalização impossível | 2 UFs diferentes em <10min | `RULE_GEO_IMPOSSIBLE` |
| Valor atípico | Transação > 10× média do cartão (últimos 90d) | `RULE_AMOUNT_OUTLIER` |
| Madrugada | Transação entre 02h–05h em conta sem histórico noturno | `RULE_NIGHT_OWL` |

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
data-streaming-tour/
├── README.md
├── CHANGELOG.md
├── .claude/                            # Config Claude Code (rules, agents, skills, hooks)
└── data-streaming-tour-2026/           # Edição em construção
    ├── README.md
    ├── shared/                         # Geradores, schemas Avro, utilitários (compartilhados entre cenas)
    │   ├── generators/
    │   │   ├── base.py                 # BaseGenerator ABC
    │   │   ├── payments/
    │   │   │   ├── contas.py
    │   │   │   ├── cartoes.py
    │   │   │   └── transacoes.py
    │   │   └── common/brazilian_formats.py
    │   ├── schemas/payments/*.avsc
    │   └── producers/kafka_producer.py
    └── ws-XX-<tema>/                   # Cenas — estrutura sugerida, não imposta
        ├── README.md
        ├── docker-compose.yml
        └── scenes/
```

> A organização interna de `data-streaming-tour-2026/` é definida pelo autor da edição conforme o conteúdo cresce. As regras em [`rules/workshop.md`](rules/workshop.md) (cenas time-boxed, comandos verificáveis, cleanup) aplicam-se automaticamente ao path `**/ws-*/**` em qualquer lugar do repo.

---

## CHANGELOG

### Política

Material de workshop evolui rápido por iteração de feedback. O hook [pre-commit-changelog.sh](hooks/pre-commit-changelog.sh) bloqueia commits que mexem em código sem entrada no `CHANGELOG.md` na raiz.

**Escopo**: vale para `.py`, `.sql`, `.tf`, `.tfvars`, `.avsc`, `.yaml`, `.yml`, `.json`. Mudanças apenas em `*.md` (READMEs, docs de cena) **não** exigem CHANGELOG — o git log já é suficiente para documentação.

### Formato

```markdown
## [Unreleased]

### Added
- tour-2026/cdc: Cena sobre filtragem de eventos CDC com Single Message Transforms `(@author)`

### Changed
- tour-2026/kafka-fundamentals: Reduz tópico inicial de 6 para 3 partições para acelerar setup `(@author)`

### Fixed
- tour-2026/flink-sql: Corrige watermark delay de 5s para 30s na cena de janelas tumbling `(@author)`
```

### Regras

1. Sempre adicionar sob `## [Unreleased]` no topo
2. Categorias: Added, Changed, Deprecated, Removed, Fixed, Security, Documentation
3. Imperativo: "Adiciona X" / "Add X" — não "Adicionado" / "Added"
4. Tag de autor: `(@MateusHenriqueOliveira)` ou `(@claude-code)`
5. Prefixo da edição/cena quando aplicável: `tour-2026/<tema>:` ou simplesmente `<tema>:`
6. Específico: "tour-2026/lakehouse: Adiciona MERGE com upsert para CDC delete events" — não "Adiciona merge"

---

**Última atualização**: 2026-05-11
