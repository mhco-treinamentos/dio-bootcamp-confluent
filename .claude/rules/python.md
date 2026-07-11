---
globs: "**/*.py"
---

# Python Rules

## Style
- Formatter: Black (line length: 100)
- Linter: Ruff
- Type checker: mypy (strict em código de `shared/`; relaxado em scripts de lab)
- Type hints: obrigatórios em toda função pública
- Docstrings: estilo NumPy

## Money & Precision
- Sempre `Decimal` para valores monetários (BRL) — nunca `float`
- Import: `from decimal import Decimal`
- Campos de pagamentos: `valor_transacao`, `saldo`, `limite`, `valor_aprovado`, `valor_estornado`

## Enums
- Sempre `Enum` para campos de status/tipo — nunca strings cruas
- Definir em `enums.py` ou `models.py`
- Enums comuns: `StatusTransacao`, `BandeiraCartao`, `TipoConta`, `MotivoNegacao`, `RegraFraude`

## Imports
```python
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from confluent_kafka import Producer
```

## Logging
- `logging.getLogger(__name__)` — nunca `print()`
- Lazy formatting: `logger.info("Processed %d transactions", count)` — nunca f-strings em chamadas de log
- Níveis: DEBUG (per-record), INFO (batch summaries), WARNING (degraded state), ERROR (failures)
- Para eventos CDC: sempre incluir `source_system`, `schema`, `table`, `operation` (c/u/d) em `extra`
- Para transações: sempre incluir `transaction_id`, `account_id`, `card_id` em `extra`
- **Nunca** logar PII em texto puro (CPF, PAN completo, nome) — mascarar ou redigir (`***`)
- PAN em log: sempre `BIN + last4` (ex: `424242****4242`)

## Error Handling
- Custom exceptions com contexto — nunca `except:` pelado
- Não engolir exceções que você não pode tratar de forma significativa
- Para erros de Kafka/Flink: log e roteia para DLQ; não silencia
- Para erros de Kafka Connect: surface via REST API status; não suprime

## Performance
- Generators/iterators para grandes volumes — nunca `list()` em streams sem fim
- `@lru_cache` para lookups repetidos (tabela MCC, BIN ranges, lista de UFs)
- Pré-gerar pools de Faker para dados sintéticos (ver `FakerPool` em `shared/generators/`)

## LGPD / Pagamentos
- CPF, CNPJ, nome, endereço, telefone, email — **PII**, taggear na ingestão
- PAN de cartão — **PCI-DSS** restrito; sempre mascarado em topic e em log
- CVV, senha de cartão, dados completos de magnetic stripe — **proibidos** mesmo em sintético
- Dados de localização (lat/lon, cidade da transação) — sensíveis, taggear
- O bootcamp usa exclusivamente dados sintéticos — nenhum dado real, mesmo "anonimizado"
- Pseudonimização preferível à anonimização quando reversibilidade é necessária

## Confluent Cloud / Schema Registry
- Credenciais sempre via env vars (`CC_*`, `SR_*`) — nunca hardcoded; seletor cloud/local via `KAFKA_PROFILE=cloud|local` (ver [.claude/rules.md](../rules.md))
- Schema Registry cloud: basic auth com `basic.auth.user.info=<sr-key>:<sr-secret>`; no fallback local não há auth
- Sempre setar timeouts em chamadas REST do Schema Registry (default: 10s)
- Sempre verificar compatibilidade do schema antes de publicar (`schema_registry_client.test_compatibility(...)`)
- Producer idempotente: `enable.idempotence=True`, `acks=all`
