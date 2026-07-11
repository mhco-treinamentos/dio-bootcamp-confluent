# Skill: Generator Builder

> **Build Python data generators using Faker and custom logic**

## Purpose

Create Python generator classes that produce:
- Realistic synthetic data
- Deterministic output (with seeds)
- Domain-specific patterns
- Configurable volumes

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `entity` | Yes | Entity to generate (guia, membro, prestador, plano) |
| `domain` | Yes | Domain (saude, financeiro, clinico) |
| `schema` | No | Avro schema to conform to |
| `patterns` | No | Specific patterns (distributions, frequencies) |

## Process

1. **Analyze Entity**: Understand fields and relationships
2. **Design Generator**: Create class structure
3. **Implement Fields**: Use Faker providers and custom logic
4. **Add Patterns**: Statistical distributions, temporal patterns
5. **Test**: Verify output validity and determinism

## Output Example

```python
"""Guia TISS generator for synthetic payments data."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator
import random

from faker import Faker


@dataclass
class GuiaGeneratorConfig:
    """Configuration for guia TISS generator."""

    seed: int | None = None
    locale: str = "pt_BR"
    min_valor: float = 50.00
    max_valor: float = 100000.00
    business_hours_weight: float = 0.8


class GuiaGenerator:
    """Generate synthetic guia TISS records."""

    TIPOS_GUIA = ["CONSULTA", "SP_SADT", "INTERNACAO", "ODONTOLOGICA", "HONORARIOS"]
    TIPO_WEIGHTS = [0.40, 0.30, 0.10, 0.10, 0.10]

    def __init__(self, config: GuiaGeneratorConfig | None = None) -> None:
        self.config = config or GuiaGeneratorConfig()
        self.fake = Faker(self.config.locale)
        if self.config.seed:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)

    def generate(self, count: int) -> Iterator[dict]:
        """Generate synthetic guia TISS records.

        Parameters
        ----------
        count : int
            Number of guias to generate.

        Yields
        ------
        dict
            Guia TISS record.
        """
        for _ in range(count):
            yield self._generate_guia()

    def _generate_guia(self) -> dict:
        """Generate a single guia TISS."""
        tipo_guia = random.choices(self.TIPOS_GUIA, weights=self.TIPO_WEIGHTS, k=1)[0]
        return {
            "guia_id": self.fake.uuid4(),
            "numero_guia": self.fake.numerify("################"),
            "tipo_guia": tipo_guia,
            "valor_autorizado": self._generate_valor(tipo_guia),
            "membro_id": self.fake.uuid4(),
            "prestador_id": self.fake.uuid4(),
            "data_solicitacao": self._generate_timestamp(),
            "status": random.choice(
                ["SOLICITADA", "EM_ANALISE", "AUTORIZADA", "EXECUTADA"]
            ),
        }

    def _generate_valor(self, tipo_guia: str) -> float:
        """Generate valor based on tipo_guia with Pareto distribution."""
        base_multiplier = {
            "CONSULTA": 1.0,
            "SP_SADT": 3.0,
            "INTERNACAO": 20.0,
            "ODONTOLOGICA": 1.5,
            "HONORARIOS": 5.0,
        }
        amount = random.paretovariate(1.5) * 50 * base_multiplier.get(tipo_guia, 1.0)
        return round(
            min(max(amount, self.config.min_valor), self.config.max_valor),
            2,
        )

    def _generate_timestamp(self) -> str:
        """Generate timestamp weighted toward business hours."""
        base = datetime.now() - timedelta(days=random.randint(0, 30))

        if random.random() < self.config.business_hours_weight:
            # Business hours: 7am - 6pm
            hour = random.randint(7, 17)
        else:
            hour = random.randint(0, 23)

        return base.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        ).isoformat()
```

## Example Usage

```
"Create a guia TISS generator with Pareto-distributed values"
"Build a membro generator with age-appropriate planos"
"Generate a prestador generator with speciality distribution"
"Create a glosa generator with motivo patterns"
```

---

## Postgres-Direct Simulator Pattern (Labs de CDC — módulo 3)

Existem dois padrões de gerador, e a escolha afeta tudo downstream:

| Pattern | Where data lands first | When to use |
|---------|------------------------|-------------|
| **Kafka-direct** (default above) | Kafka topic via `confluent_kafka.Producer` | Eventos que nascem como mensagem (labs dos módulos 2 e 4; testes de stream sintético) |
| **Postgres-direct** | Tabela Postgres via DML (`psycopg`); o conector Debezium gerenciado leva ao Kafka | Simular o sistema operacional onde o CDC é a fonte da verdade (módulo 3 e desafio final) |

### Racional do Postgres-direct
O lab do módulo 3 e o desafio final usam **Postgres 16 (Docker, `wal_level=logical`) → Debezium managed → Confluent Cloud**. Gerar dados **dentro do Postgres** exercita o pipeline real — snapshot, replicação lógica, envelope Debezium, SMTs e DLQ. Gerar direto no Kafka pula tudo isso.

### Dependencies
```txt
# requirements.txt
psycopg[binary]==3.1.*
```

### Connection Helper
```python
# db.py
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

import psycopg

logger = logging.getLogger(__name__)


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    """Connect to the lab Postgres (CDC source). Reads connection info from env vars."""
    conn = psycopg.connect(
        host=os.environ["PG_HOST"],
        port=os.environ.get("PG_PORT", "5432"),
        dbname=os.environ["PG_DBNAME"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

### Simulator Pattern
```python
# simulate.py
from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

from db import connect

logger = logging.getLogger(__name__)


def create_transaction(account_id: str, card_id: str, amount: Decimal, mcc: str) -> int:
    """Create a payment transaction: INSERT transaction, UPDATE account balance.

    Returns the new transaction_id (captured by the Debezium connector).
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO transactions (account_id, card_id, amount, mcc, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING transaction_id
            """,
            (account_id, card_id, amount, mcc, datetime.now()),
        )
        transaction_id = int(cur.fetchone()[0])
        cur.execute(
            "UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
            (amount, account_id),
        )
        logger.info("Transaction %d: account %s, BRL %s", transaction_id, account_id, amount)
        return transaction_id
```

### Key Rules for Postgres-Direct Simulators

1. **Use transactions** — `with connect() as conn:` commits on exit, rolls back on exception. Never leave half-states that break downstream CDC consistency.
2. **Every CDC'd table needs a stable PK** and must be included in the Debezium publication (`CREATE PUBLICATION ... FOR TABLE ...` — see [skills/confluent/managed-connector.md](../confluent/managed-connector.md)).
3. **Use bind parameters** — `%s` placeholders. Never f-string values into SQL (see [rules/sql.md](../../rules/sql.md)).
4. **Seed for determinism** — `random.seed(42)` at the top of the simulator so every aluno sees identical data.
5. **Orchestrate per lab** — wrap the simulator in a script with named steps per lab do módulo (setup → generate → verify → teardown). See [rules/labs.md](../../rules/labs.md).

### When to Prefer Kafka-Direct

- Labs de Flink SQL independentes do CDC (módulo 4)
- Stress test de throughput Kafka
- Eventos externos que não passam pelo banco (webhooks, apps)

Nos labs de CDC (módulo 3) e no desafio final, Postgres-direct exercita mais do pipeline real.

### Example Usage (Postgres-Direct)

```
"Build a Postgres-direct simulator for payment transactions using psycopg"
"Create the lab runner do módulo 3 com setup/generate/verify/teardown"
"Extend the transaction simulator to also update card limits (multi-table CDC)"
```
