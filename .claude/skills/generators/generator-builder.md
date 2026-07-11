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
| `entity` | Yes | Entity to generate (transaction, account, card, merchant, refund) |
| `domain` | Yes | Domain (payments — default do bootcamp) |
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
"""Transaction generator for synthetic payments data."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator
import random

from faker import Faker


@dataclass
class TransactionGeneratorConfig:
    """Configuration for transaction generator."""

    seed: int | None = None
    locale: str = "pt_BR"
    min_amount: Decimal = Decimal("1.00")
    max_amount: Decimal = Decimal("10000.00")
    bimodal_hours_weight: float = 0.7  # picos 11-13h e 18-21h


class TransactionGenerator:
    """Generate synthetic payment transaction records."""

    CHANNELS = ["PIX", "CREDIT", "DEBIT", "BOLETO"]
    CHANNEL_WEIGHTS = [0.45, 0.25, 0.20, 0.10]

    def __init__(self, config: TransactionGeneratorConfig | None = None) -> None:
        self.config = config or TransactionGeneratorConfig()
        self.fake = Faker(self.config.locale)
        if self.config.seed:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)

    def generate(self, count: int) -> Iterator[dict]:
        """Generate synthetic transaction records.

        Parameters
        ----------
        count : int
            Number of transactions to generate.

        Yields
        ------
        dict
            Transaction record.
        """
        for _ in range(count):
            yield self._generate_transaction()

    def _generate_transaction(self) -> dict:
        """Generate a single transaction."""
        channel = random.choices(self.CHANNELS, weights=self.CHANNEL_WEIGHTS, k=1)[0]
        return {
            "transaction_id": self.fake.uuid4(),
            "account_id": self.fake.uuid4(),
            "card_id": self.fake.uuid4() if channel in ("CREDIT", "DEBIT") else None,
            "channel": channel,
            "amount": str(self._generate_amount(channel)),
            "merchant_mcc": self.fake.numerify("####"),
            "uf_transacao": self.fake.estado_sigla(),
            "created_at": self._generate_timestamp(),
            "status": random.choices(
                ["SETTLED", "AUTHORIZED", "DECLINED"], weights=[0.88, 0.07, 0.05], k=1
            )[0],
        }

    def _generate_amount(self, channel: str) -> Decimal:
        """Generate amount with log-normal distribution (median ~R$80, long tail)."""
        channel_multiplier = {
            "PIX": 1.0,
            "CREDIT": 1.5,
            "DEBIT": 0.7,
            "BOLETO": 3.0,
        }
        amount = random.lognormvariate(4.4, 1.1) * channel_multiplier.get(channel, 1.0)
        clamped = min(max(Decimal(str(round(amount, 2))), self.config.min_amount), self.config.max_amount)
        return clamped.quantize(Decimal("0.01"))

    def _generate_timestamp(self) -> str:
        """Generate timestamp with bimodal daily pattern (11-13h, 18-21h BRT)."""
        base = datetime.now() - timedelta(days=random.randint(0, 30))

        if random.random() < self.config.bimodal_hours_weight:
            hour = random.choice([11, 12, 13, 18, 19, 20, 21])
        else:
            hour = random.randint(6, 23)

        return base.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        ).isoformat()
```

## Example Usage

```
"Create a transaction generator with log-normal amounts and bimodal hours"
"Build an account generator with tier-appropriate credit limits"
"Generate a merchant generator with top-10 MCC concentration"
"Create a refund generator with reason patterns (FRAUD 40%, NOT_RECOGNIZED 30%)"
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
