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

## Oracle-Direct Simulator Pattern (Workshop & CDC-First Pipelines)

Two distinct generator patterns exist, and the choice affects everything downstream:

| Pattern | Where data lands first | When to use |
|---------|------------------------|-------------|
| **Kafka-direct** (default above) | Kafka topic via `confluent_kafka.Producer` | Simulating events that originate as messages (IoT, app events, synthetic stream tests) |
| **Oracle-direct** | Oracle table via `python-oracledb` DML; CDC carries it to Kafka | Simulating an operational system where CDC is the source of truth (workshop stack, most prod scenarios) |

### Oracle-Direct Rationale
The workshop (`ws-fundamentals-kafka-cdc/samples/Hospital_DataGen_v2.md`) and the the workshop production platform both use Oracle → Debezium → Kafka. Generating data **into Oracle** exercises the real pipeline — you see CDC lag, LogMiner behavior, schema mapping, and SMT effects. Generating directly to Kafka bypasses all of that.

### Dependencies
```txt
# requirements.txt
python-oracledb==2.0.1   # Official successor to cx_Oracle since 2022
```

`python-oracledb` in **thin mode** (default) needs no Oracle Instant Client. All DML/DDL operations work out of the box.

### Connection Helper
```python
# db.py
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

import oracledb

logger = logging.getLogger(__name__)


@contextmanager
def connect() -> Iterator[oracledb.Connection]:
    """Connect to Oracle XE (thin mode). Reads connection info from env vars."""
    conn = oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASS"],
        dsn=f"{os.environ['ORACLE_HOST']}:{os.environ['ORACLE_PORT']}/{os.environ['ORACLE_SERVICE']}",
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
import random
from datetime import datetime

from db import connect

logger = logging.getLogger(__name__)


def admit_patient(paciente_id: int, leito_id: int, cid: str) -> int:
    """Admit a patient: INSERT internacao, UPDATE leito status.

    Returns the new internacao_id.
    """
    with connect() as conn:
        cur = conn.cursor()
        internacao_id_var = cur.var(oracledb.NUMBER)
        cur.execute(
            """
            INSERT INTO internacoes (paciente_id, leito_id, cid, data_entrada)
            VALUES (:p, :l, :c, :d)
            RETURNING id INTO :id_out
            """,
            p=paciente_id,
            l=leito_id,
            c=cid,
            d=datetime.now(),
            id_out=internacao_id_var,
        )
        cur.execute(
            "UPDATE leitos SET status = 'OCUPADO' WHERE id = :l",
            l=leito_id,
        )
        internacao_id = int(internacao_id_var.getvalue()[0])
        logger.info("Admitted paciente %d → leito %d (CID %s)", paciente_id, leito_id, cid)
        return internacao_id
```

### Key Rules for Oracle-Direct Simulators

1. **Use transactions** — `with connect() as conn:` commits on exit, rolls back on exception. Never leave half-states that break downstream CDC consistency.
2. **Respect supplemental logging requirements** — every CDC'd table needs a stable PK and `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`. See [rules/sql.md](../../rules/sql.md).
3. **Use bind parameters** — `:p`, `:l`, etc. Never f-string user inputs into SQL (this is a rule anyway — see [rules/sql.md](../../rules/sql.md)).
4. **Seed for determinism** — `random.seed(42)` at the top of the simulator so every attendee of a workshop sees identical data.
5. **Orchestrate with a scene runner** — wrap the simulator in a bash script (`run_demo.sh`) that exposes named scenes (`cena1`, `cena2`, `cena3`) with a single verb per scene. See [rules/workshop.md](../../rules/workshop.md) for scene design.

### When to Prefer Kafka-Direct

- Testing Flink SQL independently of Oracle + Debezium
- Stress-testing Kafka throughput
- Simulating external event sources (webhooks, IoT) that do not land in Oracle

Otherwise, Oracle-direct is closer to production and exercises more of the real pipeline.

### Example Usage (Oracle-Direct)

```
"Build an Oracle-direct simulator for hospital admissions using python-oracledb"
"Create a run_demo.sh scene runner with cena1/cena2/cena3 scenes for the workshop"
"Extend the admission simulator to also generate exames TISS records"
```
