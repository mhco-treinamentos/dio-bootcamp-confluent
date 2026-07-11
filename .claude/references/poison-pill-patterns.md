# Poison Pill Patterns Reference

> **12 data quality issue types with implementation patterns, injection strategies, and testing approaches for payments data**

## Overview

Real-world data pipelines must handle dirty data. Poison pills are intentionally injected data quality issues that test pipeline robustness. Each type maps to a specific pipeline capability.

**Pedagogical home**: Curso 3, Aula 4 — SMT + Dead Letter Topic (DLQ). The module 3 lab injects these pills through the managed connector pipeline so students watch bad records get routed to the DLQ (with context headers) instead of killing the connector. DLQ configuration details live in [cdc-connect-patterns.md](cdc-connect-patterns.md).

| # | Poison Pill | Tests Pipeline Capability |
|---|-------------|---------------------------|
| 1 | Duplicate events | Deduplication logic |
| 2 | Late-arriving data | Watermarks, windowing |
| 3 | Out-of-order events | Event ordering, resequencing |
| 4 | Schema evolution | Schema Registry compatibility |
| 5 | Null/missing fields | Null handling, coalesce |
| 6 | Malformed data | Input validation, error routing |
| 7 | Orphan records | Referential integrity checks |
| 8 | Extreme values | Boundary validation, outlier detection |
| 9 | Timezone mismatch | Timezone normalization |
| 10 | Retroactive corrections | Idempotency, upsert logic |
| 11 | Tombstone events | Compacted topic handling |
| 12 | Encoding issues | UTF-8, special character handling |

---

## 1. Duplicate Events

**What**: Same record emitted twice with identical `guia_id` and payload.

**Why it happens**: Producer retries (`acks=all` + network timeout), consumer rebalance before offset commit, CDC connector restart.

**Injection rate**: 2% (moderate), 5% (aggressive)

```python
def inject_duplicate(record: T) -> T:
    """Exact copy — same ID, same payload, same timestamp."""
    return copy.deepcopy(record)
```

**Kafka behavior**: Two messages with same key on same partition. Consumer sees both.

**Pipeline test**: Does `SELECT DISTINCT guia_id` or Flink dedup window catch it?

**Variation — Near-duplicate**: Same ID but slightly different timestamp (±1s). Tests whether dedup is ID-based or hash-based.

```python
def inject_near_duplicate(record: T) -> T:
    clone = copy.deepcopy(record)
    clone.timestamp += timedelta(seconds=random.choice([-1, 1]))
    return clone
```

---

## 2. Late-Arriving Data

**What**: Event with `event_time` in the past but arriving at `processing_time` = now.

**Why it happens**: Hospital system offline sync, batch file from prestador delayed, partner system downtime, network partition.

**Injection rate**: 5% (moderate), 10% (aggressive)

```python
LATE_PROFILES = {
    "slightly_late": {"min_hours": 1, "max_hours": 24},
    "day_late": {"min_days": 1, "max_days": 3},
    "very_late": {"min_days": 3, "max_days": 7},
    "extremely_late": {"min_days": 7, "max_days": 30},
}

def inject_late_arriving(record: T, profile: str = "day_late") -> T:
    clone = copy.deepcopy(record)
    p = LATE_PROFILES[profile]
    if "min_hours" in p:
        delay = timedelta(hours=random.randint(p["min_hours"], p["max_hours"]))
    else:
        delay = timedelta(days=random.randint(p["min_days"], p["max_days"]))
    clone.timestamp = clone.timestamp - delay
    return clone
```

**Pipeline test**: Does Flink watermark allow late events? Is allowed_lateness configured? Do late events land in a side output?

---

## 3. Out-of-Order Events

**What**: Event B (logically second) arrives before event A (logically first).

**Why it happens**: Multiple partitions, async producers, microservice fanout, network jitter.

**Injection rate**: 3% (moderate), 8% (aggressive)

```python
def inject_out_of_order(buffer: list[T], window_size: int = 5) -> list[T]:
    """Shuffle events within fixed-size windows."""
    result = []
    for i in range(0, len(buffer), window_size):
        chunk = buffer[i:i + window_size]
        random.shuffle(chunk)
        result.extend(chunk)
    return result
```

**Pipeline test**: Does the pipeline sort by event_time before processing? Does windowed aggregation handle out-of-order correctly?

---

## 4. Schema Evolution (mid-stream)

**What**: New field appears in a record that wasn't in the original schema.

**Why it happens**: Schema Registry version bump, producer upgraded before consumer, backward-compatible addition.

**Injection rate**: One-time event at configurable percentage through the stream

```python
def inject_schema_evolution(record: T, new_fields: dict) -> dict:
    """Add new fields to simulate schema v2."""
    data = asdict(record)
    data.update(new_fields)
    return data

# Example: adding telemedicina flag mid-stream
SCHEMA_V2_FIELDS = {
    "telemedicina": lambda: random.choice([True, False]),
    "canal_solicitacao": lambda: random.choice(["PORTAL", "APP", "TELEFONE"]),
    "_schema_version": lambda: "v2",
}
```

**Pipeline test**: Does consumer handle unknown fields? Does Schema Registry reject or accept? Is BACKWARD compatibility enforced?

---

## 5. Null/Missing Fields

**What**: Required or commonly-present field is null or absent.

**Why it happens**: Partial form submission, API timeout before complete response, ETL bug, source system data quality.

**Injection rate**: 1% (moderate), 3% (aggressive)

```python
# Fields safe to nullify (won't break FK integrity)
NULLABLE_FIELDS = {
    "Guia": ["cid_10", "senha_autorizacao", "valor_executado"],
    "Membro": ["email", "telefone", "complemento"],
    "Prestador": ["crm", "complemento"],
    "Mensalidade": ["dt_pagamento"],
}

def inject_null_field(record: T, entity_type: str) -> T:
    clone = copy.deepcopy(record)
    fields = NULLABLE_FIELDS.get(entity_type, [])
    if fields:
        field = random.choice(fields)
        setattr(clone, field, None)
    return clone
```

**Pipeline test**: Does `COALESCE()` or `IFNULL()` handle missing values? Do NOT NULL constraints catch them?

---

## 6. Malformed Data

**What**: Field value violates expected format or type.

**Why it happens**: Manual data entry, legacy system migration, encoding issues, API misuse.

**Injection rate**: 0.5% (moderate), 2% (aggressive)

```python
MALFORMATIONS = {
    "cpf": [
        "ABC.DEF.GHI-JK",           # letters where digits expected
        "123.456.789-00",            # invalid check digit
        "000.000.000-00",            # all zeros
        "12345678900",               # unformatted (may be valid)
    ],
    "cns": [
        "000000000000000",           # all zeros
        "12345",                      # too short
        "ABCDEFGHIJKLMNO",          # letters
    ],
    "cep": [
        "000",                        # too short
        "ABCDE-FGH",                 # letters
        "99999-999",                  # valid format, nonexistent CEP
    ],
    "email": [
        "not-an-email",
        "@missing-local.com",
        "spaces in@email.com",
        "",                           # empty string
    ],
    "valor": [
        Decimal("-999.99"),           # negative procedure value
        Decimal("0"),                 # zero (might be valid)
        Decimal("99999999.99"),       # exceeds precision
    ],
    "numero_guia": [
        "ABC1234567",                # letters in guide number
        "123",                        # too short
        "",                           # empty
    ],
    "codigo_tuss": [
        "INVALID",                   # non-numeric
        "999",                        # too short
        "00000000",                  # all zeros
    ],
    "phone": [
        "0000000000",                # all zeros
        "+55 (31) ABCD-EFGH",       # letters
        "123",                        # too short
    ],
    "date_str": [
        "2025-13-32",                # impossible date
        "not-a-date",
        "32/01/2025",                # dd/mm/yyyy with invalid day
    ],
}
```

**Pipeline test**: Does input validation catch malformed records? Are they routed to a dead-letter queue?

---

## 7. Orphan Records

**What**: Record references a FK that doesn't exist in the parent table/topic.

**Why it happens**: Race condition between parent/child writes, parent deleted before child processed, eventual consistency.

**Injection rate**: 0.2% (moderate), 1% (aggressive)

```python
def inject_orphan(record: T, fk_field: str) -> T:
    """Replace FK with non-existent reference."""
    clone = copy.deepcopy(record)
    fake_id = f"orphan-{uuid4()}"
    setattr(clone, fk_field, fake_id)
    return clone

# FK fields by entity
FK_FIELDS = {
    "Guia": "membro_id",
    "GuiaItem": "guia_id",
    "Mensalidade": "membro_id",
    "Dependente": "titular_id",
    "Fatura": "prestador_id",
    "Contrato": "prestador_id",
}
```

**Pipeline test**: Does JOIN handle missing parent? Is there a dead-letter queue for orphans?

---

## 8. Extreme Values

**What**: Numeric field at or beyond expected boundaries.

**Why it happens**: System bug, fraud, unit conversion error, missing decimal point.

**Injection rate**: 1% (moderate), 3% (aggressive)

```python
EXTREME_VALUES = {
    "valor_autorizado": [
        Decimal("0.01"),              # minimum
        Decimal("999999.99"),         # R$ 1M procedure
        Decimal("0.001"),             # sub-centavo (precision issue)
        Decimal("500000.00"),         # extremely high single procedure
    ],
    "valor_mensalidade": [
        Decimal("0.01"),              # near-zero premium
        Decimal("50000.00"),          # R$ 50K premium (extreme)
    ],
    "quantidade": [0, 999],           # zero or impossibly many procedures
    "dias_internacao": [0, 365],      # zero or year-long hospitalization
    "idade_beneficiario": [0, 120],   # newborn or impossibly old
}
```

**Pipeline test**: Do range checks catch boundary violations? Does the pipeline handle amounts beyond Decimal precision?

---

## 9. Timezone Mismatch

**What**: Timestamp in wrong timezone (UTC when BRT expected, or vice versa).

**Why it happens**: Server timezone misconfiguration, hospital system sending local time, system migration.

**Injection rate**: 1% (moderate), 3% (aggressive)

```python
def inject_timezone_mismatch(record: T) -> T:
    """Shift timestamp by ±3 hours (UTC↔BRT confusion)."""
    clone = copy.deepcopy(record)
    offset = timedelta(hours=random.choice([-3, 3]))
    clone.timestamp = clone.timestamp + offset
    return clone
```

**Pipeline test**: Is timezone normalization applied? Does `AT TIME ZONE` handle it?

---

## 10. Retroactive Corrections (Estornos / Glosas)

**What**: Corrected version of a previously processed record arrives.

**Why it happens**: Glosa (claim denial), recurso de glosa (appeal), auditoria médica correction, reconciliation adjustment.

**Injection rate**: 1-3% of guias

```python
def inject_correction(record: T) -> T:
    """Emit corrected version of previously sent record."""
    clone = copy.deepcopy(record)
    # Partial correction (e.g., valor_autorizado reduced by glosa)
    clone.valor_autorizado = clone.valor_autorizado * Decimal(str(random.uniform(0.5, 0.95)))
    clone.status = "GLOSADA"
    clone.updated_at = datetime.now()
    return clone

def inject_glosa_parcial(record: T) -> T:
    """Emit partial glosa — reduce executed value."""
    clone = copy.deepcopy(record)
    clone.valor_glosa = clone.valor_executado * Decimal(str(random.uniform(0.10, 0.40)))
    clone.valor_executado = clone.valor_executado - clone.valor_glosa
    clone.status = "GLOSADA"
    clone.updated_at = datetime.now()
    return clone
```

**Pipeline test**: Does upsert logic use `updated_at` for conflict resolution? Is idempotency maintained?

---

## 11. Tombstone Events

**What**: Message with key but null value (Kafka compacted topic convention).

**Why it happens**: Member cancellation, LGPD deletion request (direito ao esquecimento), entity end-of-life.

**Injection rate**: 0.5% (moderate), 2% (aggressive)

```python
def inject_tombstone(record: T) -> dict:
    """Null-payload message for compacted topic deletion."""
    key = extract_key(record)  # e.g., membro_id
    return {
        "key": key,
        "value": None,
        "headers": {
            "ce_type": "membro.cancelado",
            "ce_source": "data-gen",
            "ce_reason": random.choice([
                "VOLUNTARIO", "INADIMPLENCIA", "OBITO",
                "PORTABILIDADE", "LGPD_EXCLUSAO",
            ]),
        },
    }
```

**Pipeline test**: Does consumer handle null values without NPE? Does log compaction properly remove tombstoned records?

---

## 12. Encoding Issues

**What**: Names or text fields with characters that break naive ASCII handling.

**Why it happens**: Brazilian names with accents, immigrant names, special characters in addresses.

**Injection rate**: 5-10% (this is realistic for Brazilian data, not really a "poison pill")

```python
BRAZILIAN_SPECIAL_NAMES = [
    "José Conceição Müller",
    "María García-López",
    "Renée Françoise",
    "João São Paulo d'Ávila",
    "Lúcia Assunção",
    "André Gonçalves Nóbrega",
    "Inês Canção",
    "Tomé Araújo Côrrea",
]

SPECIAL_ADDRESSES = [
    "Rua São João d'El Rei, 1000",
    "Av. Cônego Antônio Müller",
    "Praça Marechal Floriano Peixoto — Centro",
    "Travessa José Bonifácio Nº 42",
]
```

**Pipeline test**: Is UTF-8 encoding consistent end-to-end? Do LIKE queries handle accented characters?

---

## Configuration Summary

```python
@dataclass
class PoisonPillConfig:
    """Master configuration for all poison pill types."""

    enabled: bool = False
    # Per-type injection rates (probability per record)
    duplicate_rate: float = 0.02
    late_arriving_rate: float = 0.05
    out_of_order_rate: float = 0.03
    schema_evolution_pct: float = 0.50   # % through stream where schema changes
    null_fields_rate: float = 0.01
    malformed_rate: float = 0.005
    orphan_rate: float = 0.002
    extreme_values_rate: float = 0.01
    timezone_mismatch_rate: float = 0.01
    correction_rate: float = 0.02
    tombstone_rate: float = 0.005
    encoding_issue_rate: float = 0.05

    # Audit
    track_injections: bool = True        # log every injection for validation
```

## Testing Strategy

```python
def test_injection_rates(scenario, config, tolerance=0.3):
    """Verify injection rates are within tolerance of configured rates."""
    labels = scenario.get_labels()["poison_pills"]
    total = len(scenario.store.get_all_guias())

    for pill_type, expected_rate in [
        ("DUPLICATE", config.duplicate_rate),
        ("LATE_ARRIVING", config.late_arriving_rate),
        # ... all types
    ]:
        actual_count = sum(1 for v in labels.values() if v == pill_type)
        actual_rate = actual_count / total
        assert abs(actual_rate - expected_rate) < expected_rate * tolerance, (
            f"{pill_type}: expected ~{expected_rate:.3f}, got {actual_rate:.3f}"
        )
```
