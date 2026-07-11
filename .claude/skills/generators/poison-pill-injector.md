# Skill: Poison Pill Injector

> **Inject configurable data quality issues into synthetic data streams**

## Purpose

Create and configure poison pill generators that inject realistic data quality problems into clean data streams. Used for testing pipeline robustness, deduplication logic, schema evolution handling, and data validation frameworks.

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `entity` | Yes | Entity to inject poison pills into (guia, membro, etc.) |
| `poison_types` | No | Specific types to inject (default: all enabled in config) |
| `config` | No | PoisonPillConfig with rates per type |
| `audit` | No | Whether to generate injection audit trail (default: true) |

## Process

1. **Select Target Entity**: Identify the clean data stream to wrap
2. **Configure Rates**: Set injection probability per poison type
3. **Implement Injectors**: Create type-specific injection functions
4. **Wrap Stream**: Apply decorator pattern over clean generator
5. **Audit Trail**: Log every injected issue with type + record_id
6. **Test**: Verify injection rates match config within tolerance

## Poison Pill Types

### 1. Duplicate Events
```python
def _duplicate(self, record: T) -> T:
    """Send exact same record twice (same ID, same payload)."""
    self._log_injection(record, "DUPLICATE")
    return copy.deepcopy(record)
```

### 2. Late-Arriving Data
```python
def _make_late(self, record: T, max_days_late: int = 7) -> T:
    """Shift timestamp to past but emit now (event_time << processing_time)."""
    clone = copy.deepcopy(record)
    days_late = random.randint(1, max_days_late)
    clone.timestamp = clone.timestamp - timedelta(days=days_late)
    self._log_injection(clone, "LATE_ARRIVING", extra={"days_late": days_late})
    return clone
```

### 3. Out-of-Order Events
```python
def _reorder(self, buffer: list[T], window: int = 5) -> list[T]:
    """Shuffle events within a window to break temporal ordering."""
    # Collect window-sized chunks and shuffle within each
    for i in range(0, len(buffer), window):
        chunk = buffer[i:i + window]
        random.shuffle(chunk)
        buffer[i:i + window] = chunk
    return buffer
```

### 4. Null/Missing Fields
```python
def _nullify_fields(self, record: T, nullable_fields: list[str]) -> T:
    """Set random optional fields to None."""
    clone = copy.deepcopy(record)
    field = random.choice(nullable_fields)
    setattr(clone, field, None)
    self._log_injection(clone, "NULL_FIELD", extra={"field": field})
    return clone
```

### 5. Malformed Data
```python
MALFORMATIONS = {
    "cpf": lambda: "ABC.DEF.GHI-JK",           # letters in CPF
    "cns": lambda: "000",                        # invalid CNS
    "cep": lambda: "000",                        # invalid CEP
    "valor_autorizado": lambda: Decimal("-999.99"),  # negative valor
    "email": lambda: "not-an-email",             # invalid email
    "data_solicitacao": lambda: "2025-13-32",    # impossible date
}

def _malform(self, record: T) -> T:
    """Replace a field with malformed data."""
    clone = copy.deepcopy(record)
    field, generator = random.choice(list(self.MALFORMATIONS.items()))
    if hasattr(clone, field):
        setattr(clone, field, generator())
        self._log_injection(clone, "MALFORMED", extra={"field": field})
    return clone
```

### 6. Orphan Records
```python
def _orphan(self, record: T, fk_field: str) -> T:
    """Replace FK with non-existent reference."""
    clone = copy.deepcopy(record)
    setattr(clone, fk_field, f"orphan-{uuid4()}")
    self._log_injection(clone, "ORPHAN", extra={"fk_field": fk_field})
    return clone
```

### 7. Extreme Values
```python
EXTREMES = {
    "valor_autorizado": [Decimal("0.01"), Decimal("9999999.99")],
    "quantidade": [0, 999999],
    "idade": [0, 150],
}

def _extreme_value(self, record: T) -> T:
    """Replace numeric field with boundary value."""
    clone = copy.deepcopy(record)
    field = random.choice([f for f in self.EXTREMES if hasattr(clone, f)])
    value = random.choice(self.EXTREMES[field])
    setattr(clone, field, value)
    self._log_injection(clone, "EXTREME_VALUE", extra={"field": field, "value": str(value)})
    return clone
```

### 8. Timezone Mismatch
```python
def _timezone_mismatch(self, record: T) -> T:
    """Emit timestamp in wrong timezone (UTC vs BRT offset)."""
    clone = copy.deepcopy(record)
    # Shift by -3h (BRT->UTC confusion) or +3h (UTC->BRT confusion)
    offset = timedelta(hours=random.choice([-3, 3]))
    clone.timestamp = clone.timestamp + offset
    self._log_injection(clone, "TIMEZONE_MISMATCH", extra={"offset_hours": offset.total_seconds() / 3600})
    return clone
```

### 9. Tombstone Events
```python
def _tombstone(self, record: T) -> dict:
    """Emit null-payload event (for compacted topics)."""
    key = getattr(record, "membro_id", getattr(record, "guia_id", None))
    self._log_injection(record, "TOMBSTONE")
    return {"key": key, "value": None}  # Kafka tombstone
```

### 10. Encoding Issues
```python
SPECIAL_NAMES = [
    "José Conceição Müller",
    "María García-López",
    "Renée Françoise",
    "Łukasz Wójcik",
    "São João d'El Rei",
]

def _encoding_issue(self, record: T) -> T:
    """Inject names with special characters that break naive encodings."""
    clone = copy.deepcopy(record)
    if hasattr(clone, "nome"):
        clone.nome = random.choice(self.SPECIAL_NAMES)
    self._log_injection(clone, "ENCODING_ISSUE")
    return clone
```

### 11. Schema Evolution (mid-stream)
```python
def _schema_evolution(self, record: T) -> dict:
    """Add unexpected fields that weren't in the original schema."""
    data = asdict(record)
    data["cid_10_secundario"] = "Z00.0"  # new field
    data["_schema_version"] = "v2"
    self._log_injection(record, "SCHEMA_EVOLUTION")
    return data
```

### 12. Retroactive Corrections
```python
def _retroactive_correction(self, record: T) -> T:
    """Emit corrected version of a previously processed record."""
    clone = copy.deepcopy(record)
    # Modify valor slightly (estorno parcial)
    if hasattr(clone, "valor_autorizado"):
        clone.valor_autorizado = clone.valor_autorizado * Decimal("0.85")  # partial refund
    clone.status = "CORRIGIDA"
    self._log_injection(clone, "RETROACTIVE_CORRECTION")
    return clone
```

## Configuration Presets

```python
# Preset: Light (for development)
LIGHT = PoisonPillConfig(
    enabled=True,
    duplicate_rate=0.01,
    late_arriving_rate=0.02,
    out_of_order_rate=0.01,
    null_fields_rate=0.005,
    malformed_rate=0.002,
    orphan_rate=0.001,
    extreme_values_rate=0.005,
    timezone_mismatch_rate=0.005,
    tombstone_rate=0.002,
)

# Preset: Moderate (for QA/staging)
MODERATE = PoisonPillConfig(
    enabled=True,
    duplicate_rate=0.02,
    late_arriving_rate=0.05,
    out_of_order_rate=0.03,
    null_fields_rate=0.01,
    malformed_rate=0.005,
    orphan_rate=0.002,
    extreme_values_rate=0.01,
    timezone_mismatch_rate=0.01,
    tombstone_rate=0.005,
)

# Preset: Aggressive (for stress testing)
AGGRESSIVE = PoisonPillConfig(
    enabled=True,
    duplicate_rate=0.05,
    late_arriving_rate=0.10,
    out_of_order_rate=0.08,
    null_fields_rate=0.03,
    malformed_rate=0.02,
    orphan_rate=0.01,
    extreme_values_rate=0.03,
    timezone_mismatch_rate=0.03,
    tombstone_rate=0.02,
)
```

## Example Usage

```
"Use poison-pill-injector to add duplicate and late-arriving events to guias"
"Configure aggressive poison pills for stress-testing the Kafka pipeline"
"Inject orphan records into guia data to test FK validation"
"Create a DataQualityScenario with all 12 poison types at moderate rates"
```
