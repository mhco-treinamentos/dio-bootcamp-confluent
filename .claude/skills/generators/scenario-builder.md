# Skill: Scenario Builder

> **Build end-to-end data generation scenarios with entities, behaviors, and sinks**

## Purpose

Create complete scenario classes that orchestrate multiple generators, behavioral patterns, and poison pills into coherent datasets. Scenarios are the top-level entry point for data generation for the **labs por módulo** do bootcamp (e para as camadas do desafio final) — they define **what** gets generated, in **what order**, with **what behaviors**, and to **which sinks** (Kafka via profile cloud|local, Postgres CDC source, JSON).

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `scenario_name` | Yes | Name of the scenario (e.g., DataQuality, RealisticPayments, FraudDetection) |
| `entities` | Yes | List of entities to generate |
| `patterns` | No | Behavioral patterns to apply (seasonal, fraud bursts, refunds, etc.) |
| `poison_pills` | No | PoisonPillConfig for data quality injection |
| `labels` | No | Ground truth labels to generate (fraud rule triggered, approval outcome) |
| `sinks` | No | Target sinks (kafka, postgres, json, console) |

## Process

1. **Map Dependencies**: Build entity dependency graph (L0 -> L1 -> L2)
2. **Configure Generators**: Instantiate generators with seed and config
3. **Define Volume Ratios**: Set realistic entity counts per account (Pareto)
4. **Compose Patterns**: Layer behavioral patterns on base generators
5. **Add Poison Pills**: Wrap streams with configurable injection
6. **Generate Labels**: Produce ground truth alongside data
7. **Wire Sinks**: Route entities to appropriate outputs
8. **Test**: Verify referential integrity, volumes, and labels

## Output Template

```python
"""Scenario: {ScenarioName} — {description}."""

from dataclasses import dataclass, field
from typing import Any

from data_gen.generators.payments import (
    AccountGenerator,
    CardGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from data_gen.generators.payments.patterns import FraudBurstBehavior
from data_gen.store.payments import PaymentsDataStore


@dataclass
class {ScenarioName}Config:
    """Configuration for {ScenarioName} scenario.

    Parameters
    ----------
    num_accounts : int
        Number of accounts to generate.
    seed : int | None
        Random seed for reproducibility.
    {pattern_params}
    """

    num_accounts: int = 1000
    seed: int | None = None
    # Entity penetration rates
    cards_per_account: tuple[int, int] = (1, 3)
    merchant_pool_size: int = 200
    txn_per_account_month: tuple[int, int] = (0, 150)  # Pareto-distributed
    # Pattern toggles
    enable_bimodal_hours: bool = True
    enable_seasonal_volume: bool = True
    enable_fraud_bursts: bool = True
    enable_refunds: bool = True
    # Poison pills
    poison_pill_config: PoisonPillConfig | None = None


class {ScenarioName}Scenario:
    """Generate {description}.

    Orchestrates generators in dependency order:
    L0: Customer, Merchant
    L1: Account, Card
    L2: Transaction
    L3: Refund, FraudAlert (derived events)
    """

    def __init__(self, config: {ScenarioName}Config | None = None) -> None:
        self.config = config or {ScenarioName}Config()
        self.store = PaymentsDataStore()
        self._labels: dict[str, dict] = {}
        self._init_generators()

    def _init_generators(self) -> None:
        """Initialize all generators with shared seed."""
        seed = self.config.seed
        self.account_gen = AccountGenerator(seed=seed)
        self.card_gen = CardGenerator(seed=seed)
        self.merchant_gen = MerchantGenerator(seed=seed)
        self.txn_gen = TransactionGenerator(seed=seed)
        # ... other generators

    def generate(self) -> PaymentsDataStore:
        """Execute scenario in dependency order.

        Returns
        -------
        PaymentsDataStore
            Store with all generated entities and relationships.
        """
        # Level 0 — no FK dependencies
        self._generate_customers()
        self._generate_merchants()

        # Level 1 — FK to Customer
        self._generate_accounts()
        self._generate_cards()

        # Level 2 — FK to Account/Card/Merchant
        self._generate_transactions()

        # Behavioral patterns
        if self.config.enable_bimodal_hours:
            self._apply_bimodal_hours()
        if self.config.enable_seasonal_volume:
            self._apply_seasonal_volume()
        if self.config.enable_fraud_bursts:
            self._apply_fraud_bursts()
        if self.config.enable_refunds:
            self._apply_refunds()

        # Poison pills
        if self.config.poison_pill_config:
            self._inject_poison_pills()

        return self.store

    def get_labels(self) -> dict[str, dict]:
        """Return ground truth labels for ML training (modulo 6)."""
        return self._labels

    def get_summary(self) -> dict[str, Any]:
        """Return scenario statistics."""
        return {
            "accounts": len(self.store.accounts),
            "merchants": len(self.store.merchants),
            "transactions": sum(
                len(txns) for txns in self.store._account_transactions.values()
            ),
            "labels": {k: len(v) for k, v in self._labels.items()},
        }

    # --- Private generation methods ---

    def _generate_accounts(self) -> None:
        """Generate Level 1: Accounts (FK -> Customer)."""
        for customer in self.store.customers.values():
            account = self.account_gen.generate_for_customer(customer)
            self.store.add_account(account)

    def _generate_cards(self) -> None:
        """Generate Level 1: Cards (FK -> Account)."""
        for account in self.store.accounts.values():
            for _ in range(self._cards_count()):
                card = self.card_gen.generate_for_account(account)
                self.store.add_card(card)

    # ... other _generate methods following same pattern
```

## Scenario Patterns

### Pattern: Composition over Inheritance

```python
# DO: Compose patterns as independent, injectable classes
class RealisticPaymentsScenario:
    def __init__(self, config):
        self.patterns = [
            BimodalHoursPattern(config.hours),
            SeasonalVolumePattern(config.seasonality),
            FraudBurstPattern(config.fraud),
            RefundPattern(config.refunds),
        ]

    def _apply_patterns(self):
        for pattern in self.patterns:
            pattern.apply(self.store)

# DON'T: Giant method with all logic inlined
```

### Pattern: Label Tracking

```python
# Track labels during generation, not after
def _apply_fraud_bursts(self):
    for txn in fraud_generator.generate():
        self.store.add_transaction(txn)
        self._labels.setdefault("fraud", {})[txn.transaction_id] = txn.fraud_rule
```

### Pattern: Volume Calibration

```python
# Use realistic multipliers, not magic numbers (see rules.md — Domain Rules)
VOLUME_RATIOS = {
    "txn_per_account_month_heavy": (40, 150),   # 20% accounts → 80% volume
    "txn_per_account_month_light": (0, 5),
    "cards_per_account": (1, 3),
    "fraud_rate": (0.005, 0.015),
    "refund_rate": 0.02,
    "approval_rate": (0.92, 0.97),
}
```

## Checklist for New Scenarios

- [ ] Dependency graph documented
- [ ] Generators initialized with shared seed
- [ ] Generation follows L0 -> L1 -> L2 -> patterns -> poison order
- [ ] Config dataclass with all parameters and defaults
- [ ] `get_labels()` returns ground truth
- [ ] `get_summary()` returns statistics
- [ ] Referential integrity maintained via PaymentsDataStore
- [ ] Tests: determinism, volume, FK integrity, label accuracy
- [ ] 80%+ test coverage

## Example Usage

```
"Use scenario-builder to create a DataQualityScenario with all 12 poison types para o lab de DLQ do módulo 3"
"Build a RealisticPaymentsScenario with bimodal transaction hours and Pareto accounts"
"Create a FraudDetectionScenario with velocity and geo-impossible patterns para o lab do módulo 4"
"Design a scenario for testing Kafka deduplication with duplicate poison pills"
```

---

**Última atualização**: 2026-07-11
