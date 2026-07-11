# Skill: Scenario Builder

> **Build end-to-end data generation scenarios with entities, behaviors, and sinks**

## Purpose

Create complete scenario classes that orchestrate multiple generators, behavioral patterns, and poison pills into coherent datasets. Scenarios are the top-level entry point for data generation for the **labs por módulo** do bootcamp (e para as camadas do desafio final) — they define **what** gets generated, in **what order**, with **what behaviors**, and to **which sinks** (Kafka via profile cloud|local, Postgres CDC source, JSON).

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `scenario_name` | Yes | Name of the scenario (e.g., DataQuality, RealisticPayments, FraudDetection) |
| `entities` | Yes | List of entities to generate |
| `patterns` | No | Behavioral patterns to apply (recurring, seasonal, glosa, etc.) |
| `poison_pills` | No | PoisonPillConfig for data quality injection |
| `labels` | No | Ground truth labels to generate (fraud rule triggered, approval outcome) |
| `sinks` | No | Target sinks (kafka, postgres, json, console) |

## Process

1. **Map Dependencies**: Build entity dependency graph (L0 -> L1 -> L2)
2. **Configure Generators**: Instantiate generators with seed and config
3. **Define Volume Ratios**: Set realistic entity counts per membro
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

from data_gen.generators.saude import (
    GuiaGenerator,
    MembroGenerator,
    PrestadorGenerator,
)
from data_gen.generators.saude.patterns import AutorizacaoBehavior
from data_gen.store.saude import SaudeDataStore


@dataclass
class {ScenarioName}Config:
    """Configuration for {ScenarioName} scenario.

    Parameters
    ----------
    num_membros : int
        Number of membros/beneficiarios to generate.
    seed : int | None
        Random seed for reproducibility.
    {pattern_params}
    """

    num_membros: int = 1000
    seed: int | None = None
    # Entity penetration rates
    planos_per_membro: int = 1
    dependentes_penetration: float = 0.40
    guia_penetration: float = 0.60
    # Pattern toggles
    enable_recurring_consultas: bool = True
    enable_seasonal_internacoes: bool = True
    enable_glosa_patterns: bool = True
    enable_coparticipacao: bool = True
    # Poison pills
    poison_pill_config: PoisonPillConfig | None = None


class {ScenarioName}Scenario:
    """Generate {description}.

    Orchestrates generators in dependency order:
    L0: Membro, Prestador
    L1: Plano, Dependente, Contrato, Credenciamento
    L2: Guia, Autorizacao, Mensalidade
    L3: Glosa, Coparticipacao (derived events)
    """

    def __init__(self, config: {ScenarioName}Config | None = None) -> None:
        self.config = config or {ScenarioName}Config()
        self.store = SaudeDataStore()
        self._labels: dict[str, dict] = {}
        self._init_generators()

    def _init_generators(self) -> None:
        """Initialize all generators with shared seed."""
        seed = self.config.seed
        self.membro_gen = MembroGenerator(seed=seed)
        self.prestador_gen = PrestadorGenerator(seed=seed)
        self.guia_gen = GuiaGenerator(seed=seed)
        # ... other generators

    def generate(self) -> SaudeDataStore:
        """Execute scenario in dependency order.

        Returns
        -------
        SaudeDataStore
            Store with all generated entities and relationships.
        """
        # Level 0 — no FK dependencies
        self._generate_membros()
        self._generate_prestadores()

        # Level 1 — FK to Membro/Prestador
        self._generate_planos()
        self._generate_dependentes()
        self._generate_contratos()

        # Level 2 — FK to Plano/Prestador
        self._generate_guias()
        self._generate_autorizacoes()
        self._generate_mensalidades()

        # Behavioral patterns
        if self.config.enable_recurring_consultas:
            self._apply_recurring_consultas()
        if self.config.enable_seasonal_internacoes:
            self._apply_seasonal_internacoes()
        if self.config.enable_glosa_patterns:
            self._apply_glosa_patterns()
        if self.config.enable_coparticipacao:
            self._apply_coparticipacao()

        # Poison pills
        if self.config.poison_pill_config:
            self._inject_poison_pills()

        return self.store

    def get_labels(self) -> dict[str, dict]:
        """Return ground truth labels for ML training."""
        return self._labels

    def get_summary(self) -> dict[str, Any]:
        """Return scenario statistics."""
        return {
            "membros": len(self.store.membros),
            "prestadores": len(self.store.prestadores),
            "guias": sum(
                len(guias) for guias in self.store._membro_guias.values()
            ),
            "labels": {k: len(v) for k, v in self._labels.items()},
        }

    # --- Private generation methods ---

    def _generate_membros(self) -> None:
        """Generate Level 0: Membros/Beneficiarios."""
        for _ in range(self.config.num_membros):
            membro = self.membro_gen.generate()
            self.store.add_membro(membro)

    def _generate_planos(self) -> None:
        """Generate Level 1: Planos (FK -> Membro)."""
        for membro in self.store.membros.values():
            for _ in range(self.config.planos_per_membro):
                plano = self.plano_gen.generate_for_membro(membro)
                self.store.add_plano(plano)

    # ... other _generate methods following same pattern
```

## Scenario Patterns

### Pattern: Composition over Inheritance

```python
# DO: Compose patterns as independent, injectable classes
class RealisticHealthInsuranceScenario:
    def __init__(self, config):
        self.patterns = [
            RecurringConsultaPattern(config.consultas),
            SeasonalInternacaoPattern(config.internacoes),
            GlosaDetectionPattern(config.glosas),
            CoparticipacaoPattern(config.coparticipacao),
        ]

    def _apply_patterns(self):
        for pattern in self.patterns:
            pattern.apply(self.store)

# DON'T: Giant method with all logic inlined
```

### Pattern: Label Tracking

```python
# Track labels during generation, not after
def _generate_glosa_guias(self):
    for guia in glosa_generator.generate():
        self.store.add_guia(guia)
        self._labels.setdefault("glosa", {})[guia.guia_id] = True
```

### Pattern: Volume Calibration

```python
# Use realistic multipliers, not magic numbers
VOLUME_RATIOS = {
    "guias_per_membro_year": (2, 12),
    "consultas_per_membro_year": (1, 8),
    "internacoes_per_membro_year": (0, 1),
    "dependentes_per_titular": (0, 4),
    "glosa_rate": 0.08,
    "coparticipacao_rate": 0.30,
}
```

## Checklist for New Scenarios

- [ ] Dependency graph documented
- [ ] Generators initialized with shared seed
- [ ] Generation follows L0 -> L1 -> L2 -> patterns -> poison order
- [ ] Config dataclass with all parameters and defaults
- [ ] `get_labels()` returns ground truth
- [ ] `get_summary()` returns statistics
- [ ] Referential integrity maintained via SaudeDataStore
- [ ] Tests: determinism, volume, FK integrity, label accuracy
- [ ] 80%+ test coverage

## Example Usage

```
"Use scenario-builder to create a DataQualityScenario with all 12 poison types para o lab de DLQ do módulo 3"
"Build a RealisticPaymentsScenario with bimodal transaction hours and Pareto accounts"
"Create a FraudDetectionScenario with velocity and geo-impossible patterns para o lab do módulo 4"
"Design a scenario for testing Kafka deduplication with duplicate poison pills"
```
