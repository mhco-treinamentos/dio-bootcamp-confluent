# Instruction: criar novo gerador de dados sintéticos

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para criar um gerador determinístico para uma entidade do domínio de payments.**

## Pré-requisitos

- Python 3.11+
- `faker`, `confluent-kafka`, `confluent-kafka[avro]` instalados
- Entidade alvo identificada (`Conta`, `Cartao`, `Transacao`, `EventoFraude`, etc.)
- Edição da tour onde o gerador vai morar (no momento: `data-streaming-tour-2026/`)

---

## Phase 1: RESEARCH

### 1.1 — Coletar requisitos
- Qual entidade vai ser gerada?
- Quais campos são obrigatórios? Quais são opcionais?
- Que relacionamentos tem com outras entidades? (Ex: `Transacao` referencia `Cartao` que referencia `Conta`)
- Que distribuições estatísticas usar? (Ver [.claude/rules.md](../rules.md) seção "Domain Rules — Payments" para defaults: log-normal de valor, Pareto de merchants, bimodal de horário, etc.)
- Que volume é esperado nas cenas?
- Que campos são PII/PCI? (Ver [.claude/rules/schemas.md](../rules/schemas.md))

### 1.2 — Revisar geradores existentes
- Inspecionar `data-streaming-tour-2026/shared/generators/payments/`
- Há `BaseGenerator` ABC para herdar?
- Há `FakerPool` ou utilitários comuns que valem reuso? (`brazilian_formats.py`)

### 1.3 — Consultar references
- [references/faker-patterns.md](../references/faker-patterns.md) — padrões de uso da lib Faker
- [references/data-modeling.md](../references/data-modeling.md) — padrões de modelagem
- [references/brazilian-data-formats.md](../references/brazilian-data-formats.md) — CPF, CNPJ, telefone, CEP, MCC

---

## Phase 2: PLAN

### 2.1 — Desenhar schema Avro

```json
{
  "type": "record",
  "name": "Transacao",
  "namespace": "com.workshop.payments.transacoes",
  "doc": "Transação financeira emitida por um cartão",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "transaction_id", "type": "string"},
    {"name": "card_id", "type": "string"},
    {
      "name": "amount",
      "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}
    },
    {"name": "mcc", "type": "string", "doc": "Merchant category code, 4 dígitos"},
    {
      "name": "status",
      "type": {"type": "enum", "name": "StatusTransacao", "symbols": ["AUTORIZADA", "NEGADA", "ESTORNADA", "PENDENTE", "EM_ANALISE"]}
    }
  ]
}
```

Regras de schema obrigatórias (ver [.claude/rules/schemas.md](../rules/schemas.md)):
- Namespace: `com.workshop.payments.<subdomínio>`
- Money fields: sempre `bytes` + `decimal` logical type, **nunca** `double`/`float`
- Status/tipo: sempre `enum`, nunca string crua
- PII/PCI: taggear com `confluent:tags` — o hook `validate-avro-pii.sh` bloqueia o save se faltar

### 2.2 — Desenhar a classe

```
TransacaoGenerator(BaseGenerator)
├── __init__(seed: int = 42, locale: str = "pt_BR")
├── generate(count: int) -> Iterator[dict]
├── _generate_single() -> dict
├── _amount_lognormal() -> Decimal   # distribuição log-normal mediana ~R$80
├── _pick_mcc() -> str               # top-10 MCC pondera 70% via Pareto
├── _pick_card_id() -> str           # referencia pool pré-gerado de cartões
└── get_schema() -> dict
```

### 2.3 — Documentar padrões aplicados
- Distribuições: log-normal de valor, Pareto de merchant, bimodal de horário (11–13h + 18–21h)
- Relacionamentos: `card_id` precisa existir no pool de cartões (referencial integrity)
- Edge cases: status `NEGADA` com motivo, valor zero (estorno), MCC raros

---

## Phase 3: ACT

### 3.1 — Criar o arquivo do gerador

**Local**: `data-streaming-tour-2026/shared/generators/payments/transacoes.py`

```python
"""Gerador de Transacao sintética (payments)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

from faker import Faker

from shared.generators.base import BaseGenerator

logger = logging.getLogger(__name__)


@dataclass
class TransacaoGeneratorConfig:
    seed: int = 42
    locale: str = "pt_BR"
    amount_median_brl: Decimal = Decimal("80.00")


class TransacaoGenerator(BaseGenerator):
    """Gera transações sintéticas com padrões realistas de pagamentos brasileiros."""

    def __init__(self, config: TransacaoGeneratorConfig | None = None) -> None:
        self.config = config or TransacaoGeneratorConfig()
        self.fake = Faker(self.config.locale)
        Faker.seed(self.config.seed)

    def generate(self, count: int) -> Iterator[dict]:
        for _ in range(count):
            yield self._generate_single()

    def _generate_single(self) -> dict:
        return {
            "event_id": self.fake.uuid4(),
            "event_time": int(self.fake.unix_time(end_datetime="now") * 1000),
            "transaction_id": self.fake.uuid4(),
            "card_id": self._pick_card_id(),
            "amount": self._amount_lognormal(),
            "mcc": self._pick_mcc(),
            "status": "AUTORIZADA",
        }

    def get_schema(self) -> dict:
        # ... ver Phase 2.1
        ...
```

### 3.2 — Adicionar testes

**Local**: `data-streaming-tour-2026/tests/generators/test_transacoes.py`

```python
"""Testes do TransacaoGenerator."""
from decimal import Decimal

from shared.generators.payments.transacoes import TransacaoGenerator, TransacaoGeneratorConfig


class TestTransacaoGenerator:
    def test_generates_valid_records(self) -> None:
        gen = TransacaoGenerator(TransacaoGeneratorConfig(seed=42))
        records = list(gen.generate(100))
        assert len(records) == 100
        for r in records:
            assert "transaction_id" in r
            assert r["amount"] > Decimal("0")
            assert len(r["mcc"]) == 4

    def test_deterministic_with_seed(self) -> None:
        gen1 = TransacaoGenerator(TransacaoGeneratorConfig(seed=42))
        gen2 = TransacaoGenerator(TransacaoGeneratorConfig(seed=42))
        assert list(gen1.generate(10)) == list(gen2.generate(10))
```

### 3.3 — Exportar do módulo

`data-streaming-tour-2026/shared/generators/payments/__init__.py`:
```python
from .transacoes import TransacaoGenerator, TransacaoGeneratorConfig

__all__ = ["TransacaoGenerator", "TransacaoGeneratorConfig"]
```

---

## Phase 4: VALIDATE

### 4.1 — Rodar testes
```bash
pytest data-streaming-tour-2026/tests/generators/test_transacoes.py -v
```

### 4.2 — Profile dos dados
```python
gen = TransacaoGenerator(TransacaoGeneratorConfig(seed=42))
records = list(gen.generate(1000))
# Verifique: distribuição de amount (log-normal), distribuição de MCC (Pareto),
# uniqueness de transaction_id, NULL em campos opcionais
```

### 4.3 — Validar realismo (checklist)
- [ ] Valores dentro de faixas plausíveis (amount > 0, mediana ~R$80, cauda longa até ~R$10k)
- [ ] Distribuições batem com requisitos
- [ ] Padrões temporais corretos (bimodal de horário)
- [ ] Integridade referencial: `card_id` existe no pool
- [ ] PII/PCI conforme regra (sem PAN completo, sem CVV)
- [ ] Edge cases tratados (status NEGADA, estorno, MCC raro)

### 4.4 — Validar Avro schema
- Save do `.avsc` em `data-streaming-tour-2026/shared/schemas/payments/transacao.avsc`
- O hook [validate-avro-pii.sh](../hooks/validate-avro-pii.sh) bloqueia se houver campo sensível sem `confluent:tags`

---

## Checklist final

- [ ] Requisitos coletados e documentados
- [ ] Schema Avro desenhado, namespace + tags PII/PCI OK
- [ ] Gerador implementado, BaseGenerator herdado
- [ ] Testes passando (≥80% coverage)
- [ ] Dados profilados e validados estatisticamente
- [ ] Schema Avro salvo e aprovado pelo hook PII
- [ ] CHANGELOG atualizado
- [ ] Module exports atualizados

---

## Exemplo de uso

```
"Siga new-generator para criar um EventoFraudeGenerator que produza
eventos de regra disparada (RULE_VELOCITY, RULE_GEO_IMPOSSIBLE) com
distribuição realista de 1% das transações"
```
