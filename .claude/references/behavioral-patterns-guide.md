# Behavioral Patterns Guide

> **Domain logic for authorization flows, billing cycles, provider utilization, member lifecycle, and seasonality in Brazilian payments**

## 1. Authorization Flow (Guias TISS)

### Overview

Every business event flows through a state machine — e.g. a payment transaction goes PENDING → AUTHORIZED → SETTLED, or PENDING → DECLINED. Behavioural patterns model these state transitions realistically across time and produce the bulk of event-driven data.

### Guide Types & Realistic Values

| Type | Examples | Value Range (BRL) | Frequency |
|------|----------|-------------------|-----------|
| **CONSULTA** | Consulta eletiva, retorno | 80–250 | Most common |
| **SP_SADT** | Exames lab, imagem, fisioterapia | 50–5,000 | High volume |
| **INTERNACAO** | Internação clínica, cirúrgica | 2,000–100,000+ | Low volume, high value |
| **ODONTOLOGICA** | Procedimentos dentários | 50–2,000 | Moderate |
| **HONORARIOS** | Honorários médicos | 200–15,000 | Per procedure |

### Authorization State Machine

```python
GUIA_TRANSITIONS = {
    "SOLICITADA":   ["EM_ANALISE", "CANCELADA"],
    "EM_ANALISE":   ["AUTORIZADA", "NEGADA", "PENDENTE_DOC"],
    "PENDENTE_DOC": ["EM_ANALISE", "CANCELADA"],
    "AUTORIZADA":   ["EXECUTADA", "CANCELADA"],
    "NEGADA":       [],  # terminal
    "EXECUTADA":    ["FATURADA"],
    "FATURADA":     ["PAGA", "GLOSADA"],
    "GLOSADA":      ["RECURSO", "PAGA_PARCIAL"],
    "CANCELADA":    [],  # terminal
}

# Transition probabilities
TRANSITION_RATES = {
    "SOLICITADA→EM_ANALISE": 0.95,
    "SOLICITADA→CANCELADA": 0.05,
    "EM_ANALISE→AUTORIZADA": 0.85,
    "EM_ANALISE→NEGADA": 0.10,
    "EM_ANALISE→PENDENTE_DOC": 0.05,
    "AUTORIZADA→EXECUTADA": 0.92,
    "AUTORIZADA→CANCELADA": 0.08,
    "EXECUTADA→FATURADA": 1.00,
    "FATURADA→PAGA": 0.88,
    "FATURADA→GLOSADA": 0.12,
}
```

### Processing Times

```python
# Time between transitions (in hours)
PROCESSING_TIMES = {
    "SOLICITADA→EM_ANALISE": {"min": 1, "max": 24},
    "EM_ANALISE→AUTORIZADA": {"min": 2, "max": 72},
    "EM_ANALISE→NEGADA": {"min": 4, "max": 48},
    "AUTORIZADA→EXECUTADA": {"min": 24, "max": 720},   # 1-30 days
    "EXECUTADA→FATURADA": {"min": 24, "max": 240},     # 1-10 days
    "FATURADA→PAGA": {"min": 720, "max": 2160},        # 30-90 days
    "FATURADA→GLOSADA": {"min": 720, "max": 1440},     # 30-60 days
}
```

---

## 2. Billing Cycles (Mensalidades)

### Overview

Every active member pays a monthly premium (mensalidade). This is the primary revenue stream for the health plan.

### Payment Patterns

```python
# Due date distribution
COMMON_DUE_DAYS = [5, 10, 15, 20, 25]

# Payment behavior
PAYMENT_BEHAVIOR = {
    "on_time": 0.85,        # paid by due date
    "late_1_5_days": 0.08,  # 1-5 days late
    "late_6_30_days": 0.04, # 6-30 days late
    "defaulted": 0.03,      # 30+ days, suspended
}

# Premium ranges by plan type
PREMIUM_RANGES = {
    "INDIVIDUAL": {"min": 200, "max": 800},
    "FAMILIAR": {"min": 500, "max": 2500},
    "EMPRESARIAL": {"min": 150, "max": 600},      # per member
    "ADESAO": {"min": 250, "max": 1200},
}

# Annual adjustment (reajuste)
ANNUAL_ADJUSTMENT = {
    "INDIVIDUAL": 0.10,     # ~10% ANS regulated
    "FAMILIAR": 0.10,
    "EMPRESARIAL": 0.08,    # negotiated, lower
    "ADESAO": 0.09,
}
```

### Coparticipação (Copayment)

```python
# Copayment percentages by procedure type
COPARTICIPACAO_RATES = {
    "CONSULTA": 0.30,          # 30% copay
    "SP_SADT_SIMPLES": 0.20,   # 20% for simple exams
    "SP_SADT_COMPLEXO": 0.10,  # 10% for complex exams
    "INTERNACAO": 0.0,         # no copay for hospitalization
}
```

---

## 3. Member Lifecycle

### State Transitions

```
Event                    From         To          Rate
──────────────────────────────────────────────────────
member.enrolled          —            CARENCIA    100% (new member)
member.activated         CARENCIA     ATIVO       95% (after carencia period)
member.suspended         ATIVO        SUSPENSO    3% (payment default)
member.reactivated       SUSPENSO     ATIVO       60% (payment regularized)
member.cancelled         ATIVO        CANCELADO   5% annual
member.cancelled         SUSPENSO     CANCELADO   40% (90+ days suspended)
member.portability       ATIVO        PORTAB      2% (transfer to another plan)
```

### Carencia (Waiting Period)

```python
# Mandatory waiting periods (ANS regulated)
CARENCIA_PERIODS = {
    "URGENCIA_EMERGENCIA": 24,       # 24 hours
    "CONSULTAS": 180,                # 180 days (6 months)
    "EXAMES_SIMPLES": 180,           # 180 days
    "EXAMES_COMPLEXOS": 180,         # 180 days
    "INTERNACAO": 180,               # 180 days
    "PARTO": 300,                    # 300 days (10 months)
    "DOENCA_PREEXISTENTE": 720,      # 720 days (24 months / CPT)
    "ODONTOLOGIA": 180,              # 180 days
}
```

### Event Generation

```python
LIFECYCLE_EVENTS = {
    "member.enrolled": {
        "source": "cadastro",
        "data": {"canal": "SITE|CORRETOR|EMPRESA", "plano_id": str}
    },
    "member.activated": {
        "source": "beneficiarios",
        "data": {"carencia_cumprida": bool}
    },
    "member.suspended": {
        "source": "faturamento",
        "data": {"dias_inadimplente": int, "mensalidades_pendentes": int}
    },
    "member.cancelled": {
        "source": "beneficiarios",
        "data": {"motivo": "VOLUNTARIO|INADIMPLENCIA|OBITO|PORTABILIDADE"}
    },
}
```

---

## 4. Provider Utilization

### Overview

Prestadores (providers) have variable utilization patterns based on specialty, season, and capacity.

### Utilization Patterns

```python
# Monthly utilization rates per member
UTILIZATION_RATES = {
    "CONSULTA": {"mean": 0.25, "std": 0.1},          # ~3 per year
    "EXAME_LABORATORIAL": {"mean": 0.15, "std": 0.08}, # ~1.8 per year
    "EXAME_IMAGEM": {"mean": 0.06, "std": 0.03},      # ~0.7 per year
    "FISIOTERAPIA": {"mean": 0.03, "std": 0.02},       # ~0.4 per year
    "INTERNACAO": {"mean": 0.008, "std": 0.005},       # ~0.1 per year
    "CIRURGIA": {"mean": 0.005, "std": 0.003},         # ~0.06 per year
}

# Age multipliers (higher utilization for elderly)
AGE_UTILIZATION_MULTIPLIER = {
    "0-18": 0.7,
    "19-29": 0.5,
    "30-49": 0.8,
    "50-64": 1.5,
    "65+": 2.5,
}
```

### Sinistralidade (Loss Ratio)

```python
def calculate_sinistralidade(total_claims: float, total_premiums: float) -> float:
    """Calculate loss ratio (sinistralidade) for the health plan."""
    if total_premiums == 0:
        return 0.0
    ratio = total_claims / total_premiums
    return round(ratio, 4)

# Healthy range
SINISTRALIDADE_RANGES = {
    "excellent": (0.0, 0.70),    # < 70% — very profitable
    "healthy": (0.70, 0.80),     # 70-80% — target range
    "concerning": (0.80, 0.90),  # 80-90% — needs attention
    "critical": (0.90, 1.00),    # 90-100% — unsustainable
    "loss": (1.00, float("inf")),# > 100% — operating at loss
}
```

---

## 5. Glosas (Claim Denials)

### Overview

12% of claims are partially or fully denied (glosadas). This is a critical metric for provider relations and operational efficiency.

### Glosa Types & Rates

| Type | Rate | Examples |
|------|------|---------|
| **ADMINISTRATIVA** | 60% | Missing documents, wrong codes, expired authorization |
| **TECNICA** | 30% | Procedure not covered, exceeded quantity, incompatible CID/TUSS |
| **FINANCEIRA** | 10% | Value above table, duplicate billing, calculation error |

### Detection Rules

```python
GLOSA_RULES = {
    "GUIA_EXPIRADA": lambda g: (datetime.now() - g.dt_autorizacao).days > 30,
    "TUSS_INVALIDO": lambda g: not validate_tuss(g.codigo_tuss),
    "CID_INCOMPATIVEL": lambda g: g.cid_10 not in TUSS_CID_MAPPING.get(g.codigo_tuss, []),
    "VALOR_ACIMA_TABELA": lambda g: g.valor_cobrado > g.valor_tabela * 1.1,
    "DUPLICIDADE": lambda g: has_duplicate_claim(g.membro_id, g.codigo_tuss, g.dt_execucao),
    "QUANTIDADE_EXCEDIDA": lambda g: g.quantidade > MAX_QUANTITIES.get(g.codigo_tuss, 1),
}
```

---

## 6. Seasonal Patterns

### Monthly Multipliers (example: Brazilian Health Insurance, legacy)

| Month | Utilization | Reason |
|-------|-----------|--------|
| **Jan** | 0.7x | Post-holiday, low demand |
| **Feb** | 0.6x | Carnival week, reduced operations |
| **Mar** | 1.0x | Normal baseline, back-to-routine |
| **Apr** | 1.0x | Normal baseline |
| **May** | 1.1x | Flu season beginning (Minas Gerais) |
| **Jun** | 1.3x | Winter peak — respiratory diseases |
| **Jul** | 1.4x | Peak flu/respiratory season |
| **Aug** | 1.2x | Late winter respiratory |
| **Sep** | 1.0x | Normal baseline |
| **Oct** | 1.0x | Normal baseline |
| **Nov** | 1.1x | Pre-end-of-year checkups |
| **Dec** | 0.8x | Holiday period, elective procedures postponed |

### Implementation

```python
SEASONAL_MULTIPLIERS = {
    1: 0.7, 2: 0.6, 3: 1.0, 4: 1.0,
    5: 1.1, 6: 1.3, 7: 1.4, 8: 1.2,
    9: 1.0, 10: 1.0, 11: 1.1, 12: 0.8,
}

def apply_seasonality(
    base_count: int,
    month: int,
    procedure_type: str = "general",
) -> int:
    """Adjust utilization count by seasonal multiplier."""
    multiplier = SEASONAL_MULTIPLIERS.get(month, 1.0)

    # Procedure-specific adjustments
    if procedure_type == "INTERNACAO" and month in (6, 7):
        multiplier *= 1.3  # extra winter hospitalization boost
    elif procedure_type == "CONSULTA" and month in (11, 12):
        multiplier *= 1.2  # end-of-year checkup rush

    return max(1, int(base_count * multiplier))
```

### Day-of-Week Patterns

```python
# Health service utilization by day of week
DAY_OF_WEEK_MULTIPLIERS = {
    0: 1.2,   # Monday — highest (pent-up weekend demand)
    1: 1.1,   # Tuesday — above average
    2: 1.0,   # Wednesday — baseline
    3: 1.0,   # Thursday — baseline
    4: 0.9,   # Friday — slightly below (shorter hours)
    5: 0.3,   # Saturday — emergency/urgent only
    6: 0.1,   # Sunday — emergency only
}
```

---

## Quick Reference: All Patterns Combined

```
Membro (L0)
├── Mensalidade — 1x/month, CREDIT to plan
│   └── 3% default → SUSPENSO after 60 days
├── Dependentes (0-5) — each with own utilization
├── Guias TISS (3-8/year) × seasonal multiplier
│   ├── 85% authorized, 10% denied, 5% pending
│   ├── CONSULTA 40%, SP_SADT 35%, INTERNACAO 10%, ODONTO 10%, HONORARIOS 5%
│   ├── Each guia → 1-8 items (procedures)
│   └── 12% of executed claims → glosa
├── Member lifecycle (CARENCIA→ATIVO→SUSPENSO→CANCELADO)
│   └── 5% annual voluntary cancellation
├── Age-based utilization multiplier (0.5x young → 2.5x elderly)
└── Sinistralidade target: 75-85%
```
