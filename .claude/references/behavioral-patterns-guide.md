# Behavioral Patterns Guide

> **Domain logic for transaction flows, refund cycles, account lifecycle, merchant patterns, fraud behavior, and seasonality in Brazilian payments**

## 1. Transaction Flow

### Overview

Every business event flows through a state machine — a payment transaction goes CREATED → AUTHORIZED → SETTLED, or CREATED → DECLINED. Behavioural patterns model these state transitions realistically across time and produce the bulk of event-driven data.

### Channels & Realistic Values

| Channel | Examples | Value Range (BRL) | Frequency |
|---------|----------|-------------------|-----------|
| **PIX** | Transferências, pagamentos QR | 1–5,000 | Most common (~45%) |
| **CREDIT** | Compras parceladas, e-commerce | 10–10,000 | High volume (~25%) |
| **DEBIT** | Compras à vista, presencial | 5–2,000 | Moderate (~20%) |
| **BOLETO** | Contas, assinaturas | 50–10,000 | Low volume (~10%) |

### Transaction State Machine

```python
TRANSACTION_TRANSITIONS = {
    "CREATED":     ["AUTHORIZED", "DECLINED"],
    "AUTHORIZED":  ["SETTLED", "CANCELLED"],
    "DECLINED":    [],  # terminal
    "SETTLED":     ["REFUNDED"],
    "CANCELLED":   [],  # terminal
    "REFUNDED":    [],  # terminal
}

# Transition probabilities (approval 92-97% — ver rules.md)
TRANSITION_RATES = {
    "CREATED→AUTHORIZED": 0.95,
    "CREATED→DECLINED": 0.05,
    "AUTHORIZED→SETTLED": 0.98,
    "AUTHORIZED→CANCELLED": 0.02,
    "SETTLED→REFUNDED": 0.02,
}

DECLINE_REASONS = {
    "INSUFFICIENT_FUNDS": 0.55,
    "SUSPECTED_FRAUD": 0.25,
    "EXPIRED_CARD": 0.12,
    "LIMIT_EXCEEDED": 0.08,
}
```

### Processing Times

```python
# Time between transitions
PROCESSING_TIMES = {
    "CREATED→AUTHORIZED": {"min_s": 0.1, "max_s": 5},        # authorization is near-real-time
    "CREATED→DECLINED": {"min_s": 0.1, "max_s": 5},
    "AUTHORIZED→SETTLED_PIX": {"min_s": 1, "max_s": 10},     # PIX liquida em segundos
    "AUTHORIZED→SETTLED_DEBIT": {"min_h": 24, "max_h": 48},  # D+1
    "AUTHORIZED→SETTLED_CREDIT": {"min_h": 24, "max_h": 720},# D+1 a D+30
    "SETTLED→REFUNDED": {"min_h": 24, "max_h": 2160},        # 1-90 days (chargeback window)
}
```

---

## 2. Refund / Chargeback Cycles

### Overview

~2% of settled transactions are refunded. Refunds are compensation events — the natural example for the Saga pattern (Live 2) and for retroactive corrections arriving late in the stream.

### Refund Patterns

```python
# Refund reasons
REFUND_REASONS = {
    "FRAUD": 0.40,             # cardholder disputes as fraud
    "NOT_RECOGNIZED": 0.30,    # cliente não reconheceu a compra
    "DUPLICATE": 0.20,         # cobrança duplicada
    "AMOUNT_ERROR": 0.07,      # valor errado
    "OTHER": 0.03,
}

# Refund timing after settlement (days) — long tail
REFUND_DELAY_DAYS = {"min": 1, "max": 90, "median": 7}

# Refund resolution
REFUND_RESOLUTION = {
    "APPROVED": 0.70,
    "REJECTED": 0.25,
    "PARTIAL": 0.05,
}
```

---

## 3. Account Lifecycle

### State Transitions

```
Event                    From         To          Rate
──────────────────────────────────────────────────────
account.opened           —            ACTIVE      100% (new account)
account.suspended        ACTIVE       SUSPENDED   2% annual (default/suspeita)
account.reactivated      SUSPENDED    ACTIVE      60% (regularization)
account.closed           ACTIVE       CLOSED      4% annual (voluntary)
account.closed           SUSPENDED    CLOSED      40% (90+ days suspended)
```

### Event Generation

```python
LIFECYCLE_EVENTS = {
    "account.opened": {
        "source": "onboarding",
        "data": {"canal": "APP|SITE|AGENCIA", "tier": "BASIC|PLUS|PREMIUM"}
    },
    "account.suspended": {
        "source": "risk",
        "data": {"motivo": "FRAUD_SUSPECT|DEFAULT", "dias_inadimplente": int}
    },
    "account.reactivated": {
        "source": "risk",
        "data": {"revisado_por": str}
    },
    "account.closed": {
        "source": "onboarding",
        "data": {"motivo": "VOLUNTARIO|INATIVIDADE|FRAUDE|OBITO"}
    },
}
```

---

## 4. Merchant & Account Utilization

### Overview

Transaction volume is heavily skewed: few accounts and few merchants concentrate most of the volume (Pareto — ver rules.md).

### Utilization Patterns

```python
# Transactions per account per month
UTILIZATION_RATES = {
    "heavy":  {"share": 0.20, "txn_month": (40, 150)},  # 20% accounts → 80% volume
    "medium": {"share": 0.50, "txn_month": (5, 40)},
    "light":  {"share": 0.30, "txn_month": (0, 5)},
}

# MCC concentration: top 10 MCCs ≈ 70% of volume
MCC_CONCENTRATION = {
    "5411": 0.18,  # supermercados
    "5541": 0.11,  # combustível
    "5812": 0.10,  # restaurantes
    "5814": 0.08,  # fast food
    "5912": 0.07,  # farmácias
    # ... resto distribuído em long tail
}

# Tier multipliers (higher volume/value for premium accounts)
TIER_MULTIPLIER = {
    "BASIC": 0.7,
    "PLUS": 1.2,
    "PREMIUM": 2.0,
}
```

---

## 5. Fraud Behavior (para labs de Flink CEP e ML_PREDICT)

### Overview

0.5–1.5% of transactions are fraudulent. Fraud is injected as coherent *bursts*, not isolated flags — each pattern below maps to a RULE_* indicator (rules.md) detectable via MATCH_RECOGNIZE (módulo 4) or scored via ML_PREDICT (módulo 6).

### Fraud Patterns & Injection

| Pattern | Shape | Indicador |
|---------|-------|-----------|
| **Velocity** | 3+ transactions on same card in <60s, values escalating | `RULE_VELOCITY` |
| **Geo impossible** | 2 transactions in different UFs within <10min | `RULE_GEO_IMPOSSIBLE` |
| **Amount outlier** | Transaction > 10× the card's 90-day average | `RULE_AMOUNT_OUTLIER` |
| **Night owl** | 02h–05h transaction on account with no nocturnal history | `RULE_NIGHT_OWL` |

```python
FRAUD_INJECTION = {
    "RULE_VELOCITY": {
        "rate": 0.004,
        "burst_size": (3, 8),
        "interval_s": (2, 20),
        "amount_pattern": "escalating",   # R$50 → R$200 → R$800...
    },
    "RULE_GEO_IMPOSSIBLE": {
        "rate": 0.003,
        "uf_pairs": [("SP", "AM"), ("RS", "CE"), ("MG", "RR")],
        "interval_min": (1, 10),
    },
    "RULE_AMOUNT_OUTLIER": {
        "rate": 0.002,
        "multiplier": (10, 50),           # vs card 90-day average
    },
    "RULE_NIGHT_OWL": {
        "rate": 0.002,
        "hour_range": (2, 5),
        "requires": "no_night_history",
    },
}
```

---

## 6. Seasonal Patterns

### Monthly Multipliers (Brazilian payments)

| Month | Volume | Reason |
|-------|--------|--------|
| **Jan** | 0.8x | Pós-festas, contas do início do ano |
| **Feb** | 0.8x | Carnaval — consumo deslocado p/ lazer |
| **Mar** | 1.0x | Baseline |
| **Apr** | 1.0x | Baseline |
| **May** | 1.1x | Dia das Mães |
| **Jun** | 1.0x | Festas juninas (regional) |
| **Jul** | 1.0x | Férias escolares |
| **Aug** | 1.0x | Dia dos Pais |
| **Sep** | 1.0x | Baseline |
| **Oct** | 1.1x | Dia das Crianças |
| **Nov** | 1.4x | **Black Friday** — pico do ano |
| **Dec** | 1.3x | Natal + 13º salário |

### Implementation

```python
SEASONAL_MULTIPLIERS = {
    1: 0.8, 2: 0.8, 3: 1.0, 4: 1.0,
    5: 1.1, 6: 1.0, 7: 1.0, 8: 1.0,
    9: 1.0, 10: 1.1, 11: 1.4, 12: 1.3,
}

def apply_seasonality(
    base_count: int,
    month: int,
    channel: str = "general",
) -> int:
    """Adjust transaction count by seasonal multiplier."""
    multiplier = SEASONAL_MULTIPLIERS.get(month, 1.0)

    # Channel-specific adjustments
    if channel == "CREDIT" and month == 11:
        multiplier *= 1.3  # Black Friday e-commerce boost
    elif channel == "PIX" and month == 12:
        multiplier *= 1.2  # transferências de fim de ano

    return max(1, int(base_count * multiplier))
```

### Day-of-Week / Hour Patterns

```python
# Transaction volume by day of week
DAY_OF_WEEK_MULTIPLIERS = {
    0: 0.9,   # Monday
    1: 0.9,   # Tuesday
    2: 1.0,   # Wednesday — baseline
    3: 1.0,   # Thursday
    4: 1.3,   # Friday — payday + weekend prep
    5: 1.2,   # Saturday — retail peak
    6: 0.8,   # Sunday
}

# Intraday: bimodal — peaks 11–13h and 18–21h (horário de Brasília, ver rules.md)
```

---

## Quick Reference: All Patterns Combined

```
Account (L0)
├── Cards (1-3) — each with own utilization
├── Transactions (Pareto: 20% accounts → 80% volume) × seasonal multiplier
│   ├── 95% authorized → 98% settled; 5% declined
│   ├── PIX 45%, CREDIT 25%, DEBIT 20%, BOLETO 10%
│   ├── Amounts log-normal (median ~R$80, tail até R$10K)
│   └── 2% of settled → refund (40% por FRAUD)
├── Fraud bursts (0.5–1.5%) — RULE_VELOCITY, RULE_GEO_IMPOSSIBLE,
│   RULE_AMOUNT_OUTLIER, RULE_NIGHT_OWL
├── Account lifecycle (ACTIVE→SUSPENDED→CLOSED)
│   └── 4% annual voluntary closure
├── Geo: UF da transação = UF do cartão em ≥85% dos casos
└── Intraday bimodal: picos 11–13h e 18–21h
```

---

**Última atualização**: 2026-07-11
