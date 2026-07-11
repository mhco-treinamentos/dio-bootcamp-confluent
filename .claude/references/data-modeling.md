# Data Modeling Reference

> **Domain data modeling patterns for the bootcamp payments data generation**

## Domain models — Payments

### Customer Model
```python
@dataclass
class Customer:
    """Bank customer (pessoa física)."""

    customer_id: str             # UUID
    cpf: str                     # PII — 11 digits, valid check digits
    nome: str                    # PII
    data_nascimento: date
    email: str | None            # PII
    telefone: str | None         # PII — +55XXXXXXXXXXX
    endereco: Endereco
    status: str = "ACTIVE"       # ACTIVE, SUSPENDED, CLOSED
    created_at: datetime
    updated_at: datetime
```

### Account Model
```python
@dataclass
class Account:
    """Checking/payment account."""

    account_id: str              # UUID
    customer_id: str             # FK to Customer
    agencia: str                 # 4 digits
    conta: str                   # up to 13 digits + check digit
    tier: str                    # BASIC, PLUS, PREMIUM
    balance: Decimal             # never float — BRL, 2 decimal places
    overdraft_limit: Decimal     # cheque especial (0 se não contratado)
    status: str = "ACTIVE"       # ACTIVE, SUSPENDED, CLOSED
    opened_at: date
    closed_at: date | None = None
    created_at: datetime
    updated_at: datetime
```

### Card Model
```python
@dataclass
class Card:
    """Credit/debit card linked to an account."""

    card_id: str                 # UUID
    account_id: str              # FK to Account
    pan_masked: str              # PCI — ALWAYS masked: BIN(8) + ****** + last4
    brand: str                   # VISA, MASTERCARD, ELO, HIPERCARD
    card_type: str               # CREDIT, DEBIT, MULTIPLE
    credit_limit: Decimal | None # null for debit-only
    uf_emissao: str              # 2-letter state — used for geo correlation
    status: str = "ACTIVE"       # ACTIVE, BLOCKED, EXPIRED, CANCELLED
    issued_at: date
    expires_at: date
    created_at: datetime
    updated_at: datetime
```

### Merchant Model
```python
@dataclass
class Merchant:
    """Merchant/estabelecimento accepting payments."""

    merchant_id: str             # UUID
    cnpj: str                    # PII — 14 digits, valid check digits
    nome_fantasia: str
    mcc: str                     # 4-digit MCC (ISO 18245)
    uf: str                      # 2-letter state
    endereco: Endereco
    status: str = "ACTIVE"
    created_at: datetime
    updated_at: datetime
```

### Transaction Model
```python
@dataclass
class Transaction:
    """Card/PIX transaction (append-only event entity)."""

    transaction_id: str          # UUID
    account_id: str              # FK to Account
    card_id: str | None          # FK to Card (null for PIX/boleto)
    merchant_id: str | None      # FK to Merchant
    channel: str                 # PIX, CREDIT, DEBIT, BOLETO
    amount: Decimal              # BRL, log-normal (median ~R$80)
    uf_transacao: str            # correlates ≥85% with card.uf_emissao
    status: str = "CREATED"      # CREATED→AUTHORIZED→SETTLED | DECLINED | REFUNDED
    decline_reason: str | None   # INSUFFICIENT_FUNDS, SUSPECTED_FRAUD, EXPIRED_CARD
    fraud_flag: bool = False     # 0.5–1.5% of transactions
    fraud_rule: str | None       # RULE_VELOCITY, RULE_GEO_IMPOSSIBLE, ...
    created_at: datetime         # created_at < authorized_at < settled_at
    authorized_at: datetime | None
    settled_at: datetime | None
```

### Refund Model
```python
@dataclass
class Refund:
    """Refund/chargeback of a settled transaction."""

    refund_id: str               # UUID
    transaction_id: str          # FK to Transaction
    refund_amount: Decimal       # <= transaction.amount
    reason: str                  # FRAUD, NOT_RECOGNIZED, DUPLICATE, AMOUNT_ERROR, OTHER
    initiated_by: str | None     # operator id ou null se automático
    status: str = "REQUESTED"    # REQUESTED, APPROVED, REJECTED, SETTLED
    created_at: datetime
    settled_at: datetime | None
```

### FraudAlert Model
```python
@dataclass
class FraudAlert:
    """Fraud detection alert (Flink MATCH_RECOGNIZE / ML_PREDICT output)."""

    alert_id: str                # UUID
    account_id: str              # FK to Account
    transaction_ids: list[str]   # transactions that triggered the pattern
    rule: str                    # RULE_VELOCITY, RULE_GEO_IMPOSSIBLE, RULE_AMOUNT_OUTLIER, RULE_NIGHT_OWL
    fraud_score: float | None    # ML_PREDICT output (modulo 6), null se regra determinística
    detected_at: datetime
```

## Common Models

### Endereco (Address) Model
```python
@dataclass
class Endereco:
    """Brazilian address."""

    logradouro: str
    numero: str
    complemento: str = ""
    bairro: str = ""
    cidade: str = ""
    uf: str = ""                 # 2-letter code (MG, SP, etc.)
    cep: str = ""                # CEP format: XXXXX-XXX
    pais: str = "BR"
```

## Relationships

```
Customer 1:N Account
Account  1:N Card
Account  1:N Transaction
Card     1:N Transaction (via card_id, null p/ PIX)
Merchant 1:N Transaction
Transaction 0:1 Refund
Transaction 0:N FraudAlert (via transaction_ids)
```

Master entities (CDC no Postgres fonte): Customer, Account, Card, Merchant.
Event entities (append-only): Transaction, Refund, FraudAlert.

## Event Modeling

### Event Envelope
```python
@dataclass
class Event:
    """Standard event envelope."""

    event_id: str
    event_type: str              # entity.action (e.g., transaction.authorized)
    event_time: datetime
    source: str                  # Source system/connector that generated
    subject: str                 # Entity ID affected
    data: dict                   # Event payload
    metadata: dict = field(default_factory=dict)
```

### Event Types — Payments

#### Account Events
- `account.opened` — Nova conta aberta
- `account.suspended` — Suspensão (inadimplência/suspeita)
- `account.reactivated` — Regularização
- `account.closed` — Encerramento

#### Transaction Events
- `transaction.created` — Transação iniciada
- `transaction.authorized` — Autorizada pelo emissor
- `transaction.declined` — Negada (saldo, suspeita, cartão vencido)
- `transaction.settled` — Liquidada
- `transaction.refunded` — Estornada

#### Fraud Events
- `fraud.detected` — Padrão RULE_* disparado (Flink CEP, módulo 4)
- `fraud.scored` — Score de modelo via ML_PREDICT (módulo 6)
- `fraud.confirmed` — Confirmada por análise
- `fraud.dismissed` — Falso positivo

#### Refund Events
- `refund.requested` — Estorno solicitado
- `refund.approved` — Aprovado
- `refund.rejected` — Rejeitado
- `refund.settled` — Liquidado (compensação — padrão Saga, Live 2)

---

**Última atualização**: 2026-07-11
