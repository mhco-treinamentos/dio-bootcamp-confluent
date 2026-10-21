"""Domain enums for the payments domain.

Status/type fields are always ``Enum`` — never raw strings
(.claude/rules/python.md). Values match the Avro enum symbols in
``schemas/payments/`` and the model spec in
.claude/references/data-modeling.md.
"""

from __future__ import annotations

from enum import Enum


class CustomerStatus(str, Enum):
    """Lifecycle of a bank customer."""

    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"


class AccountStatus(str, Enum):
    """Lifecycle of a checking/payment account."""

    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"


class AccountTier(str, Enum):
    """Commercial tier of an account (drives credit limit ranges)."""

    BASIC = "BASIC"
    PLUS = "PLUS"
    PREMIUM = "PREMIUM"


class CardBrand(str, Enum):
    """Card brand (bandeira) — weights follow Brazilian market share."""

    VISA = "VISA"
    MASTERCARD = "MASTERCARD"
    ELO = "ELO"
    HIPERCARD = "HIPERCARD"


class CardType(str, Enum):
    """Card function."""

    CREDIT = "CREDIT"
    DEBIT = "DEBIT"
    MULTIPLE = "MULTIPLE"


class CardStatus(str, Enum):
    """Lifecycle of a card."""

    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class MerchantStatus(str, Enum):
    """Lifecycle of a merchant."""

    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"


class Channel(str, Enum):
    """Payment channel — PIX dominates volume in Brazil."""

    PIX = "PIX"
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"
    BOLETO = "BOLETO"


class TransactionStatus(str, Enum):
    """Transaction lifecycle: CREATED → AUTHORIZED → SETTLED | DECLINED | REFUNDED."""

    CREATED = "CREATED"
    AUTHORIZED = "AUTHORIZED"
    SETTLED = "SETTLED"
    DECLINED = "DECLINED"
    REFUNDED = "REFUNDED"


class DeclineReason(str, Enum):
    """Why a transaction was declined."""

    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    SUSPECTED_FRAUD = "SUSPECTED_FRAUD"
    EXPIRED_CARD = "EXPIRED_CARD"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"


class FraudRule(str, Enum):
    """Deterministic anti-fraud rules (Flink MATCH_RECOGNIZE — módulo 4)."""

    RULE_VELOCITY = "RULE_VELOCITY"
    RULE_GEO_IMPOSSIBLE = "RULE_GEO_IMPOSSIBLE"
    RULE_AMOUNT_OUTLIER = "RULE_AMOUNT_OUTLIER"
    RULE_NIGHT_OWL = "RULE_NIGHT_OWL"
