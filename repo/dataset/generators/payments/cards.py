"""Deterministic generator for Card master records.

PAN handling follows PCI-DSS policy from .claude/rules/schemas.md: PAN is
generated **already masked** (BIN 6 + ``******`` + last4) — the cleartext PAN
never exists, not even in memory.
"""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from dataset.generators.base import BaseGenerator
from dataset.generators.common.temporal import REFERENCE_TIME
from dataset.generators.payments._schema import load_schema
from dataset.generators.payments.enums import AccountTier, CardBrand, CardStatus, CardType

logger = logging.getLogger(__name__)

#: BINs (6 dígitos) por bandeira — sintéticos, na faixa IIN de cada bandeira.
_BRAND_BINS: dict[CardBrand, tuple[str, ...]] = {
    CardBrand.VISA: ("401712", "453123"),
    CardBrand.MASTERCARD: ("512312", "554123"),
    CardBrand.ELO: ("506639", "509001"),
    CardBrand.HIPERCARD: ("606282", "606283"),
}

#: Faixas de limite de crédito (BRL) por tier da conta.
_CREDIT_LIMIT_RANGES: dict[AccountTier, tuple[int, int]] = {
    AccountTier.BASIC: (500, 3_000),
    AccountTier.PLUS: (3_000, 15_000),
    AccountTier.PREMIUM: (15_000, 80_000),
}


@dataclass(frozen=True)
class CardGeneratorConfig:
    """Configuration for :class:`CardGenerator`.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    reference_time : datetime
        Fixed "now" anchor for all timestamps.
    """

    seed: int = 42
    reference_time: datetime = REFERENCE_TIME


class CardGenerator(BaseGenerator):
    """Generate synthetic cards referencing existing accounts (and their customers).

    ``uf_emissao`` is copied from the owning customer's address — the geo
    correlation that the anti-fraud labs (RULE_GEO_IMPOSSIBLE) rely on.
    """

    def __init__(
        self,
        accounts: Sequence[dict[str, Any]],
        customers: Sequence[dict[str, Any]],
        config: CardGeneratorConfig | None = None,
    ) -> None:
        if not accounts:
            raise ValueError("CardGenerator requires a non-empty account pool")
        self.accounts = list(accounts)
        self._customer_uf = {c["customer_id"]: c["endereco"]["uf"] for c in customers}
        self.config = config or CardGeneratorConfig()
        self.rng = random.Random(self.config.seed)

    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield ``count`` card records.

        The first ``min(count, len(accounts))`` cards cover each account once;
        extras are assigned randomly (some accounts hold multiple cards).

        Parameters
        ----------
        count : int
            Number of cards to generate.

        Yields
        ------
        dict[str, Any]
            Card record matching ``schemas/payments/card.avsc``.
        """
        for i in range(count):
            account = self.accounts[i] if i < len(self.accounts) else self.rng.choice(self.accounts)
            yield self._generate_single(account)
        logger.info("Generated %d card records", count)

    def _generate_single(self, account: dict[str, Any]) -> dict[str, Any]:
        brand = self.rng.choices(
            [CardBrand.VISA, CardBrand.MASTERCARD, CardBrand.ELO, CardBrand.HIPERCARD],
            weights=[0.40, 0.35, 0.20, 0.05],
            k=1,
        )[0]
        card_type = self.rng.choices(
            [CardType.CREDIT, CardType.DEBIT, CardType.MULTIPLE],
            weights=[0.45, 0.35, 0.20],
            k=1,
        )[0]
        status = self.rng.choices(
            [CardStatus.ACTIVE, CardStatus.BLOCKED, CardStatus.EXPIRED, CardStatus.CANCELLED],
            weights=[0.92, 0.04, 0.03, 0.01],
            k=1,
        )[0]
        tier = AccountTier(account["tier"])
        limit_lo, limit_hi = _CREDIT_LIMIT_RANGES[tier]
        issued_at = min(
            account["opened_at"] + timedelta(days=self.rng.randint(0, 360)),
            self.config.reference_time.date() - timedelta(days=1),
        )
        issued_dt = datetime.combine(
            issued_at, datetime.min.time(), tzinfo=self.config.reference_time.tzinfo
        ) + timedelta(seconds=self.rng.randint(8 * 3600, 20 * 3600))
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_time": issued_dt,
            "card_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "account_id": account["account_id"],
            "pan_masked": self._pan_masked(brand),
            "brand": brand.value,
            "card_type": card_type.value,
            "credit_limit": (
                # Múltiplos de R$ 100 — como emissores reais fazem
                Decimal(self.rng.randint(limit_lo // 100, limit_hi // 100) * 100).quantize(
                    Decimal("1.00")
                )
                if card_type is not CardType.DEBIT
                else None
            ),
            "uf_emissao": self._customer_uf.get(account["customer_id"], "SP"),
            "status": status.value,
            "issued_at": issued_at,
            "expires_at": issued_at + timedelta(days=5 * 365),
            "created_at": issued_dt,
            "updated_at": issued_dt,
        }

    def _pan_masked(self, brand: CardBrand) -> str:
        """Build a masked PAN: BIN(6) + ``******`` + last4 — never cleartext."""
        bin_ = self.rng.choice(_BRAND_BINS[brand])
        last4 = f"{self.rng.randint(0, 9999):04d}"
        return f"{bin_}******{last4}"

    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema for card records."""
        return load_schema("card")
