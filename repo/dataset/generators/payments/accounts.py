"""Deterministic generator for Account (conta corrente/pagamento) master records."""

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
from dataset.generators.payments.enums import AccountStatus, AccountTier

logger = logging.getLogger(__name__)

#: Faixas de saldo inicial (BRL) por tier — plausíveis, não normativas.
_BALANCE_RANGES: dict[AccountTier, tuple[int, int]] = {
    AccountTier.BASIC: (0, 5_000),
    AccountTier.PLUS: (500, 25_000),
    AccountTier.PREMIUM: (5_000, 150_000),
}

#: Faixas de cheque especial (BRL) por tier, quando contratado.
_OVERDRAFT_RANGES: dict[AccountTier, tuple[int, int]] = {
    AccountTier.BASIC: (200, 1_000),
    AccountTier.PLUS: (1_000, 5_000),
    AccountTier.PREMIUM: (5_000, 20_000),
}


@dataclass(frozen=True)
class AccountGeneratorConfig:
    """Configuration for :class:`AccountGenerator`.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    reference_time : datetime
        Fixed "now" anchor for all timestamps.
    """

    seed: int = 42
    reference_time: datetime = REFERENCE_TIME


class AccountGenerator(BaseGenerator):
    """Generate synthetic accounts referencing an existing customer pool.

    Referential integrity is the caller's contract: every generated
    ``customer_id`` comes from the ``customers`` pool passed in.
    """

    def __init__(
        self,
        customers: Sequence[dict[str, Any]],
        config: AccountGeneratorConfig | None = None,
    ) -> None:
        if not customers:
            raise ValueError("AccountGenerator requires a non-empty customer pool")
        self.customers = list(customers)
        self.config = config or AccountGeneratorConfig()
        self.rng = random.Random(self.config.seed)

    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield ``count`` account records.

        The first ``min(count, len(customers))`` accounts cover each customer
        once (every customer has at least one account when ``count >=``
        pool size); extras are assigned randomly (some customers hold 2+).

        Parameters
        ----------
        count : int
            Number of accounts to generate.

        Yields
        ------
        dict[str, Any]
            Account record matching ``schemas/payments/account.avsc``.
        """
        for i in range(count):
            customer = (
                self.customers[i] if i < len(self.customers) else self.rng.choice(self.customers)
            )
            yield self._generate_single(customer)
        logger.info("Generated %d account records", count)

    def _generate_single(self, customer: dict[str, Any]) -> dict[str, Any]:
        tier = self.rng.choices(
            [AccountTier.BASIC, AccountTier.PLUS, AccountTier.PREMIUM],
            weights=[0.70, 0.22, 0.08],
            k=1,
        )[0]
        status = self.rng.choices(
            [AccountStatus.ACTIVE, AccountStatus.SUSPENDED, AccountStatus.CLOSED],
            weights=[0.94, 0.04, 0.02],
            k=1,
        )[0]
        # Conta aberta depois do cadastro do cliente, antes do anchor
        opened_at = min(
            customer["created_at"] + timedelta(days=self.rng.randint(0, 720)),
            self.config.reference_time - timedelta(days=1),
        )
        closed_at = (
            min(
                opened_at + timedelta(days=self.rng.randint(30, 1460)),
                self.config.reference_time,
            )
            if status is AccountStatus.CLOSED
            else None
        )
        balance_lo, balance_hi = _BALANCE_RANGES[tier]
        has_overdraft = self.rng.random() < 0.40
        od_lo, od_hi = _OVERDRAFT_RANGES[tier]
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_time": opened_at,
            "account_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "customer_id": customer["customer_id"],
            "agencia": f"{self.rng.randint(1, 9999):04d}",
            "conta": f"{self.rng.randint(1, 9_999_999_999):010d}{self.rng.randint(0, 9)}",
            "tier": tier.value,
            "balance": Decimal(self.rng.randint(balance_lo * 100, balance_hi * 100)) / Decimal(100),
            "overdraft_limit": (
                Decimal(self.rng.randint(od_lo, od_hi)).quantize(Decimal("1.00"))
                if has_overdraft
                else Decimal("0.00")
            ),
            "status": status.value,
            "opened_at": opened_at.date(),
            "closed_at": closed_at.date() if closed_at else None,
            "created_at": opened_at,
            "updated_at": min(
                opened_at + timedelta(seconds=self.rng.randint(0, 60 * 86_400)),
                self.config.reference_time,
            ),
        }

    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema for account records."""
        return load_schema("account")
