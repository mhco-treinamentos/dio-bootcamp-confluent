"""Deterministic generator for Transaction events (append-only entity).

Statistical realism follows .claude/rules.md — Domain Rules (Payments):

- amount: log-normal, median ~R$ 80, long tail to ~R$ 10k
- transactions per account: Pareto (20% of accounts → 80% of volume)
- time of day: bimodal (11–13h and 18–21h)
- approval rate 92–97%; fraud 0.5–1.5% of transactions
- geography: ``uf_transacao`` matches the card holder's UF in ≥85% of records

Fraud is injected as *detectable patterns*, not just flags: RULE_VELOCITY
emits an actual burst (3+ txns <60s on one card) and RULE_GEO_IMPOSSIBLE emits
two UFs minutes apart — so the Flink CEP labs (módulo 4) have something real
to match.
"""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from dataset.generators.base import BaseGenerator
from dataset.generators.common.brazilian_formats import UF_WEIGHTS
from dataset.generators.common.temporal import REFERENCE_TIME, transaction_datetime
from dataset.generators.payments._schema import load_schema
from dataset.generators.payments.enums import (
    Channel,
    DeclineReason,
    FraudRule,
    TransactionStatus,
)

logger = logging.getLogger(__name__)

#: Faixas de valor (BRL) por canal — clamp da log-normal.
_CHANNEL_RANGES: dict[Channel, tuple[int, int]] = {
    Channel.PIX: (1, 5_000),
    Channel.CREDIT: (10, 10_000),
    Channel.DEBIT: (5, 2_000),
    Channel.BOLETO: (50, 10_000),
}

_STATUS_WEIGHTS: tuple[tuple[TransactionStatus, float], ...] = (
    (TransactionStatus.SETTLED, 0.88),
    (TransactionStatus.AUTHORIZED, 0.05),
    (TransactionStatus.CREATED, 0.02),
    (TransactionStatus.DECLINED, 0.04),
    (TransactionStatus.REFUNDED, 0.01),
)

_DECLINE_WEIGHTS: tuple[tuple[DeclineReason, float], ...] = (
    (DeclineReason.INSUFFICIENT_FUNDS, 0.55),
    (DeclineReason.SUSPECTED_FRAUD, 0.25),
    (DeclineReason.EXPIRED_CARD, 0.12),
    (DeclineReason.LIMIT_EXCEEDED, 0.08),
)

_FRAUD_RULE_WEIGHTS: tuple[tuple[FraudRule, float], ...] = (
    (FraudRule.RULE_VELOCITY, 0.35),
    (FraudRule.RULE_GEO_IMPOSSIBLE, 0.25),
    (FraudRule.RULE_AMOUNT_OUTLIER, 0.25),
    (FraudRule.RULE_NIGHT_OWL, 0.15),
)


@dataclass(frozen=True)
class TransactionGeneratorConfig:
    """Configuration for :class:`TransactionGenerator`.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    reference_time : datetime
        Fixed "now" anchor for all timestamps.
    lookback_days : int
        Event-time window in the past (default 30 days).
    fraud_burst_rate : float
        Probability of starting a fraud pattern at each draw. Each burst
        yields ~2 flagged records on average, so the overall flagged share
        lands inside the 0.5–1.5% domain rule with the default value.
    geo_match_rate : float
        Share of transactions whose ``uf_transacao`` matches the holder UF.
    """

    seed: int = 42
    reference_time: datetime = REFERENCE_TIME
    lookback_days: int = 30
    fraud_burst_rate: float = 0.004
    geo_match_rate: float = 0.88


class TransactionGenerator(BaseGenerator):
    """Generate synthetic transactions over existing customer/account/card/merchant pools."""

    def __init__(
        self,
        customers: Sequence[dict[str, Any]],
        accounts: Sequence[dict[str, Any]],
        cards: Sequence[dict[str, Any]],
        merchants: Sequence[dict[str, Any]],
        config: TransactionGeneratorConfig | None = None,
    ) -> None:
        if not accounts or not merchants:
            raise ValueError("TransactionGenerator requires non-empty account and merchant pools")
        self.config = config or TransactionGeneratorConfig()
        self.rng = random.Random(self.config.seed)

        self.accounts = list(accounts)
        self.merchants = list(merchants)
        self._customer_uf = {c["customer_id"]: c["endereco"]["uf"] for c in customers}
        self._cards_by_account: dict[str, list[dict[str, Any]]] = {}
        for card in cards:
            self._cards_by_account.setdefault(card["account_id"], []).append(card)

        # Pareto: 20% das contas concentram ~80% das transações
        self._account_weights = [self.rng.paretovariate(1.2) for _ in self.accounts]
        self._pending: list[dict[str, Any]] = []

    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield exactly ``count`` transaction records.

        Parameters
        ----------
        count : int
            Number of transactions to generate.

        Yields
        ------
        dict[str, Any]
            Transaction record matching ``schemas/payments/transaction.avsc``.
        """
        produced = 0
        while produced < count:
            if self._pending:
                yield self._pending.pop(0)
                produced += 1
                continue
            if self.rng.random() < self.config.fraud_burst_rate:
                self._pending.extend(self._fraud_burst())
                continue
            yield self._generate_single()
            produced += 1
        logger.info("Generated %d transaction records", count)

    # ------------------------------------------------------------------ #
    # Normal path
    # ------------------------------------------------------------------ #

    def _pick_account(self) -> dict[str, Any]:
        return self.rng.choices(self.accounts, weights=self._account_weights, k=1)[0]

    def _amount_lognormal(self, channel: Channel) -> Decimal:
        """Log-normal amount (median ≈ e^4.4 ≈ R$ 81) clamped to the channel range."""
        lo, hi = _CHANNEL_RANGES[channel]
        raw = self.rng.lognormvariate(4.4, 1.1)
        clamped = min(max(raw, float(lo)), float(hi))
        return Decimal(str(clamped)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _home_uf(self, account: dict[str, Any], card: dict[str, Any] | None) -> str:
        if card is not None:
            return str(card["uf_emissao"])
        return str(self._customer_uf.get(account["customer_id"], "SP"))

    def _pick_uf(self, home_uf: str, match: bool) -> str:
        if match:
            return home_uf
        others = [uf for uf in UF_WEIGHTS if uf != home_uf]
        return self.rng.choice(others)

    def _generate_single(
        self,
        *,
        account: dict[str, Any] | None = None,
        card: dict[str, Any] | None = None,
        created_at: datetime | None = None,
        uf_override: str | None = None,
        amount_factor: float = 1.0,
    ) -> dict[str, Any]:
        account = account or self._pick_account()
        account_cards = self._cards_by_account.get(account["account_id"], [])

        channel = self.rng.choices(
            [Channel.PIX, Channel.CREDIT, Channel.DEBIT, Channel.BOLETO],
            weights=[0.45, 0.25, 0.20, 0.10],
            k=1,
        )[0]
        if card is not None:
            channel = self.rng.choice([Channel.CREDIT, Channel.DEBIT])
        elif channel in (Channel.CREDIT, Channel.DEBIT):
            if account_cards:
                card = self.rng.choice(account_cards)
            else:
                channel = Channel.PIX  # conta sem cartão só transaciona PIX/boleto

        merchant = self.rng.choice(self.merchants)
        statuses, weights = zip(*_STATUS_WEIGHTS, strict=False)
        status = self.rng.choices(statuses, weights=weights, k=1)[0]
        decline_reason = None
        if status is TransactionStatus.DECLINED:
            reasons, r_weights = zip(*_DECLINE_WEIGHTS, strict=False)
            decline_reason = self.rng.choices(reasons, weights=r_weights, k=1)[0]

        created_at = created_at or transaction_datetime(
            self.rng, max_days=self.config.lookback_days, anchor=self.config.reference_time
        )
        authorized_at = None
        settled_at = None
        if status in (
            TransactionStatus.AUTHORIZED,
            TransactionStatus.SETTLED,
            TransactionStatus.REFUNDED,
        ):
            authorized_at = created_at + timedelta(seconds=self.rng.randint(1, 5))
        if status in (TransactionStatus.SETTLED, TransactionStatus.REFUNDED):
            assert authorized_at is not None
            settled_at = min(
                authorized_at + timedelta(seconds=self.rng.randint(60, 2 * 86_400)),
                self.config.reference_time,
            )

        amount = self._amount_lognormal(channel)
        if amount_factor != 1.0:
            amount = (amount * Decimal(str(amount_factor))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        home_uf = self._home_uf(account, card)
        uf_transacao = uf_override or self._pick_uf(
            home_uf, match=self.rng.random() < self.config.geo_match_rate
        )

        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_time": created_at,
            "transaction_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "account_id": account["account_id"],
            "card_id": card["card_id"] if card else None,
            "merchant_id": merchant["merchant_id"],
            "channel": channel.value,
            "amount": amount,
            "mcc": merchant["mcc"],
            "uf_transacao": uf_transacao,
            "status": status.value,
            "decline_reason": decline_reason.value if decline_reason else None,
            "fraud_flag": False,
            "fraud_rule": None,
            "created_at": created_at,
            "authorized_at": authorized_at,
            "settled_at": settled_at,
        }

    # ------------------------------------------------------------------ #
    # Fraud patterns (detectáveis pelo Flink CEP — módulo 4)
    # ------------------------------------------------------------------ #

    def _fraud_burst(self) -> list[dict[str, Any]]:
        rules, weights = zip(*_FRAUD_RULE_WEIGHTS, strict=False)
        rule = self.rng.choices(rules, weights=weights, k=1)[0]

        # Fraude de cartão precisa de conta com cartão
        accounts_with_cards = [
            a for a in self.accounts if self._cards_by_account.get(a["account_id"])
        ]
        if not accounts_with_cards:
            return [self._flag(self._generate_single(), FraudRule.RULE_AMOUNT_OUTLIER)]
        account = self.rng.choice(accounts_with_cards)
        card = self.rng.choice(self._cards_by_account[account["account_id"]])
        start = transaction_datetime(
            self.rng, max_days=self.config.lookback_days, anchor=self.config.reference_time
        )

        if rule is FraudRule.RULE_VELOCITY:
            # 3–4 transações no mesmo cartão em <60s
            n = self.rng.randint(3, 4)
            burst = []
            ts = start
            for _ in range(n):
                burst.append(
                    self._flag(
                        self._generate_single(account=account, card=card, created_at=ts), rule
                    )
                )
                ts = ts + timedelta(seconds=self.rng.randint(5, 15))
            return burst

        if rule is FraudRule.RULE_GEO_IMPOSSIBLE:
            # 2 UFs diferentes em <10min no mesmo cartão
            home_uf = str(card["uf_emissao"])
            far_uf = self.rng.choice([uf for uf in UF_WEIGHTS if uf != home_uf])
            first = self._generate_single(
                account=account, card=card, created_at=start, uf_override=home_uf
            )
            second = self._generate_single(
                account=account,
                card=card,
                created_at=start + timedelta(seconds=self.rng.randint(120, 540)),
                uf_override=far_uf,
            )
            return [self._flag(first, rule), self._flag(second, rule)]

        if rule is FraudRule.RULE_AMOUNT_OUTLIER:
            # Valor > 10× a média do cartão
            record = self._generate_single(
                account=account,
                card=card,
                created_at=start,
                amount_factor=self.rng.uniform(10.0, 20.0),
            )
            return [self._flag(record, rule)]

        # RULE_NIGHT_OWL: transação na madrugada (02h–05h)
        night = start.replace(hour=self.rng.randint(2, 5))
        record = self._generate_single(account=account, card=card, created_at=night)
        return [self._flag(record, rule)]

    @staticmethod
    def _flag(record: dict[str, Any], rule: FraudRule) -> dict[str, Any]:
        record["fraud_flag"] = True
        record["fraud_rule"] = rule.value
        return record

    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema for transaction records."""
        return load_schema("transaction")
