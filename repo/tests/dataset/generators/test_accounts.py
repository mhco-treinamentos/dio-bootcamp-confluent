"""Tests for AccountGenerator."""

from __future__ import annotations

from decimal import Decimal

import pytest

from dataset.generators.payments import AccountGenerator, AccountGeneratorConfig


class TestAccountGenerator:
    def test_requires_customer_pool(self) -> None:
        with pytest.raises(ValueError, match="non-empty customer pool"):
            AccountGenerator([])

    def test_deterministic_with_seed(self, customers_pool: list[dict]) -> None:
        gen1 = AccountGenerator(customers_pool, AccountGeneratorConfig(seed=42))
        gen2 = AccountGenerator(customers_pool, AccountGeneratorConfig(seed=42))
        assert list(gen1.generate(20)) == list(gen2.generate(20))

    def test_referential_integrity(
        self, accounts_pool: list[dict], customers_pool: list[dict]
    ) -> None:
        customer_ids = {c["customer_id"] for c in customers_pool}
        for a in accounts_pool:
            assert a["customer_id"] in customer_ids

    def test_every_customer_covered_when_count_allows(
        self, accounts_pool: list[dict], customers_pool: list[dict]
    ) -> None:
        """count (60) >= pool (50) → todo customer tem pelo menos uma conta."""
        with_account = {a["customer_id"] for a in accounts_pool}
        assert with_account == {c["customer_id"] for c in customers_pool}

    def test_money_is_decimal_never_float(self, accounts_pool: list[dict]) -> None:
        for a in accounts_pool:
            assert isinstance(a["balance"], Decimal)
            assert isinstance(a["overdraft_limit"], Decimal)
            assert a["balance"] >= Decimal("0")
            assert a["overdraft_limit"] >= Decimal("0")
            # 2 casas decimais
            assert a["balance"] == a["balance"].quantize(Decimal("0.01"))

    def test_agencia_conta_formats(self, accounts_pool: list[dict]) -> None:
        for a in accounts_pool:
            assert len(a["agencia"]) == 4 and a["agencia"].isdigit()
            assert len(a["conta"]) == 11 and a["conta"].isdigit()

    def test_closed_at_only_when_closed(self, accounts_pool: list[dict]) -> None:
        for a in accounts_pool:
            if a["status"] == "CLOSED":
                assert a["closed_at"] is not None
                assert a["closed_at"] >= a["opened_at"]
            else:
                assert a["closed_at"] is None

    def test_tier_domain(self, accounts_pool: list[dict]) -> None:
        assert {a["tier"] for a in accounts_pool} <= {"BASIC", "PLUS", "PREMIUM"}
