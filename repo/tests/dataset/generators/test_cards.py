"""Tests for CardGenerator — PAN masking is the critical policy here."""

from __future__ import annotations

import re
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from dataset.generators.payments import (
    CardGenerator,
    CardGeneratorConfig,
    CustomerGenerator,
    CustomerGeneratorConfig,
)
from dataset.generators.payments.accounts import AccountGenerator, AccountGeneratorConfig

_PAN_MASKED_RE = re.compile(r"^\d{6}\*{6}\d{4}$")


class TestCardGenerator:
    def test_deterministic_with_seed(
        self, accounts_pool: list[dict], customers_pool: list[dict]
    ) -> None:
        gen1 = CardGenerator(accounts_pool, customers_pool, CardGeneratorConfig(seed=42))
        gen2 = CardGenerator(accounts_pool, customers_pool, CardGeneratorConfig(seed=42))
        assert list(gen1.generate(20)) == list(gen2.generate(20))

    def test_pan_always_masked(self, cards_pool: list[dict]) -> None:
        """PAN nunca aparece completo: BIN(6) + ****** + last4."""
        for card in cards_pool:
            assert _PAN_MASKED_RE.match(card["pan_masked"]), card["pan_masked"]
            digits = card["pan_masked"].replace("*", "")
            assert len(digits) == 10  # BIN(6) + last4

    @settings(max_examples=25, deadline=None)
    @given(seed=st.integers(min_value=0, max_value=10_000))
    def test_pan_masked_property(self, seed: int) -> None:
        """Propriedade: para qualquer seed, todo PAN gerado sai mascarado."""
        customers = list(CustomerGenerator(CustomerGeneratorConfig(seed=seed)).generate(3))
        accounts = list(AccountGenerator(customers, AccountGeneratorConfig(seed=seed)).generate(3))
        cards = list(CardGenerator(accounts, customers, CardGeneratorConfig(seed=seed)).generate(5))
        for card in cards:
            assert "*" in card["pan_masked"]
            assert _PAN_MASKED_RE.match(card["pan_masked"])

    def test_referential_integrity(self, cards_pool: list[dict], accounts_pool: list[dict]) -> None:
        account_ids = {a["account_id"] for a in accounts_pool}
        for card in cards_pool:
            assert card["account_id"] in account_ids

    def test_uf_emissao_matches_holder(
        self,
        cards_pool: list[dict],
        accounts_pool: list[dict],
        customers_pool: list[dict],
    ) -> None:
        """UF de emissão = UF do endereço do titular (base das regras geo)."""
        account_by_id = {a["account_id"]: a for a in accounts_pool}
        uf_by_customer = {c["customer_id"]: c["endereco"]["uf"] for c in customers_pool}
        for card in cards_pool:
            holder = account_by_id[card["account_id"]]["customer_id"]
            assert card["uf_emissao"] == uf_by_customer[holder]

    def test_credit_limit_null_only_for_debit(self, cards_pool: list[dict]) -> None:
        for card in cards_pool:
            if card["card_type"] == "DEBIT":
                assert card["credit_limit"] is None
            else:
                assert isinstance(card["credit_limit"], Decimal)
                assert card["credit_limit"] > Decimal("0")

    def test_expiry_after_issue(self, cards_pool: list[dict]) -> None:
        for card in cards_pool:
            assert card["expires_at"] > card["issued_at"]
