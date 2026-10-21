"""Tests for the Neon seed SQL renderer."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from dataset.generators.payments import (
    AccountGenerator,
    AccountGeneratorConfig,
    CardGenerator,
    CardGeneratorConfig,
    CustomerGenerator,
    CustomerGeneratorConfig,
    MerchantGenerator,
    MerchantGeneratorConfig,
    TransactionGenerator,
    TransactionGeneratorConfig,
)
from dataset.neon.generate_seed import _sql, render_seed_sql


def _small_pools() -> dict[str, list[dict]]:
    customers = list(CustomerGenerator(CustomerGeneratorConfig(seed=42)).generate(5))
    accounts = list(AccountGenerator(customers, AccountGeneratorConfig(seed=42)).generate(6))
    cards = list(CardGenerator(accounts, customers, CardGeneratorConfig(seed=42)).generate(8))
    merchants = list(MerchantGenerator(MerchantGeneratorConfig(seed=42)).generate(3))
    transactions = list(
        TransactionGenerator(
            customers, accounts, cards, merchants, TransactionGeneratorConfig(seed=42)
        ).generate(20)
    )
    return {
        "customers": customers,
        "accounts": accounts,
        "cards": cards,
        "merchants": merchants,
        "transactions": transactions,
    }


class TestSqlLiteral:
    def test_escapes_single_quotes(self) -> None:
        assert _sql("D'Ávila e Filhos") == "'D''Ávila e Filhos'"

    def test_null_bool_decimal_datetime(self) -> None:
        assert _sql(None) == "NULL"
        assert _sql(True) == "true"
        assert _sql(False) == "false"
        assert _sql(Decimal("80.5")) == "80.50"
        dt = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
        assert _sql(dt) == "'2026-07-01T12:00:00+00:00'"


class TestRenderSeedSql:
    def test_renders_idempotent_transactional_script(self) -> None:
        sql = render_seed_sql(_small_pools())
        assert sql.count("BEGIN;") == 1
        assert sql.count("COMMIT;") == 1
        # Idempotente: toda tabela com ON CONFLICT DO NOTHING
        for table in ("customers", "accounts", "cards", "merchants", "transactions"):
            assert f"INSERT INTO payments.{table}" in sql
        assert sql.count("ON CONFLICT (") == 5  # uma cláusula por tabela (header não conta)

    def test_deterministic_render(self) -> None:
        assert render_seed_sql(_small_pools()) == render_seed_sql(_small_pools())

    def test_no_cleartext_pan(self) -> None:
        """Nenhum número de 13–19 dígitos contíguos (PAN em claro) no seed."""
        import re

        sql = render_seed_sql(_small_pools())
        # Telefones ('+55' + 11 dígitos) são legítimos — remove antes de varrer
        sql = re.sub(r"\+55\d{11}", "", sql)
        for match in re.finditer(r"\d{13,19}", sql):
            # Única sequência ≥13 dígitos permitida é CNPJ (14)
            snippet = match.group()
            assert len(snippet) == 14, f"sequência suspeita de PAN em claro: {snippet}"
