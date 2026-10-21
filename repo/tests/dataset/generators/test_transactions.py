"""Tests for TransactionGenerator — distribuições, integridade e fraude injetada."""

from __future__ import annotations

import statistics
from datetime import timedelta
from decimal import Decimal

import pytest

from dataset.generators.payments import TransactionGenerator, TransactionGeneratorConfig


@pytest.fixture(scope="module")
def txn_generator(
    customers_pool: list[dict],
    accounts_pool: list[dict],
    cards_pool: list[dict],
    merchants_pool: list[dict],
) -> TransactionGenerator:
    return TransactionGenerator(
        customers_pool,
        accounts_pool,
        cards_pool,
        merchants_pool,
        TransactionGeneratorConfig(seed=42),
    )


@pytest.fixture(scope="module")
def transactions(txn_generator: TransactionGenerator) -> list[dict]:
    return list(txn_generator.generate(5_000))


class TestTransactionGenerator:
    def test_generates_exact_count(self, transactions: list[dict]) -> None:
        assert len(transactions) == 5_000

    def test_deterministic_with_seed(
        self,
        customers_pool: list[dict],
        accounts_pool: list[dict],
        cards_pool: list[dict],
        merchants_pool: list[dict],
    ) -> None:
        def build() -> list[dict]:
            gen = TransactionGenerator(
                customers_pool,
                accounts_pool,
                cards_pool,
                merchants_pool,
                TransactionGeneratorConfig(seed=42),
            )
            return list(gen.generate(200))

        assert build() == build()

    def test_referential_integrity(
        self,
        transactions: list[dict],
        accounts_pool: list[dict],
        cards_pool: list[dict],
        merchants_pool: list[dict],
    ) -> None:
        account_ids = {a["account_id"] for a in accounts_pool}
        card_ids = {c["card_id"] for c in cards_pool}
        merchant_ids = {m["merchant_id"] for m in merchants_pool}
        for t in transactions:
            assert t["account_id"] in account_ids
            assert t["card_id"] is None or t["card_id"] in card_ids
            assert t["merchant_id"] in merchant_ids

    def test_card_presence_matches_channel(self, transactions: list[dict]) -> None:
        """CREDIT/DEBIT exigem cartão; PIX/BOLETO nunca têm."""
        for t in transactions:
            if t["channel"] in ("CREDIT", "DEBIT"):
                assert t["card_id"] is not None
            else:
                assert t["card_id"] is None

    def test_amount_is_positive_decimal(self, transactions: list[dict]) -> None:
        for t in transactions:
            assert isinstance(t["amount"], Decimal)
            assert t["amount"] > Decimal("0")
            assert t["amount"] <= Decimal("10000") * 20  # clamp × outlier factor

    def test_amount_lognormal_median(self, transactions: list[dict]) -> None:
        """Mediana ~R$ 80 (log-normal — Domain Rules)."""
        legit = [float(t["amount"]) for t in transactions if not t["fraud_flag"]]
        assert 50 <= statistics.median(legit) <= 130

    def test_temporal_order(self, transactions: list[dict]) -> None:
        """created_at <= authorized_at <= settled_at (consistência temporal)."""
        for t in transactions:
            if t["authorized_at"] is not None:
                assert t["authorized_at"] >= t["created_at"]
            if t["settled_at"] is not None:
                assert t["authorized_at"] is not None
                assert t["settled_at"] >= t["authorized_at"]

    def test_status_timestamps_coherent(self, transactions: list[dict]) -> None:
        for t in transactions:
            if t["status"] in ("CREATED", "DECLINED"):
                assert t["authorized_at"] is None
                assert t["settled_at"] is None
            if t["status"] == "AUTHORIZED":
                assert t["authorized_at"] is not None
                assert t["settled_at"] is None
            if t["status"] in ("SETTLED", "REFUNDED"):
                assert t["settled_at"] is not None

    def test_decline_reason_only_when_declined(self, transactions: list[dict]) -> None:
        for t in transactions:
            assert (t["decline_reason"] is not None) == (t["status"] == "DECLINED")

    def test_approval_rate(self, transactions: list[dict]) -> None:
        """Taxa de aprovação 92–97% (Domain Rules)."""
        declined = sum(1 for t in transactions if t["status"] == "DECLINED")
        approval = 1 - declined / len(transactions)
        assert 0.92 <= approval <= 0.98

    def test_fraud_rate_within_domain_rule(self, transactions: list[dict]) -> None:
        """Fraude 0.5–1.5% das transações (Domain Rules)."""
        fraud = sum(1 for t in transactions if t["fraud_flag"])
        rate = fraud / len(transactions)
        assert 0.005 <= rate <= 0.015, f"fraud rate {rate:.4f} fora de 0.5–1.5%"

    def test_fraud_rule_only_when_flagged(self, transactions: list[dict]) -> None:
        for t in transactions:
            assert (t["fraud_rule"] is not None) == t["fraud_flag"]

    def test_geo_correlation(self, transactions: list[dict], cards_pool: list[dict]) -> None:
        """UF da transação bate com a UF de emissão em ≥85% (cartões, sem fraude geo)."""
        uf_by_card = {c["card_id"]: c["uf_emissao"] for c in cards_pool}
        card_txns = [
            t
            for t in transactions
            if t["card_id"] is not None and t["fraud_rule"] != "RULE_GEO_IMPOSSIBLE"
        ]
        matches = sum(1 for t in card_txns if t["uf_transacao"] == uf_by_card[t["card_id"]])
        assert matches / len(card_txns) >= 0.85

    def test_velocity_bursts_are_detectable(self, transactions: list[dict]) -> None:
        """RULE_VELOCITY emite 3+ transações no mesmo cartão em <60s (CEP do módulo 4)."""
        velocity = [t for t in transactions if t["fraud_rule"] == "RULE_VELOCITY"]
        assert velocity, "amostra de 5k deveria conter ao menos um burst de velocity"
        by_card: dict[str, list[dict]] = {}
        for t in velocity:
            by_card.setdefault(t["card_id"], []).append(t)
        found_burst = False
        for txns in by_card.values():
            txns.sort(key=lambda t: t["created_at"])
            for i in range(len(txns) - 2):
                if txns[i + 2]["created_at"] - txns[i]["created_at"] <= timedelta(seconds=60):
                    found_burst = True
        assert found_burst

    def test_geo_impossible_bursts_have_two_ufs(self, transactions: list[dict]) -> None:
        """RULE_GEO_IMPOSSIBLE: 2 UFs diferentes em <10min no mesmo cartão."""
        geo = [t for t in transactions if t["fraud_rule"] == "RULE_GEO_IMPOSSIBLE"]
        if not geo:
            pytest.skip("amostra sem burst geo (aceitável em amostras pequenas)")
        by_card: dict[str, list[dict]] = {}
        for t in geo:
            by_card.setdefault(t["card_id"], []).append(t)
        assert any(
            len({t["uf_transacao"] for t in txns}) >= 2
            and max(t["created_at"] for t in txns) - min(t["created_at"] for t in txns)
            <= timedelta(minutes=10)
            for txns in by_card.values()
            if len(txns) >= 2
        )

    def test_pareto_concentration(self, transactions: list[dict]) -> None:
        """Poucas contas concentram a maior parte do volume (Pareto)."""
        counts: dict[str, int] = {}
        for t in transactions:
            counts[t["account_id"]] = counts.get(t["account_id"], 0) + 1
        ordered = sorted(counts.values(), reverse=True)
        top20 = ordered[: max(1, len(ordered) // 5)]
        share = sum(top20) / len(transactions)
        assert share >= 0.5, f"top 20% das contas geram só {share:.0%} do volume"

    def test_requires_pools(self, customers_pool: list[dict]) -> None:
        with pytest.raises(ValueError, match="non-empty account and merchant pools"):
            TransactionGenerator(customers_pool, [], [], [])
