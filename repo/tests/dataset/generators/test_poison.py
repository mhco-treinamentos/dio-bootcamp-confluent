"""Tests for PoisonPillInjector — determinismo, taxas, auditoria e guarda PCI."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from dataset.generators.payments import (
    PoisonPillConfig,
    PoisonPillInjector,
    PoisonPillType,
    TransactionGenerator,
    TransactionGeneratorConfig,
)


@pytest.fixture(scope="module")
def clean_stream(
    customers_pool: list[dict],
    accounts_pool: list[dict],
    cards_pool: list[dict],
    merchants_pool: list[dict],
) -> list[dict]:
    gen = TransactionGenerator(
        customers_pool,
        accounts_pool,
        cards_pool,
        merchants_pool,
        # fraude desligada: isola o efeito dos pills
        TransactionGeneratorConfig(seed=42, fraud_burst_rate=0.0),
    )
    return list(gen.generate(5_000))


@pytest.fixture(scope="module")
def poisoned(clean_stream: list[dict]) -> tuple[list[dict], PoisonPillInjector]:
    injector = PoisonPillInjector(PoisonPillConfig(seed=42))
    return list(injector.inject(clean_stream)), injector


class TestDeterminism:
    def test_same_seed_same_poison(self, clean_stream: list[dict]) -> None:
        run1 = list(PoisonPillInjector(PoisonPillConfig(seed=42)).inject(clean_stream[:500]))
        run2 = list(PoisonPillInjector(PoisonPillConfig(seed=42)).inject(clean_stream[:500]))
        assert run1 == run2

    def test_audit_is_deterministic(self, clean_stream: list[dict]) -> None:
        inj1 = PoisonPillInjector(PoisonPillConfig(seed=42))
        inj2 = PoisonPillInjector(PoisonPillConfig(seed=42))
        list(inj1.inject(clean_stream[:500]))
        list(inj2.inject(clean_stream[:500]))
        assert inj1.summary() == inj2.summary()


class TestInjectionEffects:
    def test_stream_grows_only_by_duplicates(
        self, poisoned: tuple[list[dict], PoisonPillInjector], clean_stream: list[dict]
    ) -> None:
        records, injector = poisoned
        summary = injector.summary()
        dupes = summary.get("DUPLICATE", 0) + summary.get("NEAR_DUPLICATE", 0)
        assert len(records) == len(clean_stream) + dupes

    def test_duplicates_share_transaction_id(
        self, poisoned: tuple[list[dict], PoisonPillInjector]
    ) -> None:
        records, injector = poisoned
        dup_ids = {
            i.transaction_id
            for i in injector.injections
            if i.pill in (PoisonPillType.DUPLICATE, PoisonPillType.NEAR_DUPLICATE)
        }
        assert dup_ids
        for dup_id in dup_ids:
            assert sum(1 for r in records if r["transaction_id"] == dup_id) >= 2

    def test_exact_duplicates_are_identical(
        self, poisoned: tuple[list[dict], PoisonPillInjector]
    ) -> None:
        records, injector = poisoned
        exact_ids = {
            i.transaction_id for i in injector.injections if i.pill is PoisonPillType.DUPLICATE
        }
        by_id: dict[str, list[dict]] = {}
        for r in records:
            if r["transaction_id"] in exact_ids:
                by_id.setdefault(r["transaction_id"], []).append(r)
        for copies in by_id.values():
            assert copies[0] == copies[1]

    def test_late_events_moved_to_past(
        self, poisoned: tuple[list[dict], PoisonPillInjector], clean_stream: list[dict]
    ) -> None:
        records, injector = poisoned
        original_by_id = {r["transaction_id"]: r for r in clean_stream}
        late_ids = [
            i.transaction_id for i in injector.injections if i.pill is PoisonPillType.LATE_ARRIVING
        ]
        assert late_ids
        poisoned_by_id = {r["transaction_id"]: r for r in records}
        for late_id in late_ids:
            delta = original_by_id[late_id]["event_time"] - poisoned_by_id[late_id]["event_time"]
            assert timedelta(hours=1) <= delta <= timedelta(hours=72)

    def test_malformed_and_extreme_flow_to_quality_gate(
        self, poisoned: tuple[list[dict], PoisonPillInjector]
    ) -> None:
        """Todo pill semântico deve ser barrável pelas regras do 02_quality_gate.sql."""
        records, injector = poisoned
        flagged_ids = {
            i.transaction_id
            for i in injector.injections
            if i.pill in (PoisonPillType.MALFORMED_SEMANTIC, PoisonPillType.EXTREME_VALUE)
        }
        assert flagged_ids
        valid_ufs = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        }
        caught = 0
        for r in records:
            if r["transaction_id"] not in flagged_ids:
                continue
            gate_fails = (
                r["amount"] <= Decimal("0")
                or r["amount"] > Decimal("100000.00")
                or not (len(r["mcc"]) == 4 and r["mcc"].isdigit())
                or r["uf_transacao"] not in valid_ufs
            )
            if gate_fails:
                caught += 1
        # extreme "0.01"/"500000"? 0.01 e 999999.99/500000 — 0.01 passa no gate (é válido);
        # por isso caught >= 60% dos flagged, não 100%
        assert caught >= 0.6 * len(flagged_ids)

    def test_orphans_reference_unknown_parents(
        self,
        poisoned: tuple[list[dict], PoisonPillInjector],
        accounts_pool: list[dict],
        merchants_pool: list[dict],
    ) -> None:
        records, injector = poisoned
        orphan_ids = {
            i.transaction_id for i in injector.injections if i.pill is PoisonPillType.ORPHAN
        }
        if not orphan_ids:
            pytest.skip("amostra sem órfão (taxa 0.2%)")
        known = {a["account_id"] for a in accounts_pool} | {
            m["merchant_id"] for m in merchants_pool
        }
        for r in records:
            if r["transaction_id"] in orphan_ids:
                assert r["account_id"] not in known or r["merchant_id"] not in known


class TestRates:
    def test_rates_within_tolerance(
        self, poisoned: tuple[list[dict], PoisonPillInjector], clean_stream: list[dict]
    ) -> None:
        """Taxas observadas ~= configuradas (tolerância 50% — amostra de 5k)."""
        _, injector = poisoned
        total = len(clean_stream)
        summary = injector.summary()
        cfg = PoisonPillConfig()
        for pill, expected in (
            ("DUPLICATE", cfg.duplicate_rate),
            ("LATE_ARRIVING", cfg.late_arriving_rate),
            ("EXTREME_VALUE", cfg.extreme_value_rate),
        ):
            observed = summary.get(pill, 0) / total
            assert (
                expected * 0.5 <= observed <= expected * 1.5
            ), f"{pill}: esperado ~{expected}, observado {observed:.4f}"


class TestPCIGuard:
    def test_cleartext_pan_is_blocked(self) -> None:
        """PAN em claro NUNCA sai do injetor — nem como veneno."""
        injector = PoisonPillInjector()
        poisoned_record = {
            "transaction_id": "t-1",
            "note": "4017123412341234",  # 16 dígitos contíguos
        }
        with pytest.raises(ValueError, match="PAN em claro"):
            list(injector.inject([poisoned_record]))

    def test_masked_pan_passes(self) -> None:
        injector = PoisonPillInjector(
            PoisonPillConfig(seed=42, duplicate_rate=0, near_duplicate_rate=0)
        )
        record = {"transaction_id": "t-1", "pan_masked": "401712******4242"}
        # deepcopy-safe: sem mutação (taxas zeradas), o record passa intacto
        out = list(
            PoisonPillInjector(
                PoisonPillConfig(
                    seed=42,
                    duplicate_rate=0,
                    near_duplicate_rate=0,
                    late_arriving_rate=0,
                    null_field_rate=0,
                    malformed_rate=0,
                    orphan_rate=0,
                    extreme_value_rate=0,
                    timezone_rate=0,
                )
            ).inject([record])
        )
        assert out == [record]
        assert injector.summary() == {}
