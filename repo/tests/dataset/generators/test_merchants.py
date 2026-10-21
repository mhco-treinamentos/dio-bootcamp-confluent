"""Tests for MerchantGenerator."""

from __future__ import annotations

from dataset.generators.common.brazilian_formats import UF_WEIGHTS, validate_cnpj
from dataset.generators.payments import MerchantGenerator, MerchantGeneratorConfig
from dataset.generators.payments.mcc import ALL_MCCS


class TestMerchantGenerator:
    def test_deterministic_with_seed(self) -> None:
        gen1 = MerchantGenerator(MerchantGeneratorConfig(seed=42))
        gen2 = MerchantGenerator(MerchantGeneratorConfig(seed=42))
        assert list(gen1.generate(10)) == list(gen2.generate(10))

    def test_cnpj_is_valid_and_unformatted(self, merchants_pool: list[dict]) -> None:
        for m in merchants_pool:
            assert len(m["cnpj"]) == 14
            assert m["cnpj"].isdigit()
            assert validate_cnpj(m["cnpj"])

    def test_mcc_is_4_digit_string_from_table(self, merchants_pool: list[dict]) -> None:
        known = {code for code, _ in ALL_MCCS}
        for m in merchants_pool:
            assert len(m["mcc"]) == 4
            assert m["mcc"] in known

    def test_uf_is_valid(self, merchants_pool: list[dict]) -> None:
        for m in merchants_pool:
            assert m["uf"] in UF_WEIGHTS
            assert m["endereco"]["uf"] == m["uf"]

    def test_top_mccs_dominate(self) -> None:
        """Top-10 MCCs concentram ~70% (Pareto de merchants — rules.md)."""
        from dataset.generators.payments.mcc import TOP_MCCS

        gen = MerchantGenerator(MerchantGeneratorConfig(seed=42))
        sample = list(gen.generate(500))
        top_codes = {code for code, _ in TOP_MCCS}
        top_share = sum(1 for m in sample if m["mcc"] in top_codes) / len(sample)
        assert 0.60 <= top_share <= 0.80
