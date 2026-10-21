"""Tests for CustomerGenerator."""

from __future__ import annotations

from datetime import timedelta

from dataset.generators.common.brazilian_formats import validate_cpf
from dataset.generators.common.temporal import REFERENCE_TIME
from dataset.generators.payments import CustomerGenerator, CustomerGeneratorConfig


class TestCustomerGenerator:
    def test_generates_requested_count(self, customers_pool: list[dict]) -> None:
        assert len(customers_pool) == 50

    def test_deterministic_with_seed(self) -> None:
        gen1 = CustomerGenerator(CustomerGeneratorConfig(seed=42))
        gen2 = CustomerGenerator(CustomerGeneratorConfig(seed=42))
        assert list(gen1.generate(10)) == list(gen2.generate(10))

    def test_different_seeds_differ(self) -> None:
        gen1 = CustomerGenerator(CustomerGeneratorConfig(seed=42))
        gen2 = CustomerGenerator(CustomerGeneratorConfig(seed=43))
        assert list(gen1.generate(10)) != list(gen2.generate(10))

    def test_cpf_is_valid_and_unformatted(self, customers_pool: list[dict]) -> None:
        for c in customers_pool:
            assert len(c["cpf"]) == 11
            assert c["cpf"].isdigit()
            assert validate_cpf(c["cpf"])

    def test_customers_are_adults(self, customers_pool: list[dict]) -> None:
        for c in customers_pool:
            age_years = (REFERENCE_TIME.date() - c["data_nascimento"]).days / 365.25
            assert 17.9 <= age_years <= 90.5

    def test_status_domain(self, customers_pool: list[dict]) -> None:
        assert {c["status"] for c in customers_pool} <= {"ACTIVE", "SUSPENDED", "CLOSED"}

    def test_temporal_consistency(self, customers_pool: list[dict]) -> None:
        for c in customers_pool:
            assert c["created_at"] <= c["updated_at"] <= REFERENCE_TIME + timedelta(seconds=1)

    def test_unique_ids_and_cpfs(self, customers_pool: list[dict]) -> None:
        ids = [c["customer_id"] for c in customers_pool]
        cpfs = [c["cpf"] for c in customers_pool]
        assert len(set(ids)) == len(ids)
        assert len(set(cpfs)) == len(cpfs)

    def test_schema_matches_record_fields(self, customers_pool: list[dict]) -> None:
        schema = CustomerGenerator().get_schema()
        schema_fields = {f["name"] for f in schema["fields"]}
        assert schema_fields == set(customers_pool[0].keys())
