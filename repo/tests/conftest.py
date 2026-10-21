"""Shared fixtures — deterministic pools with seed=42 (see .claude/rules/testing.md).

Generator tests never touch Confluent Cloud or Neon: everything runs in-memory.
"""

from __future__ import annotations

import pytest

from dataset.generators.payments import (
    AccountGenerator,
    AccountGeneratorConfig,
    CardGenerator,
    CardGeneratorConfig,
    CustomerGenerator,
    CustomerGeneratorConfig,
    MerchantGenerator,
    MerchantGeneratorConfig,
)

SEED = 42


@pytest.fixture(scope="session")
def customers_pool() -> list[dict]:
    return list(CustomerGenerator(CustomerGeneratorConfig(seed=SEED)).generate(50))


@pytest.fixture(scope="session")
def accounts_pool(customers_pool: list[dict]) -> list[dict]:
    return list(AccountGenerator(customers_pool, AccountGeneratorConfig(seed=SEED)).generate(60))


@pytest.fixture(scope="session")
def cards_pool(accounts_pool: list[dict], customers_pool: list[dict]) -> list[dict]:
    return list(
        CardGenerator(accounts_pool, customers_pool, CardGeneratorConfig(seed=SEED)).generate(80)
    )


@pytest.fixture(scope="session")
def merchants_pool() -> list[dict]:
    return list(MerchantGenerator(MerchantGeneratorConfig(seed=SEED)).generate(20))
