"""Deterministic generator for Merchant (estabelecimento) master records."""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

from dataset.generators.base import BaseGenerator
from dataset.generators.common.brazilian_formats import generate_cep, generate_cnpj, pick_uf
from dataset.generators.common.temporal import REFERENCE_TIME, past_datetime
from dataset.generators.payments._schema import load_schema
from dataset.generators.payments.enums import MerchantStatus
from dataset.generators.payments.mcc import pick_mcc

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MerchantGeneratorConfig:
    """Configuration for :class:`MerchantGenerator`.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    locale : str
        Faker locale for company names/addresses.
    reference_time : datetime
        Fixed "now" anchor for all timestamps.
    """

    seed: int = 42
    locale: str = "pt_BR"
    reference_time: datetime = REFERENCE_TIME


class MerchantGenerator(BaseGenerator):
    """Generate synthetic merchants with valid CNPJ and weighted MCCs."""

    def __init__(self, config: MerchantGeneratorConfig | None = None) -> None:
        self.config = config or MerchantGeneratorConfig()
        self.rng = random.Random(self.config.seed)
        self.fake = Faker(self.config.locale)
        self.fake.seed_instance(self.config.seed)

    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield ``count`` merchant records.

        Parameters
        ----------
        count : int
            Number of merchants to generate.

        Yields
        ------
        dict[str, Any]
            Merchant record matching ``schemas/payments/merchant.avsc``.
        """
        for _ in range(count):
            yield self._generate_single()
        logger.info("Generated %d merchant records", count)

    def _generate_single(self) -> dict[str, Any]:
        mcc, _description = pick_mcc(self.rng)
        created_at = past_datetime(
            self.rng, max_days=2920, min_days=30, anchor=self.config.reference_time
        )
        status = self.rng.choices(
            [MerchantStatus.ACTIVE, MerchantStatus.SUSPENDED, MerchantStatus.CLOSED],
            weights=[0.95, 0.03, 0.02],
            k=1,
        )[0]
        uf = pick_uf(self.rng)
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_time": created_at,
            "merchant_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "cnpj": generate_cnpj(self.rng),
            "nome_fantasia": self.fake.company(),
            "mcc": mcc,
            "uf": uf,
            "endereco": {
                "logradouro": self.fake.street_name(),
                "numero": str(self.rng.randint(1, 9999)),
                "complemento": "",
                "bairro": self.fake.bairro(),
                "cidade": self.fake.city(),
                "uf": uf,
                "cep": generate_cep(self.rng),
                "pais": "BR",
            },
            "status": status.value,
            "created_at": created_at,
            "updated_at": min(
                created_at + timedelta(seconds=self.rng.randint(0, 90 * 86_400)),
                self.config.reference_time,
            ),
        }

    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema for merchant records."""
        return load_schema("merchant")
