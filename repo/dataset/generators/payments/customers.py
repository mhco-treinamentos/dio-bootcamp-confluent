"""Deterministic generator for Customer (pessoa física) master records."""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from faker import Faker

from dataset.generators.base import BaseGenerator
from dataset.generators.common.brazilian_formats import generate_cep, generate_cpf, generate_phone
from dataset.generators.common.temporal import REFERENCE_TIME, past_datetime
from dataset.generators.payments._schema import load_schema
from dataset.generators.payments.enums import CustomerStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CustomerGeneratorConfig:
    """Configuration for :class:`CustomerGenerator`.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    locale : str
        Faker locale for names/addresses.
    reference_time : datetime
        Fixed "now" anchor for all timestamps.
    """

    seed: int = 42
    locale: str = "pt_BR"
    reference_time: datetime = REFERENCE_TIME


class CustomerGenerator(BaseGenerator):
    """Generate synthetic bank customers with valid Brazilian PII formats.

    PII is synthetic by construction (Faker + check-digit generators) and
    consistent per run: same seed → same customers.
    """

    _STATUS_WEIGHTS = (
        (CustomerStatus.ACTIVE, 0.96),
        (CustomerStatus.SUSPENDED, 0.03),
        (CustomerStatus.CLOSED, 0.01),
    )

    def __init__(self, config: CustomerGeneratorConfig | None = None) -> None:
        self.config = config or CustomerGeneratorConfig()
        self.rng = random.Random(self.config.seed)
        self.fake = Faker(self.config.locale)
        self.fake.seed_instance(self.config.seed)

    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield ``count`` customer records.

        Parameters
        ----------
        count : int
            Number of customers to generate.

        Yields
        ------
        dict[str, Any]
            Customer record matching ``schemas/payments/customer.avsc``.
        """
        for _ in range(count):
            yield self._generate_single()
        logger.info("Generated %d customer records", count)

    def _generate_single(self) -> dict[str, Any]:
        created_at = past_datetime(
            self.rng, max_days=3650, min_days=30, anchor=self.config.reference_time
        )
        status = self.rng.choices(
            [s for s, _ in self._STATUS_WEIGHTS],
            weights=[w for _, w in self._STATUS_WEIGHTS],
            k=1,
        )[0]
        uf = self.fake.estado_sigla()
        birth = self._birth_date()
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_time": created_at,
            "customer_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "cpf": generate_cpf(self.rng),
            "nome": self.fake.name(),
            "data_nascimento": birth,
            "email": self.fake.email(),
            "telefone": generate_phone(self.rng),
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

    def _birth_date(self) -> date:
        """Random birth date for an adult (18–90 years before the anchor)."""
        age_days = self.rng.randint(18 * 365, 90 * 365)
        return (self.config.reference_time - timedelta(days=age_days)).date()

    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema for customer records."""
        return load_schema("customer")
