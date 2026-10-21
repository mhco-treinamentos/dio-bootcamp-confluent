"""Poison pill injector — venenos *semânticos* sobre o stream de transações.

Implementa os pills da reference .claude/references/poison-pill-patterns.md
que são **Avro-válidos** (deserializam bem, mas exigem tratamento): duplicata,
near-duplicate, atrasado, campo nulo, malformado semântico, órfão, valor
extremo e timezone. O destino pedagógico desses pills é o **Flink** (Curso 4):
dedup, watermark, quality gate e temporal join — ver ``confluent_cloud/flink/``.

Venenos de *serialização* (bytes que nem são Avro — o que quebra o converter
do sink e cai na DLQ do Connect, Curso 3 Aula 4) não vivem aqui: um record
dict não representa bytes inválidos. Ver ``producers/poison``.

Política inegociável (PCI): PAN em claro NUNCA é um pill válido — o injetor
bloqueia na saída, mesmo em dado sintético.
"""

from __future__ import annotations

import copy
import logging
import random
import re
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

#: Sequência de 13–19 dígitos contíguos = possível PAN em claro (bloqueado).
_CLEARTEXT_PAN_RE = re.compile(r"(?<!\d)\d{13,19}(?!\d)")

#: Campos seguros para anular numa Transaction (não quebram Avro: são nullable).
_NULLABLE_FIELDS = ("merchant_id", "settled_at", "authorized_at")


class PoisonPillType(str, Enum):
    """Tipos de veneno semântico injetáveis no stream de transações."""

    DUPLICATE = "DUPLICATE"
    NEAR_DUPLICATE = "NEAR_DUPLICATE"
    LATE_ARRIVING = "LATE_ARRIVING"
    NULL_FIELD = "NULL_FIELD"
    MALFORMED_SEMANTIC = "MALFORMED_SEMANTIC"
    ORPHAN = "ORPHAN"
    EXTREME_VALUE = "EXTREME_VALUE"
    TIMEZONE_MISMATCH = "TIMEZONE_MISMATCH"


@dataclass(frozen=True)
class PoisonPillConfig:
    """Taxas de injeção por tipo (probabilidade por registro).

    Defaults = perfil "moderate" da reference poison-pill-patterns.md.

    Attributes
    ----------
    seed : int
        Determinism seed (labs default: 42).
    duplicate_rate : float
        Cópia exata emitida logo após o original (testa dedup).
    near_duplicate_rate : float
        Mesmo ``transaction_id``, ``event_time`` ±1s (dedup por ID vs hash).
    late_arriving_rate : float
        ``event_time`` recuado 1–72h (testa watermark/late data).
    null_field_rate : float
        Campo nullable zerado (testa COALESCE/NOT NULL no destino).
    malformed_rate : float
        Valor fora do formato: mcc inválido, UF inexistente (quality gate).
    orphan_rate : float
        FK trocada por referência inexistente (testa join/integridade).
    extreme_value_rate : float
        ``amount`` no limite ou além do domínio (testa range check).
    timezone_rate : float
        ``event_time`` deslocado ±3h — confusão UTC↔BRT (normalização).
    """

    seed: int = 42
    duplicate_rate: float = 0.02
    near_duplicate_rate: float = 0.01
    late_arriving_rate: float = 0.05
    null_field_rate: float = 0.01
    malformed_rate: float = 0.005
    orphan_rate: float = 0.002
    extreme_value_rate: float = 0.01
    timezone_rate: float = 0.01


@dataclass
class Injection:
    """Registro de auditoria de um veneno injetado (para validar labs)."""

    pill: PoisonPillType
    transaction_id: str
    detail: str = ""


class PoisonPillInjector:
    """Injeta venenos semânticos num stream de records de Transaction.

    Determinístico: mesmo seed + mesmo stream de entrada → mesmos venenos.
    Todo veneno é registrado em :attr:`injections` (audit trail) — é assim
    que o lab prova "N pills entraram, N pills foram tratadas".

    Parameters
    ----------
    config : PoisonPillConfig or None, optional
        Taxas de injeção; defaults do perfil "moderate".

    Examples
    --------
    >>> injector = PoisonPillInjector()
    >>> poisoned = list(injector.inject(generator.generate(1000)))
    >>> len(injector.injections)  # quantos venenos entraram
    """

    def __init__(self, config: PoisonPillConfig | None = None) -> None:
        self.config = config or PoisonPillConfig()
        self.rng = random.Random(self.config.seed)
        self.injections: list[Injection] = []

    def inject(self, records: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        """Yield o stream original com venenos injetados no caminho.

        Mutações (late/null/malformed/orphan/extreme/timezone) substituem o
        registro; duplicatas são emitidas LOGO APÓS o original (mesma key →
        mesma partição, como um retry de producer real faria).

        Parameters
        ----------
        records : Iterable[dict[str, Any]]
            Stream de records de Transaction (ex.: ``TransactionGenerator``).

        Yields
        ------
        dict[str, Any]
            Records originais, mutados e duplicatas — todos Avro-válidos.
        """
        for record in records:
            record = self._maybe_mutate(record)
            self._assert_no_cleartext_pan(record)
            yield record

            duplicate = self._maybe_duplicate(record)
            if duplicate is not None:
                yield duplicate

    # ------------------------------------------------------------------ #
    # Mutações (1 registro → 1 registro alterado)
    # ------------------------------------------------------------------ #

    def _maybe_mutate(self, record: dict[str, Any]) -> dict[str, Any]:
        roll = self.rng.random()
        cfg = self.config
        threshold = 0.0

        for rate, mutator in (
            (cfg.late_arriving_rate, self._make_late),
            (cfg.null_field_rate, self._null_field),
            (cfg.malformed_rate, self._malform),
            (cfg.orphan_rate, self._orphan),
            (cfg.extreme_value_rate, self._extreme_value),
            (cfg.timezone_rate, self._shift_timezone),
        ):
            threshold += rate
            if roll < threshold:
                return mutator(copy.deepcopy(record))
        return record

    def _make_late(self, record: dict[str, Any]) -> dict[str, Any]:
        """Evento atrasado: event_time recua 1–72h (chega 'agora', ocorreu antes)."""
        delay = timedelta(hours=self.rng.randint(1, 72))
        record["event_time"] = record["event_time"] - delay
        record["created_at"] = record["created_at"] - delay
        self._track(PoisonPillType.LATE_ARRIVING, record, f"delay={delay}")
        return record

    def _null_field(self, record: dict[str, Any]) -> dict[str, Any]:
        target = self.rng.choice(_NULLABLE_FIELDS)
        record[target] = None
        self._track(PoisonPillType.NULL_FIELD, record, f"field={target}")
        return record

    def _malform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Formato inválido que o Avro aceita (string/decimal) mas o negócio não."""
        which = self.rng.choice(["mcc", "uf", "amount_zero"])
        if which == "mcc":
            record["mcc"] = self.rng.choice(["INVA", "99", "0000"])
        elif which == "uf":
            record["uf_transacao"] = self.rng.choice(["XX", "ZZ", "BR"])
        else:
            record["amount"] = Decimal("0.00")  # viola CHECK (amount > 0) do destino
        self._track(PoisonPillType.MALFORMED_SEMANTIC, record, f"which={which}")
        return record

    def _orphan(self, record: dict[str, Any]) -> dict[str, Any]:
        """FK para pai inexistente — o temporal join do Flink não vai casar."""
        fk = self.rng.choice(["account_id", "merchant_id"])
        record[fk] = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        self._track(PoisonPillType.ORPHAN, record, f"fk={fk}")
        return record

    def _extreme_value(self, record: dict[str, Any]) -> dict[str, Any]:
        record["amount"] = Decimal(self.rng.choice(["0.01", "999999.99", "500000.00", "-999.99"]))
        self._track(PoisonPillType.EXTREME_VALUE, record, f"amount={record['amount']}")
        return record

    def _shift_timezone(self, record: dict[str, Any]) -> dict[str, Any]:
        """±3h — o clássico UTC↔BRT gravado sem timezone na fonte."""
        offset = timedelta(hours=self.rng.choice([-3, 3]))
        record["event_time"] = record["event_time"] + offset
        record["created_at"] = record["created_at"] + offset
        self._track(PoisonPillType.TIMEZONE_MISMATCH, record, f"offset={offset}")
        return record

    # ------------------------------------------------------------------ #
    # Duplicatas (1 registro → registro extra)
    # ------------------------------------------------------------------ #

    def _maybe_duplicate(self, record: dict[str, Any]) -> dict[str, Any] | None:
        roll = self.rng.random()
        if roll < self.config.duplicate_rate:
            clone = copy.deepcopy(record)
            self._track(PoisonPillType.DUPLICATE, clone)
            return clone
        if roll < self.config.duplicate_rate + self.config.near_duplicate_rate:
            clone = copy.deepcopy(record)
            delta = timedelta(seconds=self.rng.choice([-1, 1]))
            clone["event_time"] = clone["event_time"] + delta
            # event_id novo: near-duplicate real (retry que re-gerou envelope)
            clone["event_id"] = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
            self._track(PoisonPillType.NEAR_DUPLICATE, clone, f"delta={delta}")
            return clone
        return None

    # ------------------------------------------------------------------ #
    # Guard-rails e auditoria
    # ------------------------------------------------------------------ #

    @staticmethod
    def _assert_no_cleartext_pan(record: dict[str, Any]) -> None:
        """PAN em claro é PROIBIDO mesmo como veneno — bloqueia na saída.

        Raises
        ------
        ValueError
            Se qualquer valor string contiver 13–19 dígitos contíguos.
        """
        for key, value in record.items():
            if isinstance(value, str) and _CLEARTEXT_PAN_RE.search(value):
                raise ValueError(
                    f"Poison pill bloqueado: possível PAN em claro no campo {key!r} "
                    "(política PCI — .claude/rules/schemas.md)"
                )

    def _track(self, pill: PoisonPillType, record: dict[str, Any], detail: str = "") -> None:
        self.injections.append(Injection(pill, record["transaction_id"], detail))
        logger.debug(
            "Injected %s into transaction %s (%s)", pill.value, record["transaction_id"], detail
        )

    def summary(self) -> dict[str, int]:
        """Contagem de venenos injetados por tipo (para o audit do lab).

        Returns
        -------
        dict[str, int]
            ``{pill_type: count}`` — vazio se nada foi injetado.
        """
        counts: dict[str, int] = {}
        for injection in self.injections:
            counts[injection.pill.value] = counts.get(injection.pill.value, 0) + 1
        return counts
