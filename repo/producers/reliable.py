"""04 · Producer robusto — o que vai para produção (e para o Desafio Final).

O topo da progressão: tudo do 03 (Avro + Schema Registry) mais o que separa
demo de produção:

- **idempotência** (``enable.idempotence=True`` + ``acks=all``) — retry do
  próprio librdkafka não duplica (Curso 1, Aula 5)
- **checagem de compatibilidade** antes do primeiro produce — o schema é
  testado contra o subject; incompatível falha rápido, não em produção
- **métricas de entrega** thread-safe (entregues/falhas/taxa de sucesso)
- **key explícita** — a escolha que decide partição e ordem por entidade

Config de conexão e profile vivem em :mod:`producers.config`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from producers.config import build_producer_config, build_schema_registry_config

if TYPE_CHECKING:
    from confluent_kafka import Message

logger = logging.getLogger(__name__)


@dataclass
class ProducerMetrics:
    """Métricas de entrega coletadas via callback do producer."""

    delivered: int = 0
    failed: int = 0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def on_delivery(self, err: object, msg: Message) -> None:
        """Callback de entrega: contabiliza sucesso/falha.

        Parameters
        ----------
        err : object
            Erro do Kafka, ou None em caso de sucesso.
        msg : Message
            A mensagem produzida.
        """
        with self._lock:
            if err is not None:
                self.failed += 1
                logger.error("Delivery failed: %s", err)
            else:
                self.delivered += 1
                logger.debug("Delivered to %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())

    @property
    def success_rate(self) -> float:
        """Fração de mensagens entregues com sucesso (1.0 se nada foi enviado)."""
        total = self.delivered + self.failed
        return self.delivered / total if total > 0 else 1.0


class AvroRecordProducer:
    """Produz records dict como Avro, integrado ao Schema Registry.

    Embrulha um par ``confluent_kafka.Producer`` + ``AvroSerializer``. O schema
    de valor é testado contra o subject antes do primeiro produce (regra:
    sempre checar compatibilidade antes de publicar).

    Parameters
    ----------
    topic : str
        Topic de destino (convenção de nome: .claude/rules.md).
    schema : dict[str, Any]
        Schema Avro dos valores (ex.: ``generator.get_schema()``).
    key_field : str
        Campo do record usado como key da mensagem (ex.: ``"account_id"`` —
        a escolha da key define particionamento e ordem por entidade).
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.
    producer_overrides : dict[str, Any] or None, optional
        Settings librdkafka extras.
    """

    def __init__(
        self,
        topic: str,
        schema: dict[str, Any],
        key_field: str,
        profile: str | None = None,
        producer_overrides: dict[str, Any] | None = None,
    ) -> None:
        self.topic = topic
        self.key_field = key_field
        self.metrics = ProducerMetrics()

        schema_str = json.dumps(schema)
        self._sr_client = SchemaRegistryClient(build_schema_registry_config(profile))
        self._ensure_compatibility(schema_str)
        self._value_serializer = AvroSerializer(
            self._sr_client, schema_str, to_dict=lambda record, ctx: record
        )
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer(build_producer_config(profile, overrides=producer_overrides))

    def _ensure_compatibility(self, schema_str: str) -> None:
        """Falha rápido se o schema for incompatível com o subject (BACKWARD)."""
        from confluent_kafka.schema_registry import Schema

        subject = f"{self.topic}-value"
        try:
            versions = self._sr_client.get_versions(subject)
        except Exception:  # noqa: BLE001 — subject ainda não existe: primeiro registro
            logger.info("Subject %s not found — first registration", subject)
            return
        if versions and not self._sr_client.test_compatibility(subject, Schema(schema_str, "AVRO")):
            raise ValueError(
                f"Schema is not compatible with subject {subject} "
                "(compatibilidade BACKWARD — ver .claude/rules/schemas.md)"
            )

    def produce(self, record: dict[str, Any]) -> None:
        """Serializa e produz um único record.

        Parameters
        ----------
        record : dict[str, Any]
            Record aderente ao schema; ``record[key_field]`` vira a key.
        """
        key = str(record[self.key_field])
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        self._producer.produce(
            topic=self.topic,
            key=self._key_serializer(key, SerializationContext(self.topic, MessageField.KEY)),
            value=self._value_serializer(record, ctx),
            callback=self.metrics.on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Descarrega mensagens pendentes e loga o resumo de entrega.

        Parameters
        ----------
        timeout : float, optional
            Segundos máximos de espera, by default 30.

        Returns
        -------
        int
            Mensagens ainda pendentes após o timeout.
        """
        pending = int(self._producer.flush(timeout))
        logger.info(
            "Flush done: delivered=%d failed=%d pending=%d",
            self.metrics.delivered,
            self.metrics.failed,
            pending,
        )
        return pending
