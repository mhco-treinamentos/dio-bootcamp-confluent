"""Producer de veneno de *serialização* — a demo da DLQ do Connect (Curso 3, Aula 4).

O ``AvroRecordProducer`` nunca deixaria estes payloads passarem (o serializer
valida contra o Schema Registry). Este módulo usa o ``confluent_kafka.Producer``
cru de propósito: os bytes chegam ao topic intactos e explodem **no consumer** —
no caso do lab, no converter Avro do sink connector, que desvia a mensagem
para o Dead Letter Topic com headers de contexto (``__connect.errors.*``).

Tipos de veneno (mapeiam para o pill #6 — Malformed Data — da reference
.claude/references/poison-pill-patterns.md):

- ``NOT_AVRO_JSON``: JSON puro num topic Avro — sem magic byte, o converter
  falha na hora. O caso clássico de "produtor legado apontou pro topic errado".
- ``TRUNCATED_AVRO``: magic byte + schema id válidos, payload corrompido —
  falha de deserialização no meio do record (o pior de debugar sem DLQ).
- ``EMPTY``: payload de zero bytes.

⚠️ Use SOMENTE em topics de exercício do bootcamp (`lab.*` ou o topic da demo
de DLQ). Envenenar topic compartilhado é anti-pattern pedagógico.
"""

from __future__ import annotations

import logging
import struct
from enum import Enum

from confluent_kafka import Producer

from producers.config import build_producer_config

logger = logging.getLogger(__name__)


class SerializationPoison(bytes, Enum):
    """Payloads que quebram o converter Avro do consumidor/sink."""

    NOT_AVRO_JSON = b'{"transaction_id": "poison-json", "amount": "not-even-avro"}'
    # magic byte (0) + schema id 1 big-endian + bytes que não fecham um record
    TRUNCATED_AVRO = struct.pack(">bI", 0, 1) + b"\xde\xad\xbe\xef"
    EMPTY = b""


def produce_serialization_poison(
    topic: str,
    poison: SerializationPoison = SerializationPoison.NOT_AVRO_JSON,
    key: str = "poison-pill",
    profile: str | None = None,
) -> None:
    """Produz um payload venenoso (não-Avro) num topic Avro.

    A mensagem é entregue com sucesso ao Kafka (o broker não valida payload —
    lição da aula: **o contrato é verificado na serialização e no consumo,
    nunca pelo broker**). O erro aparece no primeiro consumidor com converter
    Avro; num sink com ``errors.tolerance=all`` + DLQ, a mensagem é desviada
    ao DLT com os headers de diagnóstico.

    Parameters
    ----------
    topic : str
        Topic de exercício a envenenar (nunca um topic compartilhado).
    poison : SerializationPoison, optional
        Payload venenoso, by default ``NOT_AVRO_JSON``.
    key : str, optional
        Key da mensagem (string simples), by default ``"poison-pill"``.
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.

    Raises
    ------
    RuntimeError
        Se a entrega ao broker falhar (o veneno precisa CHEGAR ao topic).
    """
    config = build_producer_config(
        profile=profile,
        client_id="bootcamp-poison-injector",
        overrides={"linger.ms": 0},  # demo: entrega imediata, visível na tela
    )
    producer = Producer(config)

    errors: list[str] = []

    def _on_delivery(err: object, msg: object) -> None:
        if err is not None:
            errors.append(str(err))
        else:
            logger.info(
                "Poison %s delivered to %s (a falha vai aparecer no CONSUMER, não aqui)",
                poison.name,
                topic,
            )

    producer.produce(
        topic=topic, key=key.encode("utf-8"), value=poison.value, callback=_on_delivery
    )
    producer.flush(10)
    if errors:
        raise RuntimeError(f"Poison não chegou ao topic {topic}: {errors[0]}")
