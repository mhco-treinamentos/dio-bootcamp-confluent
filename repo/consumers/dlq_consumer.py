"""05 · Consumer com Dead Letter Topic — o padrão da Aula 4, em código.

A Aula 4 do Curso 3 mostra o DLT entregue **por configuração** no sink
connector. Este arquivo prova a tese arquitetural da aula: **DLT não é feature
do Connect, é padrão de EDA** — qualquer consumidor pode (e em produção, deve)
implementar o mesmo desenho:

    try → retry com critério → desviou pro DLT COM CONTEXTO → segue o fluxo

As três respostas possíveis à mensagem venenosa (seção 2.2 da aula):

    falha tudo        → 1 veneno derruba o pipeline inteiro          ✗
    ignora em silêncio→ o dado some e ninguém sabe (o pior)          ✗
    desvia com contexto→ fluxo vivo, dado preservado, diagnosticável ✓

Os headers reproduzem a convenção do Connect (``__connect.errors.*``) de
propósito: o aluno abre o DLT deste consumer e vê o MESMO formato do DLT do
conector. Um padrão, duas implementações.

Uso
---
::

    # 1. envenene o topic
    python -c "from producers.poison import produce_serialization_poison as p; \
        p('lab.payments.gen.transactions.avro.v1')"
    # 2. veja o desvio acontecer
    python -m consumers.dlq_consumer --max-messages 50
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from consumers.config import build_consumer_config
from producers.config import build_producer_config, build_schema_registry_config

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.avro.v1"
DLQ_TOPIC = "lab.payments.dlq.transactions.v1"
GROUP_ID = "bootcamp-dlq-consumer"

#: Tentativas antes de considerar a mensagem venenosa. Erro transitório
#: (rede, indisponibilidade) some no retry; veneno de verdade persiste.
MAX_RETRIES = 2
RETRY_BACKOFF_S = 0.2


@dataclass
class DlqStats:
    """Contagem de resultados do consumo."""

    processed: int = 0
    dead_lettered: int = 0


def _build_error_headers(msg: Message, exc: Exception, stage: str) -> list[tuple[str, bytes]]:
    """Monta os headers de diagnóstico que viajam com a mensagem desviada.

    Mesma convenção do Kafka Connect — DLT sem contexto é cemitério sem lápide.

    Parameters
    ----------
    msg : Message
        A mensagem original que falhou.
    exc : Exception
        A exceção que causou o desvio.
    stage : str
        Em que etapa quebrou (ex.: ``"VALUE_DESERIALIZER"``).

    Returns
    -------
    list[tuple[str, bytes]]
        Headers prontos para o ``produce()``.
    """
    return [
        ("__connect.errors.topic", msg.topic().encode("utf-8")),
        ("__connect.errors.partition", str(msg.partition()).encode("utf-8")),
        ("__connect.errors.offset", str(msg.offset()).encode("utf-8")),
        ("__connect.errors.stage", stage.encode("utf-8")),
        ("__connect.errors.exception.class", type(exc).__name__.encode("utf-8")),
        ("__connect.errors.exception.message", str(exc)[:500].encode("utf-8")),
    ]


def _deserialize_with_retry(
    deserializer: AvroDeserializer, msg: Message
) -> tuple[dict[str, Any] | None, Exception | None]:
    """Tenta desserializar, com retry para filtrar erro transitório de veneno.

    Parameters
    ----------
    deserializer : AvroDeserializer
        Desserializador Avro ligado ao Schema Registry.
    msg : Message
        Mensagem a desserializar.

    Returns
    -------
    tuple[dict[str, Any] or None, Exception or None]
        ``(record, None)`` em sucesso; ``(None, exc)`` se esgotou o retry.
    """
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            return deserializer(msg.value(), ctx), None
        except Exception as exc:  # noqa: BLE001 — qualquer falha vira candidata a DLT
            last_error = exc
            if attempt < MAX_RETRIES:
                logger.debug("Retry %d/%d após %s", attempt + 1, MAX_RETRIES, type(exc).__name__)
                time.sleep(RETRY_BACKOFF_S * (2**attempt))
    return None, last_error


def consume(max_messages: int = 50) -> DlqStats:
    """Consome desviando mensagens venenosas para o DLT — o pipeline não cai.

    Parameters
    ----------
    max_messages : int, optional
        Quantas mensagens processar antes de encerrar, by default 50.

    Returns
    -------
    DlqStats
        Quantas foram processadas e quantas foram desviadas.
    """
    schema_registry = SchemaRegistryClient(build_schema_registry_config())
    deserializer = AvroDeserializer(schema_registry)

    consumer = Consumer(build_consumer_config(GROUP_ID, enable_auto_commit=False))
    consumer.subscribe([TOPIC])
    dlq_producer = Producer(build_producer_config(client_id="bootcamp-dlq-writer"))

    stats = DlqStats()
    try:
        while stats.processed + stats.dead_lettered < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                logger.info("Sem mensagens novas — encerrando")
                break
            if msg.error():
                logger.error("Erro no consumo: %s", msg.error())
                continue

            record, error = _deserialize_with_retry(deserializer, msg)

            if error is not None:
                # A mensagem ORIGINAL vai intacta; o diagnóstico vai nos headers
                dlq_producer.produce(
                    topic=DLQ_TOPIC,
                    key=msg.key(),
                    value=msg.value(),
                    headers=_build_error_headers(msg, error, "VALUE_DESERIALIZER"),
                )
                dlq_producer.poll(0)
                stats.dead_lettered += 1
                logger.warning(
                    "Desviada para %s: %s em %s@%d — PIPELINE SEGUE VIVO",
                    DLQ_TOPIC,
                    type(error).__name__,
                    msg.topic(),
                    msg.offset(),
                )
            else:
                stats.processed += 1

            # Commit em AMBOS os casos: a mensagem venenosa foi "tratada"
            # (desviada). Sem isso, o consumer relê o veneno para sempre.
            consumer.commit(msg, asynchronous=False)

        dlq_producer.flush(10)
        logger.info(
            "Resultado: %d processadas · %d desviadas para o DLT",
            stats.processed,
            stats.dead_lettered,
        )
        return stats
    finally:
        consumer.close()


def main(max_messages: int = 50) -> None:
    """Entry point do exemplo."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    consume(max_messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=50, help="quantas mensagens processar")
    main(**vars(parser.parse_args()))
