"""04 · Consumer confiável — at-least-once com commit manual e rebalance limpo.

O degrau de produção. Três decisões explícitas que os exemplos anteriores
tomavam por default:

1. **Commit manual APÓS processar** — a ordem sagrada: processa → efetiva →
   commita. Morrer entre o processamento e o commit causa *reprocessamento*
   (at-least-once), nunca *perda* (at-most-once). Curso 1, Aula 5.
2. **Callbacks de rebalance** — commitar o que já foi processado ANTES de
   perder a partição evita reprocessar um lote inteiro à toa.
3. **Idempotência no destino** — reprocessar é inevitável; o efeito colateral
   precisa ser inofensivo (upsert por chave). O ``ProcessedTracker`` abaixo é
   a versão didática em memória do que num sink seria ``insert.mode=upsert``.

Uso
---
::

    python -m consumers.reliable --max-messages 100
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from consumers.config import build_consumer_config
from producers.config import build_schema_registry_config

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.avro.v1"
GROUP_ID = "bootcamp-reliable-consumer"


@dataclass
class ProcessedTracker:
    """Idempotência didática: registra o que já foi processado, por chave.

    Em produção este papel é do destino (upsert por PK, ``insert.mode=upsert``
    num sink connector). Aqui, em memória, serve para o aluno VER a duplicata
    chegando e sendo ignorada.
    """

    seen: set[str] = field(default_factory=set)
    duplicates: int = 0

    def is_new(self, key: str) -> bool:
        """Retorna True na primeira vez que a chave aparece.

        Parameters
        ----------
        key : str
            Chave de negócio (ex.: ``transaction_id``).

        Returns
        -------
        bool
            True se ainda não vista; False (e conta duplicata) caso contrário.
        """
        if key in self.seen:
            self.duplicates += 1
            return False
        self.seen.add(key)
        return True


def _on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """Commita o progresso antes de entregar as partições no rebalance."""
    logger.info("Rebalance: perdendo %d partição(ões) — commitando progresso", len(partitions))
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        logger.warning("Nada a commitar no revoke: %s", exc)


def consume(
    max_messages: int = 100,
    handler: Callable[[dict[str, Any]], None] | None = None,
) -> ProcessedTracker:
    """Consome com garantia at-least-once e dedup idempotente.

    Parameters
    ----------
    max_messages : int, optional
        Quantas mensagens processar antes de encerrar, by default 100.
    handler : Callable[[dict[str, Any]], None] or None, optional
        Ação de negócio por record. None apenas loga.

    Returns
    -------
    ProcessedTracker
        Estatísticas de processamento (únicos vs duplicatas).
    """
    schema_registry = SchemaRegistryClient(build_schema_registry_config())
    deserializer = AvroDeserializer(schema_registry)

    consumer = Consumer(build_consumer_config(GROUP_ID, enable_auto_commit=False))
    consumer.subscribe(
        [TOPIC],
        on_revoke=lambda c, p: _on_revoke(c, p),
    )

    tracker = ProcessedTracker()
    processed = 0
    try:
        while processed < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                logger.info("Sem mensagens novas — encerrando")
                break
            if msg.error():
                logger.error("Erro no consumo: %s", msg.error())
                continue

            record = deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
            if record is not None and tracker.is_new(record["transaction_id"]):
                if handler is not None:
                    handler(record)
                else:
                    logger.debug("Processada %s", record["transaction_id"])

            processed += 1
            # A ORDEM IMPORTA: processou → então commita.
            consumer.commit(msg, asynchronous=False)

        logger.info(
            "Processadas %d mensagens · %d únicas · %d duplicatas ignoradas",
            processed,
            len(tracker.seen),
            tracker.duplicates,
        )
        return tracker
    finally:
        consumer.close()


def main(max_messages: int = 100) -> None:
    """Entry point do exemplo."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    consume(max_messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=100, help="quantas mensagens processar")
    main(**vars(parser.parse_args()))
