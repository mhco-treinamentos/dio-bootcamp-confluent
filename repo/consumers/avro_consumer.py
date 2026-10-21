"""03 · Avro consumer — o contrato do lado de quem lê.

O ``AvroDeserializer`` busca o schema no Schema Registry pelo **schema id**
embutido nos primeiros bytes da mensagem (wire format: magic byte + id + payload).
Consequências que o aluno vê na prática:

- não existe "campo faltando": o schema garante a forma
- o consumidor lê mensagens escritas com schema ANTERIOR sem quebrar
  (compatibilidade BACKWARD — Curso 2, Aula 3)
- se o payload não for Avro, o erro é de **desserialização** — e é exatamente
  esse erro que o Dead Letter Topic existe para capturar (ver 05_dlq_consumer)

Uso
---
::

    python -m consumers.avro_consumer --max-messages 20
"""

from __future__ import annotations

import argparse
import logging

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from consumers.config import build_consumer_config
from producers.config import build_schema_registry_config

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.avro.v1"
GROUP_ID = "bootcamp-avro-consumer"


def main(max_messages: int = 20) -> None:
    """Consome transações Avro, resolvendo o schema pelo Registry.

    Parameters
    ----------
    max_messages : int, optional
        Quantas mensagens processar antes de encerrar, by default 20.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    schema_registry = SchemaRegistryClient(build_schema_registry_config())
    # Sem passar schema: o deserializer resolve pelo id embutido na mensagem
    deserializer = AvroDeserializer(schema_registry)

    consumer = Consumer(build_consumer_config(GROUP_ID))
    consumer.subscribe([TOPIC])

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
            if record is None:
                logger.info("Tombstone em %s@%d (remoção)", msg.topic(), msg.offset())
                consumer.commit(msg, asynchronous=False)
                continue

            logger.info(
                "Transação %s · conta %s · R$ %s · %s",
                record["transaction_id"][:8],
                record["account_id"][:8],
                record["amount"],
                record["status"],
            )
            processed += 1
            consumer.commit(msg, asynchronous=False)

        logger.info("Processadas %d transações com contrato validado", processed)
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=20, help="quantas mensagens processar")
    main(**vars(parser.parse_args()))
