"""03 · Avro producer — o contrato entra em cena (Curso 2, Aula 3).

O degrau que separa "mandei bytes" de "publiquei um evento com contrato": o
``AvroSerializer`` registra/valida o schema no **Schema Registry** antes de
serializar. Se o payload não bate com o schema, o erro acontece AQUI, no
producer — não seis meses depois, no consumidor.

Diferença para o 02 (JSON):

- schema registrado no subject ``{topic}-value`` automaticamente
- payload binário compacto (sem repetir nomes de campo em cada mensagem)
- evolução governada: BACKWARD por default (adicionar campo com default é OK,
  renomear não é) — .claude/rules/schemas.md

Este exemplo ainda produz "na mão"; o 04 embrulha tudo numa classe com
métricas, idempotência e checagem de compatibilidade explícita.

Uso
---
::

    python -m producers.avro_producer --count 10
"""

from __future__ import annotations

import argparse
import json
import logging

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from dataset.generators.payments import (
    AccountGenerator,
    CustomerGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from dataset.generators.payments.cards import CardGenerator
from producers.config import build_producer_config, build_schema_registry_config, ensure_topic

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.avro.v1"


def main(count: int = 10) -> None:
    """Produz ``count`` transações em Avro, com schema registrado no SR.

    Parameters
    ----------
    count : int, optional
        Quantidade de transações a produzir, by default 10.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    customers = list(CustomerGenerator().generate(20))
    accounts = list(AccountGenerator(customers).generate(25))
    cards = list(CardGenerator(accounts, customers).generate(30))
    merchants = list(MerchantGenerator().generate(10))
    generator = TransactionGenerator(customers, accounts, cards, merchants)

    # O schema vem do .avsc versionado em schemas/payments/ — fonte única
    # de verdade do contrato, a mesma que o Connect e o Flink enxergam.
    schema_str = json.dumps(generator.get_schema())

    schema_registry = SchemaRegistryClient(build_schema_registry_config())
    avro_serializer = AvroSerializer(schema_registry, schema_str, to_dict=lambda rec, ctx: rec)
    key_serializer = StringSerializer("utf_8")

    ensure_topic(TOPIC)
    producer = Producer(build_producer_config(client_id="bootcamp-avro-producer"))

    for record in generator.generate(count):
        producer.produce(
            topic=TOPIC,
            key=key_serializer(record["account_id"], SerializationContext(TOPIC, MessageField.KEY)),
            value=avro_serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
        )
        producer.poll(0)

    producer.flush(30)
    logger.info("Produzidas %d transações em %s — schema no subject %s-value", count, TOPIC, TOPIC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10, help="quantas transações produzir")
    main(**vars(parser.parse_args()))
