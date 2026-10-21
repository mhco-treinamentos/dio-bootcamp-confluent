"""02 · JSON producer — key, callback de entrega e o custo de não ter contrato.

Sobe um degrau em relação ao 01: agora produzimos **registros** (dicts do
gerador), com **key** (que decide a partição e, portanto, a ordem — Curso 1,
Aula 5) e **callback de entrega** (a confirmação que o 01 não tinha).

A lição negativa que este arquivo existe para ensinar: JSON sem schema aceita
QUALQUER coisa. Se o produtor mudar um campo, o consumidor quebra em produção
— e ninguém descobre na hora de publicar. É o gancho para o 03 (Avro + Schema
Registry): o contrato na ingestão.

Uso
---
::

    python -m producers.json_producer --count 10
"""

from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal
from typing import Any

from confluent_kafka import Producer

from dataset.generators.payments import (
    AccountGenerator,
    CustomerGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from dataset.generators.payments.cards import CardGenerator
from producers.config import build_producer_config, ensure_topic

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.json.v1"


def _json_default(value: Any) -> str:
    """Serializa tipos que o json module não conhece (Decimal, datetime, date).

    Parameters
    ----------
    value : Any
        Valor não-serializável encontrado pelo encoder.

    Returns
    -------
    str
        Representação textual — repare que Decimal vira **string**, não float:
        dinheiro nunca vira ponto flutuante (.claude/rules/python.md).
    """
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    raise TypeError(f"Tipo não serializável: {type(value).__name__}")


def delivery_report(err: object, msg: Any) -> None:
    """Callback de entrega — a confirmação que o exemplo 01 não tinha.

    Parameters
    ----------
    err : object
        Erro de entrega, ou None em caso de sucesso.
    msg : Any
        Mensagem produzida (topic, partition, offset).
    """
    if err is not None:
        logger.error("Falha na entrega: %s", err)
    else:
        logger.info("Entregue em %s[%d] offset %d", msg.topic(), msg.partition(), msg.offset())


def main(count: int = 10) -> None:
    """Produz ``count`` transações como JSON, com key e confirmação de entrega.

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
    transactions = TransactionGenerator(customers, accounts, cards, merchants)

    ensure_topic(TOPIC)
    producer = Producer(build_producer_config(client_id="bootcamp-json-producer"))

    for record in transactions.generate(count):
        producer.produce(
            topic=TOPIC,
            # A key define a partição → todas as transações da mesma conta
            # ficam na mesma partição, em ordem (Curso 1, Aula 5)
            key=record["account_id"],
            value=json.dumps(record, default=_json_default).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # dispara callbacks já disponíveis

    # flush() devolve quantas mensagens NÃO saíram — reportar sucesso sem
    # checar esse número é como dizer "enviei" olhando só para o próprio código.
    pending = producer.flush(30)
    if pending:
        logger.error(
            "%d de %d mensagens NÃO foram entregues — o topic %s existe? "
            "(auto-create está desligado no sandbox: rode `make aula2-setup` ou crie o topic)",
            pending,
            count,
            TOPIC,
        )
        return
    logger.info("Produzidas %d transações em %s (SEM contrato — ver 03_avro)", count, TOPIC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10, help="quantas transações produzir")
    main(**vars(parser.parse_args()))
