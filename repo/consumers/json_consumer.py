"""02 · JSON consumer — onde a falta de contrato começa a doer.

Consome o que ``producers.json_producer`` publicou. O degrau em relação ao 01:
desserializamos o payload e acessamos campos — e é exatamente aí que a ausência
de schema cobra o preço:

- payload que não é JSON → ``JSONDecodeError``
- campo que sumiu na origem → ``KeyError``
- número que veio como string → ``TypeError`` na conta

Este arquivo trata cada um explicitamente (contando os erros em vez de morrer)
para tornar o problema **visível**. O 03 elimina a classe inteira do problema
com Avro + Schema Registry.

Uso
---
::

    python -m consumers.json_consumer --max-messages 20
"""

from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer

from consumers.config import build_consumer_config

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.transactions.json.v1"
GROUP_ID = "bootcamp-json-consumer"


def main(max_messages: int = 20) -> None:
    """Consome e desserializa transações JSON, contando falhas de parse.

    Parameters
    ----------
    max_messages : int, optional
        Quantas mensagens processar antes de encerrar, by default 20.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    consumer = Consumer(build_consumer_config(GROUP_ID))
    consumer.subscribe([TOPIC])

    processed = 0
    parse_errors = 0
    total_amount = Decimal("0.00")

    try:
        while processed + parse_errors < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                logger.info("Sem mensagens novas — encerrando")
                break
            if msg.error():
                logger.error("Erro no consumo: %s", msg.error())
                continue

            try:
                record = json.loads(msg.value())
                # Decimal a partir de STRING (o producer serializou assim de
                # propósito): float perderia centavos — regra do domínio.
                total_amount += Decimal(record["amount"])
                processed += 1
            except (json.JSONDecodeError, KeyError, InvalidOperation, TypeError) as exc:
                # Sem contrato, cada um destes é um bug em potencial em produção
                parse_errors += 1
                logger.warning(
                    "Payload inválido em %s[%d]@%d: %s — %s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    type(exc).__name__,
                    exc,
                )
                continue

            # Commit manual APÓS processar = at-least-once (Curso 1, Aula 5)
            consumer.commit(msg, asynchronous=False)

        logger.info(
            "Processadas %d transações (total R$ %s) · %d payloads inválidos",
            processed,
            total_amount,
            parse_errors,
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=20, help="quantas mensagens processar")
    main(**vars(parser.parse_args()))
