"""01 · Hello Kafka — a primeira mensagem da vida do aluno (Curso 2, Aula 4).

O producer mais simples que existe: uma string, um topic, um flush. Sem Schema
Registry, sem serializer, sem callback. O objetivo é UM: ver a mensagem chegar
no topic pela UI do Confluent Cloud.

O que este exemplo deliberadamente NÃO faz (e cada aula seguinte resolve):

- não valida contrato → 03_avro (Schema Registry, Curso 2 Aula 3)
- não confirma entrega → 02_json (callback de delivery)
- não é idempotente   → 04_reliable (acks=all + enable.idempotence)

Uso
---
::

    export KAFKA_PROFILE=local   # ou cloud, com as CC_* preenchidas
    python -m producers.hello_kafka
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from producers.config import build_connection_config, ensure_topic

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.hello.v1"


def main() -> None:
    """Produz uma única mensagem de texto e espera a entrega."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    ensure_topic(TOPIC)

    # A config mínima para falar com um broker: só endereço (+ auth no cloud)
    producer = Producer(build_connection_config())

    producer.produce(topic=TOPIC, value="Olá, Kafka! Primeira mensagem do bootcamp.")

    # flush() bloqueia até a entrega. Sem ele, o processo pode morrer antes
    # de a mensagem sair do buffer — o erro nº 1 de quem começa.
    pending = producer.flush(10)
    if pending:
        logger.error("%d mensagem(ns) não entregue(s) — o broker está acessível?", pending)
    else:
        logger.info("Mensagem entregue ao topic %s", TOPIC)


if __name__ == "__main__":
    main()
