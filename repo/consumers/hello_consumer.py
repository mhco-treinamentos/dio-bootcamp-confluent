"""01 · Hello consumer — o loop de poll mais simples que existe.

O espelho do ``producers.hello_kafka``: assina um topic, entra no loop de poll
e imprime o que chegar. Sem desserialização, sem tratamento de erro, sem
commit manual.

O conceito que este exemplo planta: consumir **não é** receber um push. O
consumidor **puxa** (``poll``) no seu ritmo — é isso que permite ao Kafka ter
consumidores lentos e rápidos no mesmo topic sem coordenação (Curso 1, Aula 2).

Uso
---
::

    python -m consumers.hello_consumer          # Ctrl+C para sair
"""

from __future__ import annotations

import logging

from confluent_kafka import Consumer

from consumers.config import build_consumer_config

logger = logging.getLogger(__name__)

TOPIC = "lab.payments.gen.hello.v1"
GROUP_ID = "bootcamp-hello-consumer"


def main() -> None:
    """Consome mensagens do topic hello até Ctrl+C."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    # auto-commit ligado aqui SÓ para simplificar o primeiro exemplo;
    # do 04 em diante usamos commit manual (at-least-once de verdade).
    consumer = Consumer(build_consumer_config(GROUP_ID, enable_auto_commit=True))
    consumer.subscribe([TOPIC])

    logger.info("Consumindo %s — Ctrl+C para sair", TOPIC)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # nada novo neste ciclo — normal, não é erro
            if msg.error():
                logger.error("Erro no consumo: %s", msg.error())
                continue
            logger.info(
                "Recebido de %s[%d]@%d: %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.value().decode("utf-8") if msg.value() else "<tombstone>",
            )
    except KeyboardInterrupt:
        logger.info("Encerrando por Ctrl+C")
    finally:
        # close() faz o commit final e sai do grupo de forma limpa,
        # disparando o rebalance imediatamente (em vez de esperar timeout)
        consumer.close()


if __name__ == "__main__":
    main()
