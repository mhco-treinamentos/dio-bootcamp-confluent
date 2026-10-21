"""Aula 2 · Message Queue vs Data Streaming — a mesma plataforma, duas semânticas.

Três cenas, um só topic — o que muda é APENAS o ``group.id``:

- cena 1 (fila):      2 consumidores, MESMO grupo   → as mensagens se dividem
- cena 2 (streaming): 2 consumidores, grupos DIFERENTES → os dois recebem tudo
- cena 3 (replay):    consumidor novo, grupo novo, desde o início → tudo reaparece

A cena 3 é o argumento que fila tradicional não tem resposta: nada foi apagado
pelo consumo.

Uso
---
::

    make aula2-setup     # cria o topic
    make aula2-cena1     # fila
    make aula2-cena2     # streaming
    make aula2-cena3     # replay
    make aula2-teardown
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from confluent_kafka import Consumer, Producer

from consumers.config import build_consumer_config
from dataset.generators.payments import (
    AccountGenerator,
    CustomerGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from dataset.generators.payments.cards import CardGenerator
from demos.curso1._common import (
    RESET,
    banner,
    color_for,
    create_topic,
    delete_topics,
    format_record,
    say,
    show,
)
from producers.config import build_producer_config

TOPIC = "lab.payments.transactions.v1"
POLL_TIMEOUT_S = 2.0
#: Ciclos vazios (após já ter recebido algo) antes de encerrar sozinho.
#: A demo é ao vivo — o consumidor sobe ANTES de o apresentador produzir.
IDLE_POLLS_BEFORE_EXIT = 5


def _build_generator() -> TransactionGenerator:
    """Monta a cadeia de pools e devolve o gerador de transações."""
    customers = list(CustomerGenerator().generate(20))
    accounts = list(AccountGenerator(customers).generate(25))
    cards = list(CardGenerator(accounts, customers).generate(30))
    merchants = list(MerchantGenerator().generate(10))
    return TransactionGenerator(customers, accounts, cards, merchants)


def _json_default(value: Any) -> str:
    """Serializa Decimal como string e datetime como ISO (dinheiro nunca vira float)."""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def produce(count: int = 10, start_at: int = 1) -> None:
    """Publica ``count`` transações numeradas (a numeração ajuda a acompanhar na tela).

    Parameters
    ----------
    count : int, optional
        Quantas mensagens publicar, by default 10.
    start_at : int, optional
        Número inicial da sequência exibida, by default 1.
    """
    producer = Producer(
        build_producer_config(
            profile="local",
            client_id="demo-aula2",
            overrides={
                # O default do librdkafka é o partitioner STICKY: sem key, ele
                # gruda o lote inteiro numa partição só — e com `linger.ms=5`
                # as 10 mensagens da demo viram um lote único, indo todas para
                # a mesma partição. Resultado: um consumidor recebe tudo e o
                # outro fica zerado, destruindo a cena da divisão de trabalho.
                # `murmur2_random` sorteia a partição por mensagem (é o
                # comportamento do cliente Java para registros sem key), e
                # `linger.ms=0` evita o agrupamento que realimenta o problema.
                "partitioner": "murmur2_random",
                "linger.ms": 0,
            },
        )
    )
    generator = _build_generator()

    for offset, record in enumerate(generator.generate(count)):
        record["_seq"] = start_at + offset  # só para a narração da demo
        producer.produce(
            topic=TOPIC,
            # SEM key: a distribuição fica a cargo do partitioner, para a divisão
            # do trabalho ficar visível na tela. A key entra em cena na Aula 5,
            # quando o assunto passa a ser ORDEM por entidade.
            value=json.dumps(record, default=_json_default).encode("utf-8"),
        )
        say(f"publicada #{record['_seq']:>2} · {format_record(record)}")
    producer.flush(15)


def consume(group_id: str, label: str, color_index: int, from_beginning: bool = False) -> int:
    """Consome até o topic secar, anunciando cada mensagem com o rótulo do consumidor.

    Parameters
    ----------
    group_id : str
        Consumer group — a variável que muda a semântica entre as cenas.
    label : str
        Rótulo exibido (ex.: ``"consumidor-A"``).
    color_index : int
        Índice de cor para distinguir consumidores na tela.
    from_beginning : bool, optional
        Ler desde o offset 0 (cena 3 — replay), by default False.

    Returns
    -------
    int
        Quantas mensagens este consumidor recebeu.
    """
    color = color_for(color_index)
    consumer = Consumer(
        build_consumer_config(
            group_id,
            profile="local",
            auto_offset_reset="earliest" if from_beginning else "latest",
            enable_auto_commit=True,
        )
    )
    consumer.subscribe([TOPIC])

    say(f"{label} aguardando no grupo {group_id} — Ctrl+C para encerrar")

    received = 0
    idle_polls = 0
    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT_S)
            if msg is None:
                idle_polls += 1
                # A demo é ao vivo: o apresentador ainda vai produzir noutro
                # terminal. Só encerra após já ter recebido algo E secar.
                if received and idle_polls >= IDLE_POLLS_BEFORE_EXIT:
                    break
                continue
            if msg.error():
                say(f"erro: {msg.error()}")
                continue
            idle_polls = 0
            record = json.loads(msg.value())
            received += 1
            show(
                f"{color}[{label}] grupo={group_id}{RESET} "
                f"#{record.get('_seq', '?'):>2} p{msg.partition()}@{msg.offset()} "
                f"· {format_record(record)}"
            )
        return received
    except KeyboardInterrupt:
        say(f"{label} encerrado")
        return received
    finally:
        consumer.close()


def main() -> None:
    """Roteia a cena escolhida pela linha de comando."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scene",
        choices=["setup", "produce", "consume", "teardown"],
        help="etapa da demo",
    )
    parser.add_argument("--group", default="app-pagamentos", help="consumer group")
    parser.add_argument("--label", default="consumidor-A", help="rótulo na tela")
    parser.add_argument("--color", type=int, default=0, help="índice de cor")
    parser.add_argument("--count", type=int, default=10, help="mensagens a produzir")
    parser.add_argument("--start-at", type=int, default=1, help="número inicial da sequência")
    parser.add_argument(
        "--from-beginning", action="store_true", help="ler desde o offset 0 (replay)"
    )
    args = parser.parse_args()

    if args.scene == "setup":
        banner("Aula 2 · Setup", "um topic, 3 partições — o mesmo para as três cenas")
        create_topic(TOPIC, partitions=3)
    elif args.scene == "produce":
        banner("Aula 2 · Produzindo", f"{args.count} transações no topic {TOPIC}")
        produce(args.count, args.start_at)
    elif args.scene == "consume":
        total = consume(args.group, args.label, args.color, args.from_beginning)
        show(f"\n{args.label} recebeu {total} mensagem(ns)\n")
    else:
        banner("Aula 2 · Teardown", "removendo o topic da demo")
        delete_topics([TOPIC])


if __name__ == "__main__":
    main()
