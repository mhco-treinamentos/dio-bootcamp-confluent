"""Aula 5 · Ordem, idempotência e entrega — a duplicata aparece e depois não dói.

Três cenas:

1. **ordem por chave** — ciclo de vida de 3 transações intercaladas, com
   ``key = transaction_id``: eventos da MESMA transação caem na MESMA partição,
   na ordem de negócio. Transações diferentes se espalham.
2. **at-least-once cru** — consumidor processa um lote e "morre" ANTES do
   commit; ao religar, o lote volta e a **duplicata aparece na tela**.
3. **consumidor idempotente** — mesma releitura, mas checando ``event_id`` já
   processado: a duplicata chega e o **efeito não se repete**.

A cena 2 não é bug: é a semântica cumprindo o contrato "não perder nunca, ao
custo de repetir às vezes". A cena 3 é o antídoto.

Uso
---
::

    make aula5-setup
    make aula5-cena1     # ordem por chave
    make aula5-cena2     # crash antes do commit → duplicata
    make aula5-cena3     # dedup → duplicata inofensiva
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal

from confluent_kafka import Consumer, Producer

from consumers.config import build_consumer_config
from demos.curso1._common import BOLD, DIM, RESET, banner, create_topic, delete_topics, say, show
from producers.config import build_producer_config

TOPIC = "lab.payments.transactions.ordem.v1"
GROUP_ID = "demo-aula5-liquidacao"
PARTITIONS = 3

#: Ciclo de vida INTERCALADO de 3 transações — a intercalação é o ponto:
#: mesmo chegando misturados, os eventos de cada transação preservam a ordem.
LIFECYCLE: tuple[tuple[str, str, str], ...] = (
    ("txn-1001", "CREATED", "150.00"),
    ("txn-1002", "CREATED", "89.90"),
    ("txn-1001", "AUTHORIZED", "150.00"),
    ("txn-1003", "CREATED", "1250.00"),
    ("txn-1002", "AUTHORIZED", "89.90"),
    ("txn-1001", "SETTLED", "150.00"),
    ("txn-1003", "DECLINED", "1250.00"),
    ("txn-1002", "SETTLED", "89.90"),
)


def produce_lifecycle() -> None:
    """Publica o ciclo de vida intercalado, com ``key = transaction_id``."""
    producer = Producer(build_producer_config(profile="local", client_id="demo-aula5"))

    for seq, (txn_id, status, amount) in enumerate(LIFECYCLE, start=1):
        event = {
            "event_id": f"evt-{seq:04d}",
            "transaction_id": txn_id,
            "status": status,
            "amount": amount,
            "_seq": seq,
        }
        producer.produce(
            topic=TOPIC,
            # A KEY é a decisão de arquitetura desta aula:
            # mesma key → mesma partição → ordem garantida por transação
            key=txn_id,
            value=json.dumps(event).encode("utf-8"),
        )
        say(f"#{seq} {txn_id} → {status}")
    producer.flush(15)


def consume_showing_key_order() -> None:
    """Consome exibindo chave + partição + offset — a ordem por chave fica evidente."""
    consumer = Consumer(
        build_consumer_config(
            "demo-aula5-ordem",
            profile="local",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
    )
    consumer.subscribe([TOPIC])

    by_partition: dict[int, list[str]] = {}
    show(f"\n{BOLD}{'chave':>10} {'partição':>9} {'offset':>7}  evento{RESET}")
    show(f"{DIM}{'-' * 56}{RESET}")
    try:
        while True:
            msg = consumer.poll(timeout=8.0)
            if msg is None:
                break
            if msg.error():
                continue
            event = json.loads(msg.value())
            key = msg.key().decode("utf-8")
            by_partition.setdefault(msg.partition(), []).append(f"{key}:{event['status']}")
            show(
                f"{key:>10} {msg.partition():>9} {msg.offset():>7}  "
                f"#{event['_seq']} {event['status']}"
            )
    finally:
        consumer.close()

    show(f"\n{BOLD}Ordem por partição:{RESET}")
    for partition in sorted(by_partition):
        show(f"  p{partition}: {' → '.join(by_partition[partition])}")
    say("cada transação viveu inteira numa partição, na ordem de negócio")


def consume_crash_before_commit(batch_size: int = 4) -> None:
    """Processa um lote e 'morre' antes de commitar — cena 2 (at-least-once cru).

    Parameters
    ----------
    batch_size : int, optional
        Quantos eventos processar antes do crash simulado, by default 4.
    """
    consumer = Consumer(
        build_consumer_config(
            GROUP_ID, profile="local", auto_offset_reset="earliest", enable_auto_commit=False
        )
    )
    consumer.subscribe([TOPIC])

    total = Decimal("0.00")
    processed = 0
    try:
        while processed < batch_size:
            msg = consumer.poll(timeout=8.0)
            if msg is None:
                break
            if msg.error():
                continue
            event = json.loads(msg.value())
            total += Decimal(event["amount"])
            processed += 1
            show(
                f"  processado {event['event_id']} · {event['transaction_id']} "
                f"→ {event['status']} (acumulado R$ {total})"
            )
        show()
        say(f"processou {processed} eventos, acumulou R$ {total}")
        say("💥 CRASH agora — ANTES do commit (o offset não avançou)")
    finally:
        # close() sem commit: o grupo volta a ler do último offset commitado
        consumer.close()


def consume_idempotent(batch_size: int = 4, seen: set[str] | None = None) -> set[str]:
    """Consome com dedup por ``event_id`` — cena 3 (duplicata inofensiva).

    Parameters
    ----------
    batch_size : int, optional
        Quantos eventos ler, by default 4.
    seen : set[str] or None, optional
        IDs já processados numa execução anterior (simula o estado do destino).

    Returns
    -------
    set[str]
        Conjunto atualizado de ``event_id`` processados.
    """
    seen = seen if seen is not None else set()
    consumer = Consumer(
        build_consumer_config(
            f"{GROUP_ID}-idem",
            profile="local",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    consumer.subscribe([TOPIC])

    total = Decimal("0.00")
    read = 0
    skipped = 0
    try:
        while read < batch_size:
            msg = consumer.poll(timeout=8.0)
            if msg is None:
                break
            if msg.error():
                continue
            event = json.loads(msg.value())
            read += 1

            if event["event_id"] in seen:
                skipped += 1
                show(f"  {DIM}duplicata ignorada: {event['event_id']}{RESET}")
                continue

            seen.add(event["event_id"])
            total += Decimal(event["amount"])
            show(f"  processado {event['event_id']} (acumulado R$ {total})")
        show()
        say(f"leu {read} · aplicou {read - skipped} · ignorou {skipped} duplicata(s)")
        return seen
    finally:
        consumer.close()


def _run_idempotent_demo(batch_size: int) -> None:
    """Executa a cena 3 completa: primeira passada, crash, releitura com dedup."""
    show(f"{BOLD}1ª passada — processa e 'morre' antes do commit:{RESET}")
    seen = consume_idempotent(batch_size)

    show(f"\n{BOLD}2ª passada — o MESMO lote é reentregue:{RESET}")
    consume_idempotent(batch_size, seen)
    say("a releitura aconteceu igual — mas o efeito não se repetiu")


def main() -> None:
    """Roteia a cena escolhida."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scene",
        choices=["setup", "produce", "ordem", "crash", "idempotente", "teardown"],
        help="etapa da demo",
    )
    parser.add_argument("--batch-size", type=int, default=4, help="eventos por lote")
    args = parser.parse_args()

    if args.scene == "setup":
        banner("Aula 5 · Setup", f"topic com {PARTITIONS} partições")
        create_topic(TOPIC, partitions=PARTITIONS)
    elif args.scene == "produce":
        banner("Aula 5 · Cena 1a", "ciclo de vida intercalado, key = transaction_id")
        produce_lifecycle()
    elif args.scene == "ordem":
        banner("Aula 5 · Cena 1b", "mesma chave → mesma partição → ordem de negócio")
        consume_showing_key_order()
    elif args.scene == "crash":
        banner("Aula 5 · Cena 2", "at-least-once cru: rode DUAS VEZES e veja a duplicata")
        consume_crash_before_commit(args.batch_size)
    elif args.scene == "idempotente":
        banner("Aula 5 · Cena 3", "mesma releitura, efeito único")
        _run_idempotent_demo(args.batch_size)
    else:
        banner("Aula 5 · Teardown", "removendo o topic da demo")
        delete_topics([TOPIC])


if __name__ == "__main__":
    main()
