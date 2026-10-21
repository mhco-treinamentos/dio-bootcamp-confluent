"""Aula 4 · Producers e Consumers — rebalance ao vivo e lag como dado.

A demo mais "cinematográfica" do curso: com produção rodando, o aluno vê
partições migrarem entre consumidores em tempo real.

Roteiro (cada comando num terminal diferente):

1. terminal 1: producer lento (1 msg/s) — o fluxo visível
2. terminal 2: consumidor A sozinho no grupo → atende as 3 partições
3. terminal 3: entra o consumidor B no MESMO grupo → **rebalance na tela**
4. Ctrl+C no consumidor B → novo rebalance, A herda tudo, nada fica órfão
5. terminal 4: describe do grupo → current offset, log end offset e LAG

Os callbacks ``on_assign``/``on_revoke`` são impressos para o rebalance deixar
de ser abstração e virar evento observável.

Uso
---
::

    make aula4-setup
    make aula4-producer        # terminal 1
    make aula4-consumer-a      # terminal 2
    make aula4-consumer-b      # terminal 3
    make aula4-lag             # terminal 4
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from consumers.config import build_consumer_config
from dataset.generators.payments import (
    AccountGenerator,
    CustomerGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from dataset.generators.payments.cards import CardGenerator
from demos.curso1._common import (
    BOLD,
    DIM,
    RESET,
    banner,
    color_for,
    create_topic,
    delete_topics,
    format_record,
    say,
    show,
)
from producers.config import LOCAL_BOOTSTRAP, build_producer_config

TOPIC = "lab.payments.transactions.grupos.v1"
GROUP_ID = "app-pagamentos"
PARTITIONS = 3


def _json_default(value: Any) -> str:
    """Serializa Decimal/datetime para JSON."""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def produce_slowly(rate_per_second: float = 1.0, total: int = 10_000) -> None:
    """Produz devagar, para o fluxo ser acompanhável a olho nu.

    Parameters
    ----------
    rate_per_second : float, optional
        Mensagens por segundo, by default 1.0.
    total : int, optional
        Teto de mensagens (a demo termina por Ctrl+C antes disso), by default 10000.
    """
    producer = Producer(
        build_producer_config(profile="local", client_id="demo-aula4", overrides={"linger.ms": 0})
    )
    customers = list(CustomerGenerator().generate(20))
    accounts = list(AccountGenerator(customers).generate(25))
    cards = list(CardGenerator(accounts, customers).generate(30))
    merchants = list(MerchantGenerator().generate(10))
    generator = TransactionGenerator(customers, accounts, cards, merchants)

    interval = 1.0 / rate_per_second
    say(f"produzindo {rate_per_second:g} msg/s — Ctrl+C para parar")
    try:
        for seq, record in enumerate(generator.generate(total), start=1):
            record["_seq"] = seq
            producer.produce(
                topic=TOPIC,
                key=record["account_id"],
                value=json.dumps(record, default=_json_default).encode("utf-8"),
            )
            producer.poll(0)
            show(f"{DIM}→ #{seq:>4} {format_record(record)}{RESET}")
            time.sleep(interval)
    except KeyboardInterrupt:
        say("producer encerrado")
    finally:
        producer.flush(10)


def consume_with_rebalance_events(label: str, color_index: int) -> None:
    """Consome imprimindo os eventos de rebalance — o coração da demo.

    Parameters
    ----------
    label : str
        Rótulo do consumidor (ex.: ``"consumidor-A"``).
    color_index : int
        Índice de cor para distinguir na tela.
    """
    color = color_for(color_index)

    def on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if not partitions:
            return  # atribuição vazia é ruído de protocolo — não vai para a tela
        parts = ", ".join(str(p.partition) for p in sorted(partitions, key=lambda p: p.partition))
        show(f"\n{color}{BOLD}[{label}] ⇄ REBALANCE — recebeu partição(ões): {parts}{RESET}\n")

    def on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if not partitions:
            return
        parts = ", ".join(str(p.partition) for p in sorted(partitions, key=lambda p: p.partition))
        show(f"\n{color}{BOLD}[{label}] ⇄ REBALANCE — devolveu partição(ões): {parts}{RESET}\n")

    consumer = Consumer(
        build_consumer_config(
            GROUP_ID,
            profile="local",
            enable_auto_commit=False,
            overrides={
                # `range` (eager) em vez do cooperative-sticky de produção: o
                # grupo para, redivide e retoma numa ÚNICA rodada — na tela sai
                # "recebeu: 0, 1" e "recebeu: 2", em vez de várias atribuições
                # incrementais. É a divisão que a aula descreve.
                "partition.assignment.strategy": "range",
                # Detecta a queda do consumidor em ~6s (default é 45s): a cena 3
                # da demo não pode ter meio minuto de tela parada.
                "session.timeout.ms": 6000,
                "heartbeat.interval.ms": 2000,
            },
        )
    )
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    say(f"{label} entrou no grupo {GROUP_ID} — Ctrl+C para simular a queda")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                say(f"erro: {msg.error()}")
                continue
            record = json.loads(msg.value())
            show(
                f"{color}[{label}]{RESET} p{msg.partition()}@{msg.offset()} "
                f"#{record.get('_seq', '?'):>4} · {format_record(record)}"
            )
            consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        say(f"{label} derrubado — observe o rebalance no outro terminal")
    finally:
        consumer.close()


def show_lag() -> None:
    """Exibe current offset, log end offset e lag por partição — o dado do Curso 7."""
    admin = AdminClient({"bootstrap.servers": LOCAL_BOOTSTRAP})
    consumer = Consumer(
        build_consumer_config("demo-aula4-inspector", profile="local", enable_auto_commit=False)
    )
    try:
        future = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP_ID, None)])
        result = next(iter(future.values())).result()

        show(f"\n{BOLD}{'partição':>9} {'current':>9} {'log end':>9} {'LAG':>7}{RESET}")
        show(f"{DIM}{'-' * 38}{RESET}")
        total_lag = 0
        for tp in sorted(result.topic_partitions, key=lambda t: t.partition):
            if tp.topic != TOPIC:
                continue
            _low, high = consumer.get_watermark_offsets(
                TopicPartition(TOPIC, tp.partition), timeout=10, cached=False
            )
            current = tp.offset if tp.offset >= 0 else 0
            lag = max(high - current, 0)
            total_lag += lag
            show(f"{tp.partition:>9} {current:>9} {high:>9} {lag:>7}")
        show(f"{DIM}{'-' * 38}{RESET}")
        show(f"{'TOTAL':>9} {'':>9} {'':>9} {total_lag:>7}\n")
        say("lag = distância entre o fim do log e a posição confirmada")
        say("no Curso 7, este número vira o principal alerta de produção")
    finally:
        consumer.close()


def main() -> None:
    """Roteia a cena escolhida."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scene", choices=["setup", "produce", "consume", "lag", "teardown"], help="etapa da demo"
    )
    parser.add_argument("--label", default="consumidor-A", help="rótulo do consumidor")
    parser.add_argument("--color", type=int, default=0, help="índice de cor")
    parser.add_argument("--rate", type=float, default=1.0, help="mensagens por segundo")
    args = parser.parse_args()

    if args.scene == "setup":
        banner("Aula 4 · Setup", f"topic com {PARTITIONS} partições para o grupo {GROUP_ID}")
        create_topic(TOPIC, partitions=PARTITIONS)
    elif args.scene == "produce":
        banner("Aula 4 · Producer", "fluxo lento e visível — deixe rodando")
        produce_slowly(args.rate)
    elif args.scene == "consume":
        banner(f"Aula 4 · {args.label}", f"grupo {GROUP_ID} — observe os eventos de rebalance")
        consume_with_rebalance_events(args.label, args.color)
    elif args.scene == "lag":
        banner("Aula 4 · Inspeção do grupo", "o progresso do consumo é dado consultável")
        show_lag()
    else:
        banner("Aula 4 · Teardown", "removendo o topic da demo")
        delete_topics([TOPIC])


if __name__ == "__main__":
    main()
