"""Aula 3 · O log do Kafka — partições, offsets e segments tocáveis.

Cinco cenas que transformam a teoria do log em coisa visível:

1. topic com 3 partições + describe (retention e configs herdadas)
2. produzir SEM chave → mensagens se espalham; cada partição tem offsets do 0
3. espiar o disco do broker → arquivos .log e .index existem de verdade
4. reler a mesma partição duas vezes → log imutável, quem move é o leitor
5. consultar os offsets finais → o "tamanho" de cada partição sem ler nada

Uso
---
::

    make aula3-cena1   # criar + describe
    make aula3-cena2   # produzir sem chave e consumir mostrando p@offset
    make aula3-cena3   # ls no disco do broker
    make aula3-cena4   # reler duas vezes
    make aula3-cena5   # offsets finais
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

from consumers.config import build_consumer_config
from dataset.generators.payments import (
    AccountGenerator,
    CustomerGenerator,
    MerchantGenerator,
    TransactionGenerator,
)
from dataset.generators.payments.cards import CardGenerator
from demos.curso1._common import BOLD, DIM, RESET, banner, create_topic, delete_topics, say, show
from producers.config import LOCAL_BOOTSTRAP, build_producer_config

TOPIC = "lab.payments.transactions.log.v1"
PARTITIONS = 3


def _json_default(value: Any) -> str:
    """Serializa Decimal/datetime para JSON."""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _generator() -> TransactionGenerator:
    """Cadeia de pools para gerar transações."""
    customers = list(CustomerGenerator().generate(20))
    accounts = list(AccountGenerator(customers).generate(25))
    cards = list(CardGenerator(accounts, customers).generate(30))
    merchants = list(MerchantGenerator().generate(10))
    return TransactionGenerator(customers, accounts, cards, merchants)


def produce_without_key(count: int = 9) -> None:
    """Produz ``count`` mensagens SEM chave — o particionamento fica round-robin.

    Parameters
    ----------
    count : int, optional
        Quantas mensagens produzir, by default 9 (3 por partição).

    Notes
    -----
    Sem key, o producer distribui em round-robin: é o contraste que a Aula 5
    vai usar para mostrar o efeito de produzir COM chave.
    """
    producer = Producer(build_producer_config(profile="local", client_id="demo-aula3"))
    for seq, record in enumerate(_generator().generate(count), start=1):
        record["_seq"] = seq
        producer.produce(
            topic=TOPIC,
            value=json.dumps(record, default=_json_default).encode("utf-8"),
            # SEM key de propósito
        )
        say(f"publicada #{seq} (sem chave)")
    producer.flush(15)


def consume_showing_coordinates(label: str = "leitura") -> None:
    """Consome desde o início exibindo (partição, offset, timestamp) de cada record.

    Parameters
    ----------
    label : str, optional
        Rótulo da leitura (usado na cena 4 para diferenciar 1ª e 2ª), by default "leitura".
    """
    consumer = Consumer(
        build_consumer_config(
            f"demo-aula3-{label}",
            profile="local",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    )
    consumer.subscribe([TOPIC])

    show(f"\n{BOLD}{'partição':>9} {'offset':>7} {'timestamp':>15}  mensagem{RESET}")
    show(f"{DIM}{'-' * 62}{RESET}")
    try:
        while True:
            msg = consumer.poll(timeout=8.0)
            if msg is None:
                break
            if msg.error():
                say(f"erro: {msg.error()}")
                continue
            record = json.loads(msg.value())
            _, ts = msg.timestamp()
            show(
                f"{msg.partition():>9} {msg.offset():>7} {ts:>15}  "
                f"#{record.get('_seq', '?')} txn {record['transaction_id'][:8]}"
            )
    finally:
        consumer.close()
    show()


def show_end_offsets() -> None:
    """Consulta o offset final de cada partição — o "tamanho" do log, sem ler mensagens."""
    consumer = Consumer(
        build_consumer_config("demo-aula3-offsets", profile="local", enable_auto_commit=False)
    )
    try:
        metadata = consumer.list_topics(TOPIC, timeout=10)
        partitions = sorted(metadata.topics[TOPIC].partitions)
        show(f"\n{BOLD}{'partição':>9} {'low':>6} {'high':>6} {'mensagens':>11}{RESET}")
        show(f"{DIM}{'-' * 36}{RESET}")
        total = 0
        for part in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(TOPIC, part), timeout=10, cached=False
            )
            total += high - low
            show(f"{part:>9} {low:>6} {high:>6} {high - low:>11}")
        show(f"{DIM}{'-' * 36}{RESET}")
        show(f"{'TOTAL':>9} {'':>6} {'':>6} {total:>11}\n")
        say("nenhuma mensagem foi lida para obter estes números — o log é inspecionável")
    finally:
        consumer.close()


def main() -> None:
    """Roteia a cena escolhida."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scene",
        choices=["setup", "produce", "consume", "offsets", "teardown"],
        help="etapa da demo",
    )
    parser.add_argument("--count", type=int, default=9, help="mensagens a produzir")
    parser.add_argument("--label", default="leitura-1", help="rótulo da leitura (cena 4)")
    args = parser.parse_args()

    if args.scene == "setup":
        banner("Aula 3 · Cena 1", f"topic com {PARTITIONS} partições — 3 logs independentes")
        create_topic(TOPIC, partitions=PARTITIONS)
        say(
            f"agora rode: docker exec bootcamp-kafka kafka-topics "
            f"--describe --topic {TOPIC} --bootstrap-server {LOCAL_BOOTSTRAP}"
        )
    elif args.scene == "produce":
        banner("Aula 3 · Cena 2", "produzindo SEM chave — veja o espalhamento")
        produce_without_key(args.count)
    elif args.scene == "consume":
        banner(
            "Aula 3 · Leitura", "cada partição tem sua própria sequência de offsets, a partir do 0"
        )
        consume_showing_coordinates(args.label)
    elif args.scene == "offsets":
        banner("Aula 3 · Cena 5", "o tamanho de cada partição, sem consumir nada")
        show_end_offsets()
    else:
        banner("Aula 3 · Teardown", "removendo o topic da demo")
        delete_topics([TOPIC])


if __name__ == "__main__":
    main()
