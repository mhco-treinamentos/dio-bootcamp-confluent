"""Aula 5 · As semânticas de entrega na API — chave, crash e dedup em código.

Três cenas, cada uma provando na tela um conceito do guia:

``ordem``
    ``produce(key=...)`` → o delivery report mostra a MESMA chave caindo
    sempre na MESMA partição. Com ``--sem-chave``, a cena da ordem quebrada:
    o ciclo de vida de uma transação espalhado em partições diferentes.

``crash --commit antes|depois``
    A tabela do Passo 4, executável. O consumidor "morre" no meio do lote;
    rode o MESMO comando de novo e compare:
    ``antes``  → uma mensagem commitada e nunca processada (**perdida**)
    ``depois`` → uma mensagem processada duas vezes (**duplicada**)

``dedup``
    A duplicata inofensiva: a releitura acontece igual, mas um ``set`` de
    ids já vistos impede o efeito de se repetir.

Uso
---
::

    python -m demos.curso1.aula5_api_entrega ordem [--sem-chave]
    python -m demos.curso1.aula5_api_entrega crash --commit antes
    python -m demos.curso1.aula5_api_entrega crash --commit depois
    python -m demos.curso1.aula5_api_entrega dedup
    python -m demos.curso1.aula5_api_entrega teardown
"""

from __future__ import annotations

import argparse

from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, Producer, TopicPartition

from demos.curso1._common import (
    BOLD,
    DIM,
    RESET,
    banner,
    create_topic,
    delete_groups,
    delete_topics,
    show,
)
from producers.config import LOCAL_BOOTSTRAP

TOPIC_ORDEM = "lab.payments.api.aula5.ordem.v1"
TOPIC_SEM_CHAVE = "lab.payments.api.aula5.semchave.v1"
#: 1 partição de propósito: offsets lineares 0..7 tornam o crash determinístico.
TOPIC_SEMANTICA = "lab.payments.api.aula5.semantica.v1"

#: O ciclo de vida de 3 transações INTERCALADAS — os mesmos dados do guia.
LIFECYCLE: tuple[tuple[str, str], ...] = (
    ("txn-1001", "CREATED 150.00"),
    ("txn-1002", "CREATED 89.90"),
    ("txn-1001", "AUTHORIZED 150.00"),
    ("txn-1003", "CREATED 1250.00"),
    ("txn-1002", "AUTHORIZED 89.90"),
    ("txn-1001", "SETTLED 150.00"),
    ("txn-1003", "DECLINED 1250.00"),
    ("txn-1002", "SETTLED 89.90"),
)

#: Offset em que o crash simulado acontece (a 4ª mensagem do lote).
CRASH_AT_OFFSET = 3


# ---------------------------------------------------------------------------
# Cena 1 — ordem por chave (e a quebra sem chave)
# ---------------------------------------------------------------------------
def scene_ordem(sem_chave: bool) -> None:
    """Produz o ciclo de vida e mostra chave → partição no delivery report.

    Parameters
    ----------
    sem_chave : bool
        Se ``True``, produz sem chave com partitioner aleatório — a cena
        da ordem quebrada.
    """
    topic = TOPIC_SEM_CHAVE if sem_chave else TOPIC_ORDEM
    modo = "SEM chave (ordem NÃO garantida)" if sem_chave else "COM chave (ordem por chave)"
    banner("Aula 5 · Producer API — ordem", modo)
    create_topic(topic, partitions=3)

    config: dict[str, object] = {
        "bootstrap.servers": LOCAL_BOOTSTRAP,
        "client.id": "aula5-api-ordem",
        "acks": "all",
        "enable.idempotence": True,
        # murmur2_random: chave presente → hash determinístico (murmur2);
        # chave AUSENTE → partição aleatória por mensagem. linger.ms=0
        # impede o lote inteiro de colar numa partição só (o bug do
        # partitioner sticky que a Aula 2 documenta).
        "partitioner": "murmur2_random",
        "linger.ms": 0,
    }
    producer = Producer(config)

    def report(err: object, msg: Message) -> None:
        if err is not None:
            show(f"  ✗ FALHOU: {err}")
            return
        key = msg.key().decode() if msg.key() else "(nula)"
        show(
            f"  chave {BOLD}{key:>10}{RESET} → Partition:{msg.partition()}"
            f"  {DIM}{msg.value().decode()}{RESET}"
        )

    show(f"{BOLD}Delivery reports — repare na coluna da partição:{RESET}")
    for key, value in LIFECYCLE:
        producer.produce(
            topic=topic,
            key=None if sem_chave else key.encode(),
            value=value.encode(),
            on_delivery=report,
        )
        producer.poll(0)
    if producer.flush(timeout=10) > 0:
        raise SystemExit("flush() reportou mensagens não entregues")

    if sem_chave:
        show(
            f"\n{BOLD}A lição:{RESET} a txn-1001 caiu em partições diferentes —"
            " um consumidor pode ler SETTLED antes de CREATED."
            f"\n{DIM}partição = hash(chave) % N; sem chave, não há garantia nenhuma.{RESET}"
        )
    else:
        show(
            f"\n{BOLD}A lição:{RESET} a MESMA chave caiu SEMPRE na mesma partição —"
            " o ciclo CREATED → AUTHORIZED → SETTLED preserva a ordem."
        )


# ---------------------------------------------------------------------------
# Cena 2 — commit antes × depois, com crash no meio
# ---------------------------------------------------------------------------
def _seed_semantica() -> None:
    """Garante o topic de 1 partição com as 8 mensagens do lote (idempotente)."""
    create_topic(TOPIC_SEMANTICA, partitions=1)
    probe = Consumer({"bootstrap.servers": LOCAL_BOOTSTRAP, "group.id": "aula5-api-probe"})
    _lo, hi = probe.get_watermark_offsets(TopicPartition(TOPIC_SEMANTICA, 0), timeout=10)
    probe.close()
    if hi >= len(LIFECYCLE):
        return  # já semeado numa execução anterior
    producer = Producer({"bootstrap.servers": LOCAL_BOOTSTRAP, "enable.idempotence": True})
    for key, value in LIFECYCLE:
        producer.produce(TOPIC_SEMANTICA, key=key.encode(), value=value.encode())
    if producer.flush(timeout=10) > 0:
        raise SystemExit("seed falhou: flush() reportou pendências")
    show(f"{DIM}(topic semeado com {len(LIFECYCLE)} mensagens — partição única){RESET}\n")


def scene_crash(commit_mode: str) -> None:
    """Processa o lote commitando antes OU depois — e morre no offset 3.

    O crash só acontece na PRIMEIRA execução (grupo sem offset commitado).
    Rode o mesmo comando de novo para ver a recuperação — e o preço dela.

    Parameters
    ----------
    commit_mode : str
        ``"antes"`` (at-most-once) ou ``"depois"`` (at-least-once).
    """
    banner(
        f"Aula 5 · crash com commit {commit_mode.upper()} do processamento",
        "at-most-once" if commit_mode == "antes" else "at-least-once",
    )
    _seed_semantica()

    group = f"liquidacao-api-{commit_mode}"
    consumer = Consumer(
        {
            "bootstrap.servers": LOCAL_BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # NÓS decidimos quando commitar
        }
    )
    tp = TopicPartition(TOPIC_SEMANTICA, 0)
    committed = consumer.committed([tp], timeout=10)[0].offset
    first_run = committed < 0  # grupo nunca commitou → é a 1ª execução
    show(
        f"grupo '{group}' — "
        + (
            f"{BOLD}1ª execução{RESET}: o crash vai acontecer no offset {CRASH_AT_OFFSET}"
            if first_run
            else f"{BOLD}2ª execução{RESET}: retomando do offset commitado ({committed})"
        )
        + "\n"
    )

    consumer.assign([tp])
    processed: list[int] = []
    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break  # lote esgotado
            if msg.error():
                continue

            if commit_mode == "antes":
                # commit ANTES de processar: o broker já considera entregue.
                consumer.commit(message=msg, asynchronous=False)
                if first_run and msg.offset() == CRASH_AT_OFFSET:
                    show(f"\n💥 crash SIMULADO — offset {msg.offset()} commitado, NÃO processado")
                    raise SystemExit(0)
                _process(msg, processed)
            else:
                # processa ANTES de commitar: o efeito acontece primeiro.
                _process(msg, processed)
                if first_run and msg.offset() == CRASH_AT_OFFSET:
                    show(f"\n💥 crash SIMULADO — offset {msg.offset()} processado, NÃO commitado")
                    raise SystemExit(0)
                consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()

    show(f"\n{BOLD}fim do lote — offsets processados nesta execução: {processed}{RESET}")
    if not first_run:
        if commit_mode == "antes":
            show(
                f"  ✗ o offset {CRASH_AT_OFFSET} NÃO aparece em execução nenhuma:"
                f" foi commitado e morreu antes do efeito → {BOLD}PERDIDO{RESET}"
            )
        else:
            show(
                f"  ✓ o offset {CRASH_AT_OFFSET} apareceu nas DUAS execuções:"
                f" processado, morto antes do commit, reentregue → {BOLD}DUPLICADO{RESET}"
            )


def _process(msg: Message, processed: list[int]) -> None:
    """'Processa' a mensagem (exibe) e registra o offset."""
    processed.append(msg.offset())
    show(f"  processado Offset:{msg.offset()}  {msg.key().decode()}  {msg.value().decode()}")


# ---------------------------------------------------------------------------
# Cena 3 — a duplicata inofensiva
# ---------------------------------------------------------------------------
def scene_dedup() -> None:
    """Duas passadas sobre o mesmo lote: a 2ª reentrega tudo, o efeito não repete."""
    banner("Aula 5 · consumidor idempotente", "N entregas, 1 efeito")
    _seed_semantica()

    # O estado do dedup: em produção isto é o DESTINO idempotente
    # (INSERT ... ON CONFLICT, upsert de sink) — não um set em memória.
    seen: set[str] = set()

    for attempt in (1, 2):
        label = (
            "1ª passada — processa e 'morre' antes do commit"
            if attempt == 1
            else "2ª passada — o MESMO lote é reentregue"
        )
        show(f"{BOLD}{label}:{RESET}")
        consumer = Consumer(
            {
                "bootstrap.servers": LOCAL_BOOTSTRAP,
                "group.id": "auditoria-api-dedup",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.assign([TopicPartition(TOPIC_SEMANTICA, 0, OFFSET_BEGINNING)])
        read = applied = 0
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                continue
            read += 1
            # A identidade DE NEGÓCIO do evento — não o offset (que muda
            # se o dado for reprocessado por outro caminho).
            event_id = f"{msg.key().decode()}:{msg.value().decode()}"
            if event_id in seen:
                show(f"  {DIM}duplicata ignorada: {event_id}{RESET}")
                continue
            seen.add(event_id)
            applied += 1
            show(f"  processado {event_id}")
        consumer.close()  # sem commit — simula a morte antes de confirmar
        show(f"\n▸ leu {read} · aplicou {applied} · ignorou {read - applied} duplicata(s)\n")

    show(f"{BOLD}▸ a releitura aconteceu igual — mas o efeito não se repetiu{RESET}")


def main() -> None:
    """Ponto de entrada do módulo."""
    parser = argparse.ArgumentParser(description="Semânticas de entrega na API (Aula 5)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ordem = sub.add_parser("ordem", help="ordem por chave (e a quebra sem chave)")
    p_ordem.add_argument("--sem-chave", action="store_true")

    p_crash = sub.add_parser("crash", help="commit antes|depois + crash no meio do lote")
    p_crash.add_argument("--commit", choices=("antes", "depois"), required=True)

    sub.add_parser("dedup", help="duplicata inofensiva com set de ids")
    sub.add_parser("teardown", help="remove os topics da demo")

    args = parser.parse_args()
    if args.cmd == "ordem":
        scene_ordem(sem_chave=args.sem_chave)
    elif args.cmd == "crash":
        scene_crash(commit_mode=args.commit)
    elif args.cmd == "dedup":
        scene_dedup()
    else:
        delete_topics([TOPIC_ORDEM, TOPIC_SEM_CHAVE, TOPIC_SEMANTICA])
        # Os grupos guardam offsets mesmo sem o topic — sem removê-los, a
        # cena de crash acharia que a '1ª execução' já aconteceu.
        delete_groups(["liquidacao-api-antes", "liquidacao-api-depois", "auditoria-api-dedup"])


if __name__ == "__main__":
    main()
