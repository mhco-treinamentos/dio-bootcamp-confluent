"""Aula 4 · A Producer API por dentro — anatomia de um producer confluent-kafka.

Este arquivo é **material de leitura tanto quanto de execução**: a config é
construída campo a campo, na frente do aluno, sem factory escondendo nada.
(A versão de produção, com profile cloud/local e env vars, vive em
``producers/config.py`` — compare os dois depois.)

Três cenas, escolhidas com ``--cena``:

``anatomia`` (default)
    1. ``produce()`` é **assíncrono** — enfileira e retorna na hora
    2. quem confirma a entrega é o **delivery callback**, com partição e offset
    3. ``flush()`` é a barreira final — e o retorno dele DEVE ser checado

``envios``
    Os três padrões de envio, cronometrados sobre o mesmo lote:
    **fire-and-forget** (não espera nem sabe), **síncrono** (espera cada
    mensagem — 1 round-trip por mensagem) e **assíncrono** (callback + um
    flush no fim — rápido E confirmado).

``acks``
    A escada de durabilidade: ``acks=0`` (nem o offset volta), ``acks=1``
    (o líder confirmou) e ``acks=all`` (as réplicas in-sync confirmaram).
    Fecha provando que ``enable.idempotence`` EXIGE ``acks=all`` — o client
    se recusa a criar o producer com a combinação inválida.

Uso
---
::

    make aula4-api-producer          # cena anatomia, ou:
    python -m demos.curso1.aula4_api_producer
    python -m demos.curso1.aula4_api_producer --cena envios
    python -m demos.curso1.aula4_api_producer --cena acks
    python -m demos.curso1.aula4_api_producer --teardown
"""

from __future__ import annotations

import argparse
import time

from confluent_kafka import KafkaException, Message, Producer

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

TOPIC = "lab.payments.api.aula4.v1"

#: As 6 transações da demo — as mesmas que você digitou no CLI (Passo 1).
TRANSACTIONS: tuple[tuple[str, str], ...] = (
    ("txn-1", "AUTHORIZED 150.00"),
    ("txn-2", "AUTHORIZED 89.90"),
    ("txn-3", "DECLINED 1250.00"),
    ("txn-4", "SETTLED 42.00"),
    ("txn-5", "AUTHORIZED 310.50"),
    ("txn-6", "DECLINED 75.00"),
)


def build_config() -> dict[str, object]:
    """Monta a config do producer, campo a campo.

    Returns
    -------
    dict[str, object]
        Configuração librdkafka pronta para ``Producer(...)``.
    """
    return {
        # Onde encontrar o cluster. UM endereço basta: o client descobre os
        # demais brokers sozinho (aqui só há um — sandbox KRaft).
        "bootstrap.servers": LOCAL_BOOTSTRAP,
        # Como este client aparece em métricas e logs do broker.
        "client.id": "aula4-api-producer",
        # A âncora de confiabilidade: só considera escrito quando TODAS as
        # réplicas in-sync confirmarem. No sandbox (RF=1) é trivial; no
        # Confluent Cloud (RF=3) é o que segura a durabilidade.
        "acks": "all",
        # O retry do próprio producer não duplica mensagens no broker
        # (PID + número de sequência — a Aula 5 explica o porquê).
        "enable.idempotence": True,
        # Sem espera artificial para formar lotes — demo pequena, latência
        # mínima. Em produção, alguns ms de linger aumentam o throughput.
        "linger.ms": 0,
    }


def delivery_report(err: object, msg: Message) -> None:
    """Callback chamado UMA vez por mensagem, com o veredito da entrega.

    Este é o coração do modelo assíncrono: ``produce()`` não retorna sucesso —
    sucesso chega DEPOIS, aqui, quando o broker confirma (ou não).

    Parameters
    ----------
    err : object
        ``None`` em caso de sucesso; um ``KafkaError`` em caso de falha.
    msg : Message
        A mensagem confirmada (com partição e offset atribuídos pelo broker).
    """
    if err is not None:
        show(f"  ✗ FALHOU  {msg.key().decode()}: {err}")
        return
    key = msg.key().decode() if msg.key() else "(sem chave)"
    show(
        f"  ✓ entregue  {BOLD}{key}{RESET}" f" → Partition:{msg.partition()} Offset:{msg.offset()}"
    )


# ---------------------------------------------------------------------------
# Cena 1 — anatomia: produce assíncrono, callback, flush
# ---------------------------------------------------------------------------
def scene_anatomia() -> None:
    """Produz as 6 transações e mostra o ciclo assíncrono completo."""
    banner(
        "Aula 4 · Producer API",
        "produce() enfileira → callback confirma → flush() é a barreira",
    )
    create_topic(TOPIC, partitions=3)

    producer = Producer(build_config())

    show(f"{BOLD}1) produce() — enfileira e RETORNA NA HORA:{RESET}")
    for key, value in TRANSACTIONS:
        producer.produce(
            topic=TOPIC,
            # Para o broker, chave e valor são SÓ BYTES — serializar é papel
            # do client (o Curso 2 troca isto por Avro + Schema Registry).
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            on_delivery=delivery_report,
        )
        show(f"  → enfileirada {key}  {DIM}(nenhuma confirmação ainda){RESET}")
        # Serve os callbacks de entregas já confirmadas, sem bloquear.
        producer.poll(0)

    show(f"\n{BOLD}2) flush() — espera tudo que está na fila ser confirmado:{RESET}")
    pending = producer.flush(timeout=10)

    # A lição do bug corrigido no json_producer: flush() retorna quantas
    # mensagens NÃO foram entregues. Ignorar esse número é reportar sucesso
    # com mensagem perdida.
    if pending > 0:
        show(f"  ✗ {pending} mensagem(ns) NÃO confirmadas — broker fora do ar?")
        raise SystemExit(1)
    show("  ✓ fila vazia — todas as entregas confirmadas pelo broker")

    show(
        f"\n{DIM}Repare na ordem do output: os '→ enfileirada' saem primeiro,"
        f" os '✓ entregue' chegam depois, conforme o broker confirma."
        f" produce() não espera ninguém.{RESET}"
    )


# ---------------------------------------------------------------------------
# Cena 2 — os três padrões de envio, cronometrados
# ---------------------------------------------------------------------------
def scene_envios() -> None:
    """Fire-and-forget, síncrono e assíncrono sobre o mesmo lote, com tempos."""
    banner(
        "Aula 4 · Os três jeitos de enviar",
        "mesmo lote de 6 mensagens — muda só o quanto você espera (e sabe)",
    )
    create_topic(TOPIC, partitions=3)
    results: list[tuple[str, float, str]] = []

    # ---- 1) FIRE-AND-FORGET: sem callback, sem espera, sem saber ----------
    show(f"{BOLD}1) fire-and-forget — produce() e pronto:{RESET}")
    producer = Producer(build_config())
    t0 = time.perf_counter()
    for key, value in TRANSACTIONS:
        producer.produce(TOPIC, key=key.encode(), value=value.encode())  # SEM on_delivery
    dt_fire = (time.perf_counter() - t0) * 1000
    show(f"   {len(TRANSACTIONS)} produce() em {dt_fire:.1f} ms — e {BOLD}zero{RESET} confirmações")
    show(f"   {DIM}se uma falhasse (topic errado, broker fora), NINGUÉM ficaria sabendo{RESET}")
    # Higiene do script (fora do cronômetro): sem este flush, o processo
    # poderia morrer com mensagens ainda no buffer local — perda silenciosa,
    # que é exatamente o risco do padrão.
    producer.flush(timeout=10)
    results.append(("fire-and-forget", dt_fire, "nenhuma — falha é invisível"))

    # ---- 2) SÍNCRONO: espera a confirmação de CADA mensagem ---------------
    show(f"\n{BOLD}2) síncrono — produce() + flush() a cada mensagem:{RESET}")
    producer = Producer(build_config())
    confirmed: list[str] = []

    def quiet_report(err: object, msg: Message) -> None:
        if err is None:
            confirmed.append(msg.key().decode())

    t0 = time.perf_counter()
    for key, value in TRANSACTIONS:
        producer.produce(TOPIC, key=key.encode(), value=value.encode(), on_delivery=quiet_report)
        # A barreira POR MENSAGEM: um round-trip completo ao broker antes da
        # próxima. (Na API Java seria producer.send(...).get() — mesmo custo.)
        producer.flush(timeout=10)
    dt_sync = (time.perf_counter() - t0) * 1000
    show(f"   {len(confirmed)} confirmadas, uma a uma, em {dt_sync:.1f} ms")
    show(f"   {DIM}latência = soma dos round-trips; o throughput morre aqui{RESET}")
    results.append(("síncrono", dt_sync, "cada mensagem, antes da próxima"))

    # ---- 3) ASSÍNCRONO: callback + UM flush no fim -------------------------
    show(f"\n{BOLD}3) assíncrono — callback por mensagem, flush no fim:{RESET}")
    producer = Producer(build_config())
    confirmed.clear()
    t0 = time.perf_counter()
    for key, value in TRANSACTIONS:
        producer.produce(TOPIC, key=key.encode(), value=value.encode(), on_delivery=quiet_report)
        producer.poll(0)  # serve callbacks já prontos, sem bloquear
    producer.flush(timeout=10)  # UMA barreira, para o lote inteiro
    dt_async = (time.perf_counter() - t0) * 1000
    show(f"   {len(confirmed)} confirmadas em lote em {dt_async:.1f} ms")
    results.append(("assíncrono", dt_async, "todas — via callback"))

    # ---- resumo ------------------------------------------------------------
    show(f"\n{BOLD}{'padrão':<17}{'tempo':>10}   confirmação{RESET}")
    for name, dt, guarantee in results:
        show(f"{name:<17}{dt:>8.1f} ms   {guarantee}")
    show(
        f"\n{DIM}O assíncrono é o padrão de produção: velocidade de fire-and-forget,"
        f" garantia de síncrono. Os outros dois são trade-offs conscientes —"
        f" métrica descartável × mensagem que NÃO PODE sair da ordem do fluxo.{RESET}"
    )


# ---------------------------------------------------------------------------
# Cena 3 — a escada de acks
# ---------------------------------------------------------------------------
def scene_acks() -> None:
    """Produz o mesmo lote com acks=0, 1 e all — e mostra o que cada um devolve."""
    banner(
        "Aula 4 · acks — o contrato de durabilidade",
        "quantas confirmações você exige antes de chamar de 'escrito'?",
    )
    create_topic(TOPIC, partitions=3)

    explain = {
        "0": "não espera NINGUÉM — nem o líder",
        "1": "o LÍDER escreveu (réplicas ainda não)",
        "all": "todas as réplicas IN-SYNC escreveram",
    }
    for level in ("0", "1", "all"):
        config: dict[str, object] = {
            "bootstrap.servers": LOCAL_BOOTSTRAP,
            "client.id": f"aula4-api-acks-{level}",
            "acks": level,
            "linger.ms": 0,
            # SEM enable.idempotence: ele exige acks=all (prova no fim).
        }
        producer = Producer(config)
        offsets: list[int] = []

        def report(err: object, msg: Message, _offsets: list[int] = offsets) -> None:
            if err is None:
                _offsets.append(msg.offset())

        t0 = time.perf_counter()
        for key, value in TRANSACTIONS:
            producer.produce(TOPIC, key=key.encode(), value=value.encode(), on_delivery=report)
            producer.poll(0)
        producer.flush(timeout=10)
        dt = (time.perf_counter() - t0) * 1000

        show(f"{BOLD}acks={level:<4}{RESET} — {explain[level]}")
        show(f"   offsets no delivery report: {offsets}  ({dt:.1f} ms)")
        if level == "0":
            show(
                f"   {DIM}↑ repare: SEM offsets reais — o broker nunca respondeu."
                f" O report só diz 'saiu do client'.{RESET}"
            )
        show("")

    show(
        f"{DIM}No sandbox (1 broker, RF=1) acks=1 e acks=all são equivalentes na prática."
        f"\nNo Confluent Cloud (RF=3, min.insync.replicas=2), acks=all é o que garante"
        f"\nque a mensagem sobrevive à queda do líder — a âncora da Aula 5.{RESET}\n"
    )

    # ---- a prova final: idempotência EXIGE acks=all ------------------------
    show(f"{BOLD}E se eu pedir enable.idempotence com acks=1?{RESET}")
    try:
        Producer(
            {
                "bootstrap.servers": LOCAL_BOOTSTRAP,
                "acks": "1",
                "enable.idempotence": True,
            }
        )
        show("  (criou?! — versão do librdkafka mudou o comportamento)")
    except KafkaException as exc:
        show(f"  ✗ o client RECUSA: {exc}")
        show(
            f"  {DIM}idempotência sem confirmação de todas as réplicas seria promessa"
            f" furada — o client nem deixa você tentar.{RESET}"
        )


def main() -> None:
    """Ponto de entrada do módulo."""
    parser = argparse.ArgumentParser(description="Anatomia da Producer API (Aula 4)")
    parser.add_argument(
        "--cena",
        choices=("anatomia", "envios", "acks"),
        default="anatomia",
        help="qual cena rodar (default: anatomia)",
    )
    parser.add_argument("--teardown", action="store_true", help="remove o topic da demo")
    args = parser.parse_args()

    if args.teardown:
        delete_topics([TOPIC])
        delete_groups(
            [
                "app-pagamentos-api",
                "aula4-api-commits-auto-commit",
                "aula4-api-commits-manual",
                "aula4-api-reset-earliest",
                "aula4-api-reset-latest",
                "aula4-api-assign",
            ]
        )
        return
    if args.cena == "anatomia":
        scene_anatomia()
    elif args.cena == "envios":
        scene_envios()
    else:
        scene_acks()


if __name__ == "__main__":
    main()
