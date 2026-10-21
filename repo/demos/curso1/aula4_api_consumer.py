"""Aula 4 · A Consumer API por dentro — grupo, rebalance, commit e leitura manual.

O gêmeo do :mod:`aula4_api_producer`: a config montada campo a campo, e as
três decisões que definem um consumer explicitadas uma a uma:

1. ``group.id``           — quem divide o trabalho (e os offsets)
2. ``auto.offset.reset``  — onde começar quando o grupo é novo
3. ``enable.auto.commit`` — QUEM decide que a mensagem foi processada

Quatro cenas, escolhidas com ``--cena``:

``anatomia`` (default)
    O loop clássico: ``subscribe`` com callbacks de rebalance impressos
    (``on_assign``/``on_revoke``), ``poll`` e commit manual. Rode duas
    instâncias (``--nome A`` e ``--nome B``) e veja o rebalance ao vivo.

``commits``
    Os três jeitos de commitar, cronometrados sobre o mesmo lote:
    **auto-commit** (o timer decide), **manual síncrono** (um round-trip por
    mensagem) e **manual assíncrono** (rápido, com barreira final).

``reset``
    ``auto.offset.reset`` em ação: um grupo novo com ``earliest`` lê o
    histórico inteiro; com ``latest``, só o que chegar DEPOIS dele — a cena
    produz 2 mensagens novas para provar.

``assign``
    Leitura manual SEM grupo: ``assign()`` numa partição e offset escolhidos
    por você — o par do ``--partition 0`` do CLI (Aula 3) e a base de
    qualquer ferramenta de inspeção/replay.

``lag``
    O grupo por dentro, via API: ``committed()`` + ``get_watermark_offsets()``
    montam a tabela CURRENT-OFFSET / LOG-END-OFFSET / LAG por partição.

Uso
---
::

    python -m demos.curso1.aula4_api_consumer --nome A          # anatomia
    python -m demos.curso1.aula4_api_consumer --cena commits
    python -m demos.curso1.aula4_api_consumer --cena reset
    python -m demos.curso1.aula4_api_consumer --cena assign
    python -m demos.curso1.aula4_api_consumer --cena lag
"""

from __future__ import annotations

import argparse
import time

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from demos.curso1._common import BOLD, DIM, RESET, banner, color_for, show
from demos.curso1.aula4_api_producer import TOPIC, TRANSACTIONS
from producers.config import LOCAL_BOOTSTRAP

GROUP_ID = "app-pagamentos-api"


def build_config() -> dict[str, object]:
    """Monta a config do consumer — as três decisões, explícitas.

    Returns
    -------
    dict[str, object]
        Configuração librdkafka pronta para ``Consumer(...)``.
    """
    return {
        "bootstrap.servers": LOCAL_BOOTSTRAP,
        # DECISÃO 1 — o grupo. Instâncias com o MESMO group.id dividem as
        # partições entre si (fila); um group.id diferente recebe tudo de
        # novo (streaming). É a string que mudou a semântica na Aula 2.
        "group.id": GROUP_ID,
        # DECISÃO 2 — grupo novo, sem offset commitado: começa do início
        # ("earliest") ou só do que chegar agora ("latest")?
        "auto.offset.reset": "earliest",
        # DECISÃO 3 — commit MANUAL. Com auto.commit (default!), a posição é
        # confirmada por um timer a cada 5s, processada ou não. Desligar é o
        # que permite escolher: commit APÓS processar = at-least-once.
        "enable.auto.commit": False,
        # Timeout curto para o rebalance da demo acontecer rápido quando um
        # consumidor morre de forma abrupta (default: 45s).
        "session.timeout.ms": 6000,
    }


def _ensure_seed() -> None:
    """Garante o topic com o lote de 6 mensagens (idempotente entre cenas)."""
    from demos.curso1._common import create_topic

    create_topic(TOPIC, partitions=3)
    probe = Consumer({"bootstrap.servers": LOCAL_BOOTSTRAP, "group.id": "aula4-api-probe"})
    total = 0
    # Topic recém-criado pode ainda estar elegendo líderes — retry curto.
    for attempt in range(10):
        try:
            total = 0
            for p in range(3):
                _lo, hi = probe.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
                total += hi
            break
        except KafkaException:
            if attempt == 9:
                probe.close()
                raise
            time.sleep(1)
    probe.close()
    if total >= len(TRANSACTIONS):
        return
    producer = Producer({"bootstrap.servers": LOCAL_BOOTSTRAP, "enable.idempotence": True})
    for key, value in TRANSACTIONS:
        producer.produce(TOPIC, key=key.encode(), value=value.encode())
    if producer.flush(timeout=10) > 0:
        raise SystemExit("seed falhou: flush() reportou pendências")
    show(f"{DIM}(topic semeado com {len(TRANSACTIONS)} mensagens){RESET}\n")


# ---------------------------------------------------------------------------
# Cena 1 — anatomia: subscribe, rebalance visível, commit manual
# ---------------------------------------------------------------------------
def scene_anatomia(nome: str) -> None:
    """Loop do consumidor: subscribe → poll → processa → commit.

    Parameters
    ----------
    nome : str
        Apelido desta instância (A, B, ...), para distinguir os terminais.
    """
    color = color_for(ord(nome[0]) % 4)
    tag = f"{color}{BOLD}[consumidor {nome}]{RESET}"

    banner(
        f"Aula 4 · Consumer API — instância {nome}",
        f"grupo '{GROUP_ID}' · commit manual · Ctrl+C para sair",
    )

    consumer = Consumer(build_config())

    # ------------------------------------------------------------------
    # Os callbacks de rebalance: o coordinator avisa ANTES de tirar
    # partições (revoke) e DEPOIS de entregar novas (assign).
    # ------------------------------------------------------------------
    def on_assign(_c: Consumer, parts: list[TopicPartition]) -> None:
        nums = ", ".join(str(p.partition) for p in parts) or "nenhuma"
        show(f"{tag} ⇄ REBALANCE — recebeu partição(ões): {nums}")

    def on_revoke(_c: Consumer, parts: list[TopicPartition]) -> None:
        nums = ", ".join(str(p.partition) for p in parts) or "nenhuma"
        show(f"{tag} ⇄ REBALANCE — devolveu partição(ões): {nums}")

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    show(f"{tag} inscrito em {TOPIC} — aguardando atribuição...\n")

    processed = 0
    try:
        while True:
            # poll() é o coração do consumer: busca mensagens E mantém o
            # heartbeat com o grupo. Um consumer que para de chamar poll()
            # é declarado morto e perde as partições.
            msg: Message | None = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # nada novo neste intervalo — segue vivo
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # fim da partição não é erro
                show(f"{tag} ✗ erro: {msg.error()}")
                continue

            # ---- "processamento" (aqui, só exibir) -----------------------
            key = msg.key().decode() if msg.key() else "(sem chave)"
            value = msg.value().decode() if msg.value() else ""
            show(
                f"{tag} Partition:{msg.partition()} Offset:{msg.offset()}"
                f"  {BOLD}{key}{RESET}  {value}"
            )
            processed += 1

            # ---- commit DEPOIS de processar = at-least-once --------------
            # Se o processo morrer entre o processamento e esta linha, a
            # mensagem será relida (duplicata — Aula 5). Commitar ANTES
            # seria at-most-once: sem duplicata, com risco de perda.
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        show(f"\n{tag} encerrando — processou {processed} mensagem(ns)")
    finally:
        # close() commita o que estiver pendente e SAI DO GRUPO de forma
        # limpa → o rebalance no outro terminal é imediato, sem esperar
        # o session.timeout.
        consumer.close()
        show(f"{tag} {DIM}saiu do grupo (as partições já foram redistribuídas){RESET}")


# ---------------------------------------------------------------------------
# Cena 2 — os três jeitos de commitar, cronometrados
# ---------------------------------------------------------------------------
def scene_commits() -> None:
    """Auto-commit, manual síncrono e manual assíncrono sobre o mesmo lote."""
    banner(
        "Aula 4 · Os três jeitos de commitar",
        "mesmo lote de 6 mensagens — muda quem confirma o progresso (e quando)",
    )
    _ensure_seed()

    modes: tuple[tuple[str, dict[str, object], str], ...] = (
        (
            "auto-commit",
            {"enable.auto.commit": True, "auto.commit.interval.ms": 5000},
            "um TIMER commita a cada 5s — processado ou não",
        ),
        (
            "manual síncrono",
            {"enable.auto.commit": False},
            "commit(msg) bloqueante após CADA mensagem",
        ),
        (
            "manual assíncrono",
            {"enable.auto.commit": False},
            "commit(msg, async) por mensagem + barreira no close()",
        ),
    )
    results: list[tuple[str, float]] = []

    for name, overrides, description in modes:
        config: dict[str, object] = {
            "bootstrap.servers": LOCAL_BOOTSTRAP,
            "group.id": f"aula4-api-commits-{name.split()[0]}",
            **overrides,
        }
        consumer = Consumer(config)
        # assign() com offset explícito: a cena relê SEMPRE o lote inteiro,
        # independentemente do que o grupo commitou em execuções passadas.
        consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in range(3)])

        show(f"{BOLD}{name}{RESET} — {description}")

        # Fase 1 — consumir o lote (fetch inicial custa o mesmo nos três
        # modos; medir junto esconderia a diferença que interessa).
        batch: list[Message] = []
        while len(batch) < len(TRANSACTIONS):
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if not msg.error():
                batch.append(msg)  # "processa"

        # Fase 2 — SÓ os commits, cronometrados.
        t0 = time.perf_counter()
        for msg in batch:
            if name == "manual síncrono":
                # Um round-trip ao broker POR MENSAGEM — o análogo do
                # producer síncrono, com o mesmo preço.
                consumer.commit(message=msg, asynchronous=False)
            elif name == "manual assíncrono":
                # Enfileira o commit e segue — rápido; a barreira fica
                # para o close() (que faz um commit final síncrono).
                consumer.commit(message=msg, asynchronous=True)
            # auto-commit: NENHUMA linha de commit — o timer (e o close())
            # confirmam por você. Conveniente, e é exatamente aí que mora
            # a janela de 5s do Passo 6.
        consumer.close()  # barreira final (e o commit do auto/async pendente)
        dt = (time.perf_counter() - t0) * 1000
        calls = 0 if name == "auto-commit" else len(batch)
        show(f"   {len(batch)} processadas · {calls} chamadas de commit · {dt:.1f} ms\n")
        results.append((name, dt))

    show(f"{BOLD}{'modo':<20}{'commits':>10}{RESET}")
    for name, dt in results:
        show(f"{name:<20}{dt:>8.1f} ms")
    show(
        f"\n{DIM}O padrão de produção é o do meio-termo: commit manual (você decide"
        f" QUANDO — depois de processar), assíncrono no loop, síncrono só na saída."
        f" Auto-commit é aceitável quando reprocessar é barato; o síncrono por"
        f" mensagem, só quando cada offset commitado é sagrado.{RESET}"
    )


# ---------------------------------------------------------------------------
# Cena 3 — auto.offset.reset: earliest × latest
# ---------------------------------------------------------------------------
def scene_reset() -> None:
    """Grupo novo com earliest lê o histórico; com latest, só o futuro."""
    banner(
        "Aula 4 · auto.offset.reset — de onde um grupo NOVO começa",
        "earliest = o histórico inteiro · latest = só o que chegar depois",
    )
    _ensure_seed()

    def read_all(consumer: Consumer, wait: float) -> list[str]:
        got: list[str] = []
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg is None or msg.error():
                continue
            got.append(msg.key().decode())
        return got

    # Nenhum dos grupos commita NUNCA (auto.commit off, sem commit manual):
    # assim o auto.offset.reset vale de novo em toda execução da cena.
    base: dict[str, object] = {
        "bootstrap.servers": LOCAL_BOOTSTRAP,
        "enable.auto.commit": False,
    }

    show(f"{BOLD}1) grupo novo com auto.offset.reset=earliest:{RESET}")
    earliest = Consumer(
        {**base, "group.id": "aula4-api-reset-earliest", "auto.offset.reset": "earliest"}
    )
    earliest.subscribe([TOPIC])
    got = read_all(earliest, wait=6.0)
    earliest.close()
    show(f"   leu {len(got)} mensagens do HISTÓRICO: {got}\n")

    show(f"{BOLD}2) grupo novo com auto.offset.reset=latest:{RESET}")
    latest = Consumer({**base, "group.id": "aula4-api-reset-latest", "auto.offset.reset": "latest"})
    latest.subscribe([TOPIC])
    # Espera a atribuição de partições ANTES de produzir as novas — sem
    # isso, as "novas" chegariam antes de o consumer marcar sua posição.
    while not latest.assignment():
        latest.poll(timeout=0.5)
    show("   partições atribuídas; o histórico ficou para trás. Produzindo 2 novas...")
    producer = Producer({"bootstrap.servers": LOCAL_BOOTSTRAP, "enable.idempotence": True})
    for key, value in (("txn-7", "AUTHORIZED 500.00"), ("txn-8", "SETTLED 22.10")):
        producer.produce(TOPIC, key=key.encode(), value=value.encode())
    producer.flush(timeout=10)
    got = read_all(latest, wait=6.0)
    latest.close()
    show(f"   leu {len(got)} mensagens — SÓ as novas: {got}\n")

    show(
        f"{DIM}A pegadinha clássica: 'meu consumidor não lê nada!' — é um grupo novo"
        f" com latest (o default!) num topic onde ninguém produz. E o inverso:"
        f" earliest num topic de anos = reprocessar anos no primeiro deploy."
        f"\nIMPORTANTE: o reset só vale para grupo SEM offset commitado — depois do"
        f" primeiro commit, quem manda é o offset do grupo.{RESET}"
    )


# ---------------------------------------------------------------------------
# Cena 4 — assign: leitura manual, sem grupo
# ---------------------------------------------------------------------------
def scene_assign() -> None:
    """Lê uma partição e offset escolhidos a dedo — sem rebalance, sem grupo."""
    banner(
        "Aula 4 · assign() — leitura manual de partição",
        "sem subscribe, sem rebalance: VOCÊ escolhe partição e offset",
    )
    _ensure_seed()

    config: dict[str, object] = {
        "bootstrap.servers": LOCAL_BOOTSTRAP,
        # group.id é obrigatório na config, mas com assign() ele não
        # coordena nada: sem rebalance, sem divisão de trabalho.
        "group.id": "aula4-api-assign",
        "enable.auto.commit": False,
    }

    def read_partition(start_offset: int, label: str) -> None:
        show(f"{BOLD}{label}{RESET}")
        consumer = Consumer(config)
        consumer.assign([TopicPartition(TOPIC, 0, start_offset)])
        while True:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                break
            if msg.error():
                continue
            key, value = msg.key().decode(), msg.value().decode()
            show(f"   Partition:0 Offset:{msg.offset()}  {key}  {value}")
        consumer.close()
        show("")

    read_partition(OFFSET_BEGINNING, "1) partição 0, do início (OFFSET_BEGINNING):")
    read_partition(1, "2) partição 0, a partir do offset 1 — replay parcial:")

    show(
        f"{DIM}É o par do '--partition 0' do CLI (Aula 3) e a base de qualquer"
        f" ferramenta de inspeção: nenhum on_assign disparou, nenhum grupo se"
        f" reorganizou — a posição é 100% sua. O preço: paralelismo, failover e"
        f" progresso commitado viram problema SEU (é o que o grupo dava de graça).{RESET}"
    )


# ---------------------------------------------------------------------------
# Cena 5 — o grupo por dentro: offsets e LAG via API
# ---------------------------------------------------------------------------
def scene_lag() -> None:
    """CURRENT-OFFSET, LOG-END-OFFSET e LAG por partição — lidos pela API."""
    banner(
        f"Aula 4 · O grupo '{GROUP_ID}' por dentro",
        "committed() + get_watermark_offsets() = a tabela de LAG, sem CLI",
    )
    _ensure_seed()

    # Este consumer NÃO entra no grupo (nenhum subscribe/assign): ele só
    # PERGUNTA ao coordinator onde o grupo está — é um observador.
    observer = Consumer(
        {
            "bootstrap.servers": LOCAL_BOOTSTRAP,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        }
    )
    tps = [TopicPartition(TOPIC, p) for p in range(3)]
    committed = observer.committed(tps, timeout=10)

    show(f"{BOLD}{'PARTITION':<11}{'CURRENT-OFFSET':>15}{'LOG-END-OFFSET':>16}{'LAG':>7}{RESET}")
    total_lag = 0
    for tp in committed:
        _lo, hi = observer.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)
        has_commit = tp.offset >= 0
        current = tp.offset if has_commit else 0
        lag = hi - current
        total_lag += lag
        cur_txt = str(tp.offset) if has_commit else "-"
        show(f"{tp.partition:<11}{cur_txt:>15}{hi:>16}{lag:>7}")
    observer.close()

    show(
        f"\n{BOLD}LAG total: {total_lag}{RESET}"
        + (
            f"  {DIM}— o consumo alcançou a produção{RESET}"
            if total_lag == 0
            else f"  {DIM}— há mensagens que o grupo ainda não confirmou{RESET}"
        )
    )
    show(
        f"\n{DIM}As três colunas: CURRENT-OFFSET (committed do grupo, '-' = nunca"
        f" commitou), LOG-END-OFFSET (a watermark alta do log) e LAG (a diferença)."
        f"\nExperimente: rode 'make aula4-api-envios' (produz mais 18) e esta cena de"
        f" novo — o LAG sobe. Rode o consumidor A e volte — o LAG zera.{RESET}"
    )


def main() -> None:
    """Ponto de entrada do módulo."""
    parser = argparse.ArgumentParser(description="Anatomia da Consumer API (Aula 4)")
    parser.add_argument(
        "--cena",
        choices=("anatomia", "commits", "reset", "assign", "lag"),
        default="anatomia",
        help="qual cena rodar (default: anatomia)",
    )
    parser.add_argument("--nome", default="A", help="apelido da instância (cena anatomia)")
    args = parser.parse_args()

    if args.cena == "anatomia":
        scene_anatomia(args.nome)
    elif args.cena == "commits":
        scene_commits()
    elif args.cena == "reset":
        scene_reset()
    elif args.cena == "lag":
        scene_lag()
    else:
        scene_assign()


if __name__ == "__main__":
    main()
