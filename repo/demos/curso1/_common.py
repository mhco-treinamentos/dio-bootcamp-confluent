"""Utilitários compartilhados pelas demos do Curso 1.

Foco em **legibilidade na tela**: o aluno assiste a um terminal, então o output
precisa ser largo, alinhado e sem ruído de log. Nada aqui é lógica de negócio —
é apresentação.

Todas as demos do Curso 1 rodam no sandbox local (KRaft single-broker) —
ver deployment/docker/docker-compose.yml.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka.admin import AdminClient, NewTopic

from producers.config import LOCAL_BOOTSTRAP

#: Cores ANSI — cada consumidor/grupo ganha a sua, para o aluno distinguir
#: à distância quem recebeu o quê.
COLORS: tuple[str, ...] = (
    "\033[96m",  # ciano
    "\033[93m",  # amarelo
    "\033[92m",  # verde
    "\033[95m",  # magenta
)
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def color_for(index: int) -> str:
    """Cor estável para um consumidor/grupo pelo índice.

    Parameters
    ----------
    index : int
        Índice do consumidor (0-based).

    Returns
    -------
    str
        Código de cor ANSI.
    """
    return COLORS[index % len(COLORS)]


def show(*args: Any, **kwargs: Any) -> None:
    """``print`` com flush imediato — a demo é ao vivo.

    Sem ``flush=True`` o Python bufferiza a saída quando ela não é um terminal
    (pipe, ``tee``, captura de tela): o texto sairia em blocos atrasados,
    dessincronizado do que está acontecendo no broker.
    """
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)


def banner(title: str, subtitle: str = "") -> None:
    """Imprime um cabeçalho de cena, visível numa gravação.

    Parameters
    ----------
    title : str
        Título da cena.
    subtitle : str, optional
        Linha de contexto (o que observar), by default "".
    """
    line = "═" * 78
    show(f"\n{BOLD}{line}{RESET}")
    show(f"{BOLD}  {title}{RESET}")
    if subtitle:
        show(f"{DIM}  {subtitle}{RESET}")
    show(f"{BOLD}{line}{RESET}\n")


def say(message: str) -> None:
    """Mensagem de narração da demo (não é log — é fala na tela).

    Parameters
    ----------
    message : str
        Texto a exibir.
    """
    show(f"{DIM}▸ {message}{RESET}")


def create_topic(name: str, partitions: int = 3, bootstrap: str = LOCAL_BOOTSTRAP) -> None:
    """Cria um topic no sandbox local (idempotente: já existir não é erro).

    Parameters
    ----------
    name : str
        Nome do topic.
    partitions : int, optional
        Número de partições, by default 3.
    bootstrap : str, optional
        Bootstrap servers, by default ``localhost:9092``.

    Notes
    -----
    RF=1 é imposição do sandbox de 1 broker — no Confluent Cloud o default é
    RF=3 com ``min.insync.replicas=2``.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if name in existing:
        say(f"topic {name} já existe ({len(existing[name].partitions)} partições)")
        return

    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    for topic, future in futures.items():
        future.result()  # levanta se falhar
        say(f"topic {topic} criado com {partitions} partição(ões), RF=1")


def delete_topics(names: list[str], bootstrap: str = LOCAL_BOOTSTRAP) -> None:
    """Remove topics do sandbox (teardown entre cenas).

    Parameters
    ----------
    names : list[str]
        Topics a remover.
    bootstrap : str, optional
        Bootstrap servers, by default ``localhost:9092``.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    targets = [n for n in names if n in existing]
    if not targets:
        say("nada a remover")
        return
    for topic, future in admin.delete_topics(targets).items():
        try:
            future.result()
            say(f"topic {topic} removido")
        except Exception as exc:  # noqa: BLE001 — teardown não pode travar a aula
            say(f"falha ao remover {topic}: {exc}")


def delete_groups(names: list[str], bootstrap: str = LOCAL_BOOTSTRAP) -> None:
    """Remove consumer groups do sandbox (best-effort; inexistentes são ignorados).

    Necessário nos teardowns das cenas que detectam "1ª execução" pelo offset
    commitado: apagar só o topic deixaria o estado do grupo para trás.

    Parameters
    ----------
    names : list[str]
        Group ids a remover.
    bootstrap : str, optional
        Bootstrap servers, by default ``localhost:9092``.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for group, future in admin.delete_consumer_groups(names, request_timeout=10).items():
        try:
            future.result()
            say(f"grupo {group} removido")
        except Exception:  # noqa: BLE001 — grupo inexistente/ativo não é falha de teardown
            pass


def format_record(record: dict[str, Any]) -> str:
    """Resumo de uma transação em uma linha, legível na tela.

    Parameters
    ----------
    record : dict[str, Any]
        Record de transação do gerador.

    Returns
    -------
    str
        Linha compacta: id curto, canal, valor e status.
    """
    return (
        f"txn {record['transaction_id'][:8]} · {record['channel']:<6} · "
        f"R$ {record['amount']:>9} · {record['status']}"
    )
