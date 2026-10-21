"""Configuração de consumer — reusa o seletor de profile dos producers.

Um consumer no Kafka é definido por três decisões que este módulo torna
explícitas (e que os exemplos 01–05 exploram uma a uma):

1. ``group.id`` — quem compartilha o trabalho (e os offsets)
2. ``auto.offset.reset`` — o que fazer quando não há offset commitado
3. ``enable.auto.commit`` — quem decide que a mensagem foi processada

A decisão (3) é a que separa at-most-once de at-least-once (Curso 1, Aula 5).
"""

from __future__ import annotations

import logging
from typing import Any

from producers.config import active_profile, build_connection_config

logger = logging.getLogger(__name__)


def build_consumer_config(
    group_id: str,
    profile: str | None = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Config de consumer para o profile ativo.

    Parameters
    ----------
    group_id : str
        Consumer group — a unidade de paralelismo e de offset no Kafka.
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.
    auto_offset_reset : str, optional
        ``"earliest"`` (lê o histórico — default dos labs) ou ``"latest"``
        (só o que chegar a partir de agora), by default ``"earliest"``.
    enable_auto_commit : bool, optional
        **False por default no bootcamp**: commit manual APÓS processar é o
        que garante at-least-once. Com auto-commit, uma falha entre o commit
        e o processamento perde a mensagem (at-most-once), by default False.
    overrides : dict[str, Any] or None, optional
        Settings librdkafka extras.

    Returns
    -------
    dict[str, Any]
        Configuração librdkafka do consumer.

    Raises
    ------
    KeyError
        Se o profile cloud estiver ativo e faltar env var ``CC_*``.
    """
    resolved = active_profile(profile)
    config: dict[str, Any] = {
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
        # Rebalance cooperativo: partições migram sem stop-the-world no grupo.
        # É o default certo em produção — mas reatribui em rodadas incrementais,
        # então a demo da Aula 4 sobrescreve para `range` (divisão em um passo,
        # mais legível na tela).
        "partition.assignment.strategy": "cooperative-sticky",
    }
    config.update(build_connection_config(profile))
    config.update(overrides or {})
    logger.info("Built consumer config for profile %s (group.id=%s)", resolved, group_id)
    return config
