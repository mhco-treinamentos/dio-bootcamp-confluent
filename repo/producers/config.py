"""Configuração de conexão — o seletor de profile do bootcamp.

Um único ponto de configuração para ``KAFKA_PROFILE=cloud|local``, usado por
TODOS os producers (01–05) e reexportado pelos consumers. Credenciais sempre
via env vars (``CC_*``, ``SR_*``) — nunca hardcoded, nunca commitadas
(.claude/rules.md).

Âncoras de confiabilidade do bootcamp: ``acks=all`` + idempotência no producer;
RF=3 e ``min.insync.replicas=2`` nos topics do Confluent Cloud. O fallback local
(broker KRaft único, RF=1) mantém os settings do producer mas **não consegue
demonstrar as âncoras de replicação** — dizer isso em voz alta ao gravar demos.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_VALID_PROFILES = ("cloud", "local")

#: Bootstrap do fallback local (docker compose em deployment/docker/).
LOCAL_BOOTSTRAP = "localhost:9092"
#: Schema Registry do fallback local (sem auth).
LOCAL_SCHEMA_REGISTRY = "http://localhost:8081"


def active_profile(profile: str | None = None) -> str:
    """Resolve o profile ativo: argumento > ``KAFKA_PROFILE`` > detecção automática.

    Sem ``KAFKA_PROFILE`` definida, o profile é **local** — a não ser que
    ``CC_BOOTSTRAP`` exista no ambiente, indicando um `.env` de nuvem carregado.
    Isso faz o botão ▶ da IDE funcionar contra o sandbox Docker sem
    configuração, e mantém o cloud como escolha **explícita** (créditos custam).

    Parameters
    ----------
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; None consulta a env var / detecta.

    Returns
    -------
    str
        Profile resolvido.

    Raises
    ------
    ValueError
        Se o profile não for ``cloud`` nem ``local``.
    """
    default = "cloud" if os.environ.get("CC_BOOTSTRAP") else "local"
    resolved = profile or os.environ.get("KAFKA_PROFILE", default)
    if resolved not in _VALID_PROFILES:
        raise ValueError(f"Invalid KAFKA_PROFILE {resolved!r} — expected one of {_VALID_PROFILES}")
    return resolved


def build_connection_config(profile: str | None = None) -> dict[str, Any]:
    """Config de conexão + autenticação, comum a producer e consumer.

    Parameters
    ----------
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.

    Returns
    -------
    dict[str, Any]
        ``bootstrap.servers`` (+ SASL_SSL no cloud).

    Raises
    ------
    KeyError
        Se o profile cloud estiver ativo e faltar alguma env var ``CC_*``.
    """
    if active_profile(profile) == "cloud":
        return {
            "bootstrap.servers": os.environ["CC_BOOTSTRAP"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["CC_API_KEY"],
            "sasl.password": os.environ["CC_API_SECRET"],
        }
    return {"bootstrap.servers": LOCAL_BOOTSTRAP}


def build_producer_config(
    profile: str | None = None,
    client_id: str = "bootcamp-gen-payments",
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Config de producer confiável para o profile ativo.

    Parameters
    ----------
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.
    client_id : str, optional
        ``client.id`` do Kafka, by default ``"bootcamp-gen-payments"``.
    overrides : dict[str, Any] or None, optional
        Settings extras aplicados por cima (ex.: ``linger.ms=0`` em demo).

    Returns
    -------
    dict[str, Any]
        Configuração librdkafka.

    Raises
    ------
    KeyError
        Se o profile cloud estiver ativo e faltar env var ``CC_*``.
    """
    resolved = active_profile(profile)
    config: dict[str, Any] = {
        "client.id": client_id,
        # Âncoras de confiabilidade do bootcamp
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        # Performance
        "linger.ms": 5,
        "batch.size": 16384,
        "compression.type": "snappy",
    }
    config.update(build_connection_config(profile))
    config.update(overrides or {})
    logger.info("Built producer config for profile %s (client.id=%s)", resolved, client_id)
    return config


def ensure_topic(topic: str, partitions: int = 3, profile: str | None = None) -> None:
    """Cria o topic se ele não existir — idempotente.

    O sandbox local roda com ``auto.create.topics.enable=false`` (topics são
    criados explicitamente, com partições escolhidas a dedo — a lição da Aula
    3), então produzir para um topic inexistente falha silenciosamente. Os
    exemplos chamam esta função para funcionarem no botão ▶ da IDE sem
    pré-requisito.

    Parameters
    ----------
    topic : str
        Nome do topic.
    partitions : int, optional
        Partições a criar, by default 3.
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para :func:`active_profile`.

    Notes
    -----
    No Confluent Cloud, criar topic exige permissão da API key — em labs, o
    topic normalmente já foi criado pelo `setup.sh`. Falhas aqui são logadas,
    não levantadas: o produce seguinte dará o erro definitivo.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(build_connection_config(profile))
    try:
        if topic in admin.list_topics(timeout=10).topics:
            return
        # RF=1 no sandbox de 1 broker; no Confluent Cloud o default é 3.
        replication = 1 if active_profile(profile) == "local" else 3
        for name, future in admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=replication)]
        ).items():
            future.result()
            logger.info("Topic %s criado (%d partições, RF=%d)", name, partitions, replication)
    except Exception as exc:  # noqa: BLE001 — exemplo didático não morre por isso
        logger.warning("Não foi possível garantir o topic %s: %s", topic, exc)


def build_schema_registry_config(profile: str | None = None) -> dict[str, str]:
    """Config do cliente Schema Registry para o profile ativo.

    Parameters
    ----------
    profile : str or None, optional
        ``"cloud"`` ou ``"local"``; fallback para ``KAFKA_PROFILE``.

    Returns
    -------
    dict[str, str]
        Basic auth no cloud; sem auth no fallback local.
    """
    if active_profile(profile) == "cloud":
        return {
            "url": os.environ["SR_URL"],
            "basic.auth.user.info": (f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}"),
        }
    return {"url": LOCAL_SCHEMA_REGISTRY}
