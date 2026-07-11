# Instruction: subir stack Kafka local (Docker Compose)

> **Playbook RESEARCH → PLAN → ACT → VALIDATE para subir Kafka + Schema Registry + Connect localmente para uma cena do workshop. 100% Docker, sem Confluent Cloud.**

## Pré-requisitos

- Docker Desktop com ≥ 4 GB livres (sem Oracle XE) ou ≥ 8 GB (com CDC)
- `docker compose` v2+
- `kcat` (kafkacat) instalado para debug: `brew install kcat`
- Edição da tour: `data-streaming-tour-2026/<cena>/`

---

## Phase 1: RESEARCH

### 1.1 — Coletar requisitos
- Qual cena vai usar essa stack? (kafka-fundamentals, cdc, flink-sql, lakehouse)
- Quais componentes são necessários? Mínimo: Kafka + SR. CDC adiciona Kafka Connect + Oracle XE. Flink adiciona JobManager + TaskManager. Lakehouse adiciona MinIO + Nessie + Trino.
- Throughput esperado? (Workshop tipicamente < 1k msg/s — não há tuning de produção a fazer)
- Retention dos topics? (24h é suficiente para a maioria das cenas)

### 1.2 — Revisar ambiente
- Há `docker-compose.yml` em outras cenas que vale reusar?
- Quais portas estão livres? (Defaults: 9092 Kafka, 8081 SR, 8083 Connect, 9021 Control Center)

### 1.3 — Consultar references
- [references/kafka-producer-guide.md](../references/kafka-producer-guide.md) — padrões de producer
- [skills/confluent/docker-compose.md](../skills/confluent/docker-compose.md) — skill para gerar compose
- [skills/confluent/topics.md](../skills/confluent/topics.md) — criação de topics

---

## Phase 2: PLAN

### 2.1 — Topic naming (ver [.claude/rules.md](../rules.md))

```
{stage}.{domain}.{source_system}.{entity}.{event_type}.v{version}

Exemplos:
dev.payments.gen.transacoes.created.v1
dev.payments.oracle-pagamentos.transacoes.cdc.v1
dev.payments.flink.fraude.detected.v1
```

### 2.2 — Componentes da stack (matriz por cena)

| Componente | kafka-fund. | cdc | flink-sql | lakehouse |
|------------|-------------|-----|-----------|-----------|
| Kafka broker | ✓ | ✓ | ✓ | ✓ |
| Schema Registry | ✓ | ✓ | ✓ | ✓ |
| Control Center | ✓ | ✓ | ✓ | ✓ |
| Kafka Connect | — | ✓ | — | — |
| Oracle XE | — | ✓ | — | — |
| Flink JM + TM | — | — | ✓ | ✓ |
| MinIO + Nessie + Trino | — | — | — | ✓ |

### 2.3 — Producer config (ver `local_producer_config` em [.claude/rules.md](../rules.md))

---

## Phase 3: ACT

### 3.1 — Criar docker-compose.yml

**Local**: `data-streaming-tour-2026/<cena>/docker-compose.yml`

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: ws-zookeeper
    ports: ["2181:2181"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: ws-kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,HOST://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # crie topics explicitamente
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    container_name: ws-schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      retries: 5

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.6.0
    container_name: ws-control-center
    depends_on:
      kafka:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports: ["9021:9021"]
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
```

> **Atenção**: pin de versão (`7.6.0`) é obrigatório (ver [.claude/rules/workshop.md](../rules/workshop.md)). Não usar `:latest`.

### 3.2 — Script de criação de topics

**Local**: `data-streaming-tour-2026/<cena>/scripts/create_topics.py`

```python
"""Cria os topics da cena."""
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    {"name": "dev.payments.gen.transacoes.created.v1", "partitions": 6, "retention_ms": 86_400_000},
    {"name": "dev.payments.gen.contas.snapshot.v1", "partitions": 3, "retention_ms": 86_400_000},
]


def main() -> None:
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    new_topics = [
        NewTopic(
            t["name"],
            num_partitions=t["partitions"],
            replication_factor=1,
            config={"retention.ms": str(t["retention_ms"])},
        )
        for t in TOPICS
    ]
    for topic, fut in admin.create_topics(new_topics).items():
        try:
            fut.result()
            print(f"Created: {topic}")
        except Exception as e:
            print(f"Failed {topic}: {e}")


if __name__ == "__main__":
    main()
```

### 3.3 — Producer config

Em `shared/producers/kafka_producer.py` (ver [.claude/rules.md](../rules.md)):
```python
local_producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "ws-gen-payments",
    "acks": "all",
    "retries": 3,
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.size": 16384,
    "compression.type": "snappy",
}
```

---

## Phase 4: VALIDATE

### 4.1 — Subir e checar healthcheck
```bash
cd data-streaming-tour-2026/<cena>
docker compose up -d
docker compose ps
# Todos os serviços devem chegar a "healthy" em <60s
```

### 4.2 — Verificar Kafka
```bash
docker exec ws-kafka kafka-topics --bootstrap-server localhost:9092 --list
# Esperado: __consumer_offsets, _schemas (ou vazio se ainda sem topics de negócio)
```

### 4.3 — Verificar Schema Registry
```bash
curl http://localhost:8081/subjects
# Esperado: []
```

### 4.4 — Criar topics
```bash
python data-streaming-tour-2026/<cena>/scripts/create_topics.py
# Esperado: linhas "Created: dev.payments..."
```

### 4.5 — Smoke test producer → consumer
```bash
# Em um terminal — consumer
kcat -C -b localhost:9092 -t dev.payments.gen.transacoes.created.v1 -o beginning

# Em outro — produzir 10 mensagens
python -m shared.generators.payments.transacoes --count 10
```

### 4.6 — Abrir Control Center
```bash
open http://localhost:9021
```

---

## Cleanup

```bash
# Cleanup leve (mantém dados em volumes)
docker compose down

# Cleanup completo (apaga volumes)
docker compose down -v
# Tempo esperado: ~30s para stack base; ~1 min com Oracle XE
```

---

## Checklist final

- [ ] `docker-compose.yml` com versões pinned (sem `latest`)
- [ ] Healthchecks em todos os serviços principais
- [ ] Script `create_topics.py` cria topics da cena com retention adequada
- [ ] Producer/consumer smoke test passou
- [ ] Control Center acessível em :9021
- [ ] Cena documentada com comandos copy-pasteable (regra `workshop.md`)
- [ ] CHANGELOG atualizado

---

## Exemplo de uso

```
"Siga kafka-setup para subir a stack base (Kafka + SR + Control Center)
da cena de Kafka fundamentals em data-streaming-tour-2026/"
```
