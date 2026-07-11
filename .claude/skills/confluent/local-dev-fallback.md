# Local Dev Fallback (Docker Compose)

## Skill: `/confluent-local-dev-fallback`

Gera a stack local **enxuta** para demos offline e gravação de aulas sem gastar créditos do Confluent Cloud: **1 broker Kafka em KRaft** (sem ZooKeeper) + **Schema Registry**. Nada mais.

## Quando usar (e quando NÃO usar)

| Cenário | Fallback local? |
|---------|-----------------|
| Gravar aula demonstrando producer/consumer, partições, offsets | Sim |
| Testar geradores Python e serialização Avro com Schema Registry | Sim |
| Rodar testes de integração (testcontainers ou compose) sem rede | Sim |
| Demonstrar compatibilidade BACKWARD e evolução de schema | Sim |
| **Conectores gerenciados / CDC** | **Não — cloud-only** ([managed-connector.md](managed-connector.md)) |
| **Flink SQL (compute pools, statements)** | **Não — cloud-only** (módulo 4) |
| **Tableflow / Iceberg / catálogos** | **Não — cloud-only** ([tableflow.md](tableflow.md)) |
| **RBAC, service accounts, Metrics API** | **Não — cloud-only** (módulo 7) |

> Regra do bootcamp: tudo aponta para Confluent Cloud por padrão. O fallback local existe apenas para **topics + Schema Registry**. O seletor é a env var `KAFKA_PROFILE=cloud|local` — um único ponto de configuração, nunca código duplicado (ver [rules.md](../../rules.md)).

---

## docker-compose.yml

Single-broker KRaft (`confluentinc/cp-kafka` 7.6.x — a imagem já suporta modo KRaft, sem ZooKeeper) + Schema Registry:

```yaml
services:
  broker:
    image: confluentinc/cp-kafka:7.6.1
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
    environment:
      # KRaft: broker + controller no mesmo nó (só para dev local)
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@broker:29093"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      # Listeners: interno (29092), controller (29093), host (9092)
      KAFKA_LISTENERS: "PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      # Single-broker: RF=1 (na cloud os defaults são RF=3, min.insync.replicas=2)
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - broker-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "broker:29092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  broker-data:

networks:
  default:
    name: bootcamp-local
```

> `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` de propósito: criar topics explicitamente é parte do aprendizado (e é o comportamento do Confluent Cloud).

---

## Subir, verificar, derrubar

```bash
# Subir
docker compose up -d

# Health check: espera os dois serviços ficarem healthy
docker compose ps
docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092 | head -1
curl -s http://localhost:8081/subjects   # deve retornar [] na primeira subida

# Smoke test: criar topic, produzir, consumir
docker exec broker kafka-topics --bootstrap-server localhost:9092 \
  --create --topic lab.payments.gen.smoke.created.v1 --partitions 3 --replication-factor 1
echo 'hello-bootcamp' | docker exec -i broker kafka-console-producer \
  --bootstrap-server localhost:9092 --topic lab.payments.gen.smoke.created.v1
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic lab.payments.gen.smoke.created.v1 --from-beginning --max-messages 1

# Derrubar (mantém dados)
docker compose down

# Derrubar e limpar volumes (reset completo)
docker compose down -v
```

## Endpoints locais

| Serviço | Endpoint | Perfil |
|---------|----------|--------|
| Kafka broker | `localhost:9092` | PLAINTEXT, sem auth |
| Schema Registry | `http://localhost:8081` | REST, sem auth |

Config de producer/SR para o perfil local → [rules.md](../../rules.md) (`KAFKA_PROFILE=local`).

## Diferenças que o aluno vai notar vs. cloud

| Aspecto | Local (este compose) | Confluent Cloud |
|---------|----------------------|-----------------|
| Replication factor | 1 | 3 (fixo) |
| `min.insync.replicas` | 1 | 2 |
| Segurança | PLAINTEXT | SASL_SSL + API key |
| Schema Registry auth | Sem auth | API key/secret (`SR_API_KEY`/`SR_API_SECRET`) |
| Connect / Flink / Tableflow | **Não existem aqui** | Gerenciados |

---

**Última atualização**: 2026-07-11
