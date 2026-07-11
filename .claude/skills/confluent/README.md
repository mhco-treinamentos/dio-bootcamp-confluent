# Confluent Platform Skills

Skills para subir e gerenciar a stack local Confluent Platform / Kafka no workshop (100% Docker Compose).

## Skills

| Skill | Command | Descrição |
|-------|---------|-----------|
| Docker Compose | `/confluent-docker-compose` | Gerar `docker-compose.yml` da stack (Kafka, SR, Connect, Oracle XE) |
| Kafka Connect | `/confluent-connect` | Deploy e gerenciamento de connectors |
| Oracle CDC | `/confluent-oracle-cdc` | Deploy de Debezium / Oracle CDC Source contra Oracle XE local |
| Schema Registry | `/confluent-schema` | Gerenciar schemas Avro/JSON/Protobuf |
| Topics | `/confluent-topics` | Criar e gerenciar topics |
| Troubleshoot | `/confluent-troubleshoot` | Debug de issues comuns |

> Skills específicas de Confluent Cloud (environments, networking PrivateLink, RBAC Cloud, Terraform Confluent) **foram removidas** — este workshop é 100% Docker self-managed. Para conteúdo de Cloud, consultar a documentação oficial Confluent.

## Quick start

### 1. Subir a stack
```
"Use docker-compose para gerar a stack do ws-01 (Kafka, Zookeeper, Schema Registry, Control Center)"
```

### 2. Criar topics
```
"Use topics para criar dev.payments.gen.transacoes.created.v1 com 6 partições e retention de 7 dias"
```

### 3. Deploy do Oracle CDC connector
```
"Use oracle-cdc para configurar Debezium no Oracle XE capturando PAYMENTS.TRANSACOES"
```

### 4. Registrar schemas
```
"Use schema para registrar o Avro de Transacao no subject dev.payments...-value"
```

### 5. Debug
```
"Use troubleshoot — o connector está em FAILED, ver últimas exceções"
```

## UIs locais

| Serviço | URL | Notas |
|---------|-----|-------|
| Control Center | http://localhost:9021 | UI Kafka + Connect + SR |
| Schema Registry | http://localhost:8081 | REST API |
| Kafka Connect | http://localhost:8083 | REST API |
| Kafka Broker | localhost:9092 | PLAINTEXT (workshop local) |

## Related agents

- `confluent-platform-expert` — `.claude/agents/confluent-platform-expert.md`
- `oracle-cdc-expert` — `.claude/agents/oracle-cdc-expert.md`
- `oracle-database-expert` — `.claude/agents/oracle-database-expert.md`
- `kafka-streaming-expert` — `.claude/agents/kafka-streaming-expert.md`
- `kafka-security-expert` — `.claude/agents/kafka-security-expert.md`
