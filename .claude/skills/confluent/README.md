# Confluent Cloud Skills

Skills para provisionar e operar a stack do bootcamp no **Confluent Cloud** (CLI `confluent` v4). Fallback local enxuto apenas para topics + Schema Registry.

## Skills

| Skill | Command | Descrição |
|-------|---------|-----------|
| Cloud Setup | `/confluent-cloud-setup` | Aula 0: login, environment, cluster, API keys, service accounts, flink quickstart, teardown |
| Topics | `/confluent-topics` | Criar e gerenciar topics (`confluent kafka topic ...`) |
| Schema Registry | `/confluent-schema` | Schemas Avro/JSON/Protobuf, compatibilidade BACKWARD, Data Contracts |
| Connect (geral) | `/confluent-connect` | Plugins, ciclo de vida e monitoramento de conectores gerenciados |
| Managed Connector | `/confluent-managed-connector` | Source Debezium Postgres (CDC), sinks, SMT, DLQ — módulo 3 |
| Tableflow | `/confluent-tableflow` | Materializar topics como tabelas Iceberg, catálogos — módulo 5 |
| Troubleshoot | `/confluent-troubleshoot` | Debug: auth, conectores, SR, lag, Flink, billing |
| Local Dev Fallback | `/confluent-local-dev-fallback` | Compose enxuto (1 broker KRaft + SR) para demos offline sem gastar créditos |

> **Cloud-first**: Connect, Flink e Tableflow existem **somente** no Confluent Cloud. O fallback local cobre apenas topics + Schema Registry ([local-dev-fallback.md](local-dev-fallback.md)).

## Quick start

### 1. Aula 0 — setup e teardown
```
"Use cloud-setup para criar environment, cluster Basic, API keys e o teardown.sh do lab"
```

### 2. Criar topics
```
"Use topics para criar dev.payments.gen.transactions.created.v1 com 6 partições e retention de 7 dias"
```

### 3. Registrar schemas
```
"Use schema-registry para registrar o Avro de TransactionCreated no subject dev.payments.gen.transactions.created.v1-value com BACKWARD"
```

### 4. CDC do módulo 3
```
"Use managed-connector para configurar o Debezium Postgres capturando public.transactions do pg-payments"
```

### 5. Lakehouse do módulo 5
```
"Use tableflow para materializar dev.payments.flink.transactions.enriched.v1 como tabela Iceberg"
```

### 6. Debug
```
"Use troubleshoot — o conector está FAILED, ver trace e eventos"
```

## Endpoints

| Recurso | Onde obter |
|---------|-----------|
| Bootstrap (`CC_BOOTSTRAP`) | `confluent kafka cluster describe <cluster-id>` |
| Schema Registry (`SR_URL`) | `confluent schema-registry cluster describe` |
| Fallback local | `localhost:9092` (PLAINTEXT) + `http://localhost:8081` |

## Related agents

- `confluent-cloud-expert` — `.claude/agents/confluent-cloud-expert.md`
- `connect-cdc-expert` — `.claude/agents/connect-cdc-expert.md`
- `flink-confluent-expert` — `.claude/agents/flink-confluent-expert.md`
- `tableflow-lakehouse-expert` — `.claude/agents/tableflow-lakehouse-expert.md`
- `kafka-streaming-expert` — `.claude/agents/kafka-streaming-expert.md`
- `kafka-security-expert` — `.claude/agents/kafka-security-expert.md`

---

**Última atualização**: 2026-07-11
