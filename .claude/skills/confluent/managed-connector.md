# Managed Connectors (Confluent Cloud)

## Skill: `/confluent-managed-connector`

Cria e gerencia **conectores totalmente gerenciados** no Confluent Cloud via CLI `confluent` + arquivos JSON de config. Cobre o source CDC do bootcamp (**Debezium Postgres — `PostgresCdcSourceV2`**), sink connectors (DW, NoSQL, Object Storage), SMTs, DLQ e troubleshooting. Módulo 3 do bootcamp.

> Connect é **cloud-only** neste bootcamp — não há worker de Connect no fallback local ([local-dev-fallback.md](local-dev-fallback.md)).

## Usage

```
/confluent-managed-connector <action> [options]
```

### Actions
- `create-source` — Source Debezium Postgres (CDC)
- `create-sink` — Sink (S3, JDBC/DW, MongoDB, etc.)
- `status` — Estado do conector e das tasks
- `dlq` — Configurar/inspecionar Dead Letter Queue
- `troubleshoot` — Diagnóstico de falhas comuns

---

## Banco fonte do lab: Postgres 16 em Docker

O source CDC dos labs é um Postgres 16 local com replicação lógica habilitada:

```yaml
# docker-compose.yml (banco fonte do módulo 3)
services:
  postgres:
    image: postgres:16
    container_name: pg-payments
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: payments
      POSTGRES_USER: payments_admin
      POSTGRES_PASSWORD: ${PG_PASSWORD}
    command: >-
      postgres -c wal_level=logical
               -c max_wal_senders=4
               -c max_replication_slots=4
```

```sql
-- Usuário de CDC com least privilege (nunca usar o superuser no conector)
CREATE USER cdc_user WITH REPLICATION PASSWORD 'change-me';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
CREATE PUBLICATION dbz_publication FOR TABLE public.accounts, public.cards, public.transactions;
```

```bash
# Verificar que a replicação lógica está ativa
docker exec pg-payments psql -U payments_admin -d payments -c "SHOW wal_level;"   # logical
```

> **Rede**: o conector gerenciado roda **na cloud** e precisa alcançar o Postgres. Para o lab, exponha o Docker local via túnel TCP (ex.: `ngrok tcp 5432`) ou rode o Postgres numa VM com IP público + firewall restrito. Nunca exponha o superuser.

---

## Source: Debezium Postgres (managed)

### Config file — `debezium-postgres-source.json`

```json
{
  "name": "pg-payments-cdc-source",
  "connector.class": "PostgresCdcSourceV2",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "<sa-id>",

  "database.hostname": "<pg-host>",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "${PG_CDC_PASSWORD}",
  "database.dbname": "payments",
  "database.sslmode": "prefer",

  "topic.prefix": "dev.payments.pg-payments",
  "table.include.list": "public.accounts,public.cards,public.transactions",
  "slot.name": "dbz_bootcamp",
  "publication.name": "dbz_publication",
  "snapshot.mode": "initial",

  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "after.state.only": "false",
  "tombstones.on.delete": "true",

  "transforms": "route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "dev\\.payments\\.pg-payments\\.public\\.(.*)",
  "transforms.route.replacement": "dev.payments.pg-payments.$1.cdc.v1",

  "tasks.max": "1"
}
```

Resultado: topics `dev.payments.pg-payments.accounts.cdc.v1`, `...cards.cdc.v1`, `...transactions.cdc.v1` (naming de [rules.md](../../rules.md)).

### Criar via CLI

```bash
# Contexto: environment + cluster já selecionados (ver cloud-setup.md)
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>

# Conferir o plugin e as configs obrigatórias
confluent connect plugin list | grep -i postgres
confluent connect plugin describe PostgresCdcSourceV2

# Criar o conector
confluent connect cluster create --config-file debezium-postgres-source.json

# Acompanhar até RUNNING
confluent connect cluster list
confluent connect cluster describe <lcc-id>
```

---

## Sinks

### S3 Sink — `s3-sink.json` (Object Storage / data lake bruto)

```json
{
  "name": "s3-sink-transactions",
  "connector.class": "S3_SINK",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "<sa-id>",

  "topics": "dev.payments.flink.transactions.enriched.v1",
  "input.data.format": "AVRO",

  "s3.bucket.name": "bootcamp-payments-lake",
  "aws.access.key.id": "${AWS_ACCESS_KEY_ID}",
  "aws.secret.access.key": "${AWS_SECRET_ACCESS_KEY}",
  "output.data.format": "PARQUET",
  "time.interval": "HOURLY",
  "flush.size": "1000",

  "tasks.max": "1"
}
```

> No módulo 5 o caminho preferido para lakehouse é **Tableflow** ([tableflow.md](tableflow.md)) — o S3 Sink fica como comparação "jeito clássico vs. shift-left".

### JDBC/DW e NoSQL

Mesmo fluxo: `confluent connect plugin describe <plugin>` para ver campos obrigatórios, montar JSON, `confluent connect cluster create --config-file`. Plugins usados nos labs: `SnowflakeSink`, `MongoDbAtlasSink`, `PostgresSink`.

---

## SMTs (Single Message Transforms)

Cadeia típica no source CDC (roteamento + mascaramento + metadado):

```json
"transforms": "route,maskPii,insertMeta",

"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "dev\\.payments\\.pg-payments\\.public\\.(.*)",
"transforms.route.replacement": "dev.payments.pg-payments.$1.cdc.v1",

"transforms.maskPii.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.maskPii.fields": "cpf,email,phone",
"transforms.maskPii.replacement": "[MASKED]",

"transforms.insertMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.insertMeta.timestamp.field": "kafka_ingested_at"
```

| SMT | Uso no bootcamp |
|-----|-----------------|
| `RegexRouter` | Renomear topics CDC para o padrão `{stage}.{domain}.{source}.{entity}.{event_type}.v{version}` |
| `MaskField` | Mascarar PII na borda (LGPD — Live 3) |
| `InsertField` | Timestamp de ingestão para medir lag end-to-end |
| `ExtractField`/`ValueToKey` | Definir chave de partição (ex.: `account_id`) |
| `Filter` + predicates | Descartar tombstones/operações indesejadas |

---

## DLQ (Dead Letter Queue)

Resiliência do módulo 3: mensagens com erro de conversão/schema não derrubam o conector.

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dev.payments.pg-payments.dlq.v1",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.log.enable": "true",
"errors.log.include.messages": "true"
```

> Em **sink connectors gerenciados**, se você não informar o nome, o Confluent Cloud cria automaticamente o DLQ `dlq-<lcc-id>`. Nos labs usamos nome explícito para seguir a convenção de naming.

```bash
# Inspecionar a DLQ (headers trazem topic/partição/offset/erro originais)
confluent kafka topic consume dev.payments.pg-payments.dlq.v1 --from-beginning --print-key

# Lab de poison pill: injetar registro inválido no Postgres e observá-lo cair na DLQ
```

---

## Status e troubleshooting

```bash
# Lifecycle
confluent connect cluster list                       # todos os conectores + status
confluent connect cluster describe <lcc-id>          # config + status por task
confluent connect cluster pause <lcc-id>
confluent connect cluster resume <lcc-id>
confluent connect cluster update <lcc-id> --config-file debezium-postgres-source.json
confluent connect cluster delete <lcc-id>            # TEARDOWN: sempre ao final do lab

# Eventos de log do conector (topic de eventos no cluster)
confluent connect event describe
```

### Problemas comuns (Debezium Postgres managed)

| Sintoma | Causa provável | Correção |
|---------|----------------|----------|
| `FAILED` logo após criar | Cloud não alcança o Postgres | Túnel/IP público + firewall; testar `psql -h <host>` de fora |
| `must be superuser or replication role` | `cdc_user` sem `REPLICATION` | `ALTER USER cdc_user WITH REPLICATION;` |
| `logical decoding requires wal_level >= logical` | `wal_level` errado | Subir Postgres com `-c wal_level=logical` e reiniciar |
| `publication "dbz_publication" does not exist` | Publication não criada | `CREATE PUBLICATION ... FOR TABLE ...` |
| `replication slot ... is active` | Slot preso de execução anterior | `SELECT pg_drop_replication_slot('dbz_bootcamp');` |
| Snapshot travado | Tabela grande + lock | `snapshot.mode=no_data` se o histórico não importa no lab |
| Erro de schema no consumer | Topic sem compatibilidade BACKWARD | Ver [schema-registry.md](schema-registry.md) |
| Mensagens sumindo silenciosamente | `errors.tolerance=all` sem monitorar DLQ | Alertar em DLQ > 0 (módulo 7) |
| WAL crescendo sem parar no Postgres | Conector pausado segura o slot | Deletar o slot ao pausar por longos períodos |

---

## Best practices

1. **Um service account por conector**, com ACLs mínimas (write nos topics de destino + DLQ)
2. **Sempre configurar DLQ** com headers de contexto habilitados
3. **Config em arquivo JSON versionado** (sem segredos — usar env vars/placeholders)
4. **`confluent connect plugin describe`** antes de escrever o JSON — campos obrigatórios mudam por plugin
5. **Snapshot em horário controlado** e `tasks.max=1` para CDC
6. **Evolução de schema sempre BACKWARD** (campos novos opcionais com default)
7. **Teardown**: `confluent connect cluster delete` no `teardown.sh` do lab — conector ocioso consome créditos

---

**Última atualização**: 2026-07-11
