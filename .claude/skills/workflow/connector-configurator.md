# Connector Configurator

## Skill: `/connector-configurator`

Configura **conectores gerenciados do Confluent Cloud** com SMTs, tratamento de erros (DLQ) e monitoramento. Cobre o source Debezium Postgres do bootcamp e sinks (S3, Snowflake, MongoDB). Complementa [skills/confluent/managed-connector.md](../confluent/managed-connector.md) (passo a passo do módulo 3) com foco em **design da configuração**.

## Usage

```
/connector-configurator <connector-type> [options]
```

### Connector Types
- `postgres-cdc` — Debezium Postgres CDC Source (`PostgresCdcSourceV2`)
- `s3-sink` — S3 Sink (data lake bruto / comparação com Tableflow)
- `snowflake-sink` — Snowflake Sink (DW)
- `mongodb-sink` — MongoDB Atlas Sink (NoSQL)
- `datagen` — Datagen Source (mock rápido)

### Options
- `--stage` — `dev` | `lab` (não há prod no bootcamp)
- `--tables` — Tabelas do Postgres fonte (`public.accounts,public.cards,public.transactions`)

---

## Postgres CDC Source (Debezium managed)

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

  "transforms": "route,maskPii,insertMeta",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "dev\\.payments\\.pg-payments\\.public\\.(.*)",
  "transforms.route.replacement": "dev.payments.pg-payments.$1.cdc.v1",
  "transforms.maskPii.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.maskPii.fields": "cpf,email,phone",
  "transforms.maskPii.replacement": "[MASKED]",
  "transforms.insertMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.insertMeta.timestamp.field": "kafka_ingested_at",

  "tasks.max": "1"
}
```

Deploy: `confluent connect cluster create --config-file pg-payments-cdc-source.json`.

---

## Mapa fonte → topics (naming de rules.md)

| Tabela Postgres | Topic |
|-----------------|-------|
| `public.accounts` | `dev.payments.pg-payments.accounts.cdc.v1` |
| `public.cards` | `dev.payments.pg-payments.cards.cdc.v1` |
| `public.transactions` | `dev.payments.pg-payments.transactions.cdc.v1` |
| (erros) | `dev.payments.pg-payments.dlq.v1` |

---

## SMT Patterns

### Cadeia completa (route + mask + metadata)

Ver o JSON acima. Regras de escolha:

| SMT | Quando usar |
|-----|-------------|
| `RegexRouter` | Sempre — nome CDC cru não segue a convenção do bootcamp |
| `MaskField$Value` | PII que não pode chegar a topic em claro (LGPD); PAN **nunca** chega em claro |
| `InsertField$Value` | Timestamp de ingestão p/ medir lag end-to-end no módulo 7 |
| `ExtractField$Key` / `ValueToKey` | Ajustar chave de partição (ex.: `account_id`) |
| `Filter` + `RecordIsTombstone` | Sink que não trata tombstone |
| `Flatten$Value` | Sink tabular que não aceita struct aninhada |

---

## Sinks

### S3 Sink (Parquet, particionado por hora)

```json
{
  "name": "s3-sink-transactions-enriched",
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

### Snowflake Sink

```json
{
  "name": "snowflake-sink-fraud",
  "connector.class": "SnowflakeSink",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "<sa-id>",
  "topics": "dev.payments.flink.fraud.detected.v1",
  "input.data.format": "AVRO",
  "snowflake.url.name": "${SNOWFLAKE_URL}",
  "snowflake.user.name": "${SNOWFLAKE_USER}",
  "snowflake.private.key": "${SNOWFLAKE_PRIVATE_KEY}",
  "snowflake.database.name": "PAYMENTS",
  "snowflake.schema.name": "STREAMING",
  "tasks.max": "1"
}
```

> Campos exatos por plugin: `confluent connect plugin describe <plugin>` — é a fonte da verdade, não este arquivo.

---

## Error handling (DLQ)

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dev.payments.pg-payments.dlq.v1",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.log.enable": "true",
"errors.log.include.messages": "true"
```

Em sinks gerenciados, sem nome explícito o Cloud cria `dlq-<lcc-id>` automaticamente. Lab do módulo 3: injetar poison pill (generators/poison-pill-injector) e rastrear o registro na DLQ pelos headers.

---

## Monitoramento

```bash
confluent connect cluster list
confluent connect cluster describe <lcc-id>
confluent connect event describe

# Alerta base do módulo 7: DLQ > 0
confluent kafka topic consume dev.payments.pg-payments.dlq.v1 --from-beginning --print-key
```

---

## Configuration Checklist

```markdown
Para todo conector:

Conexão:
- [ ] Credenciais via env vars/placeholders (nunca hardcoded, nunca commitadas)
- [ ] Postgres do lab alcançável pela cloud (túnel/IP público + firewall)
- [ ] Usuário do banco com privilégio mínimo (cdc_user REPLICATION + SELECT)

Dados:
- [ ] output/input.data.format = AVRO (integra SR e habilita Tableflow depois)
- [ ] Compatibilidade BACKWARD nos subjects gerados
- [ ] PII mascarada na borda (MaskField) ou tokenizada no Flink (Live 3)

Erros:
- [ ] DLQ nomeada na convenção + context headers habilitados
- [ ] errors.log.enable = true

Naming:
- [ ] RegexRouter aplicando {stage}.{domain}.{source}.{entity}.{event_type}.v{version}
- [ ] DLQ segue a mesma convenção

Operação:
- [ ] tasks.max = 1 (labs)
- [ ] Service account próprio com ACLs mínimas
- [ ] Delete do conector no teardown.sh do lab (créditos!)
```

---

**Última atualização**: 2026-07-11
