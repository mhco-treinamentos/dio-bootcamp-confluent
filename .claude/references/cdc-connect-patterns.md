# CDC & Connect Patterns — Deep Reference

> **Guia de profundidade para Change Data Capture com conectores Debezium gerenciados no Confluent Cloud. Fonte dos labs: Postgres 16 em Docker. Casa pedagógica: Curso 3 (Connect e CDC) do bootcamp.**

---

## Postgres 16 como fonte CDC (labs)

### Requisitos do banco

O Debezium para Postgres usa **logical decoding** (plugin `pgoutput`, nativo desde o Postgres 10) — nada de agentes ou leitura de binários externos. Três pré-requisitos:

1. `wal_level=logical` — o WAL passa a carregar informação suficiente para reconstruir mudanças linha a linha
2. Uma **publication** cobrindo as tabelas capturadas
3. Um **replication slot** — o Postgres retém WAL até o slot confirmar consumo (é isso que garante zero perda **e** é isso que enche disco se o conector parar; ver [Failure modes](#failure-modes-comuns))

```yaml
# docker-compose.yml — fonte CDC dos labs (módulo 3)
services:
  postgres:
    image: postgres:16
    command: >
      postgres
      -c wal_level=logical
      -c max_wal_senders=4
      -c max_replication_slots=4
    environment:
      POSTGRES_DB: payments
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: ${PG_PASSWORD}
    ports:
      - "5432:5432"
```

### Setup SQL (usuário, REPLICA IDENTITY, publication)

```sql
-- 1. Usuario dedicado ao conector (least privilege)
CREATE USER cdc_user WITH REPLICATION PASSWORD 'change-me';
GRANT CONNECT ON DATABASE payments TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON public.accounts, public.cards, public.transactions TO cdc_user;

-- 2. REPLICA IDENTITY FULL — sem isso, eventos UPDATE/DELETE trazem apenas a PK
--    no campo `before`. FULL grava a linha inteira no WAL (custo aceitavel nos labs).
ALTER TABLE public.accounts     REPLICA IDENTITY FULL;
ALTER TABLE public.cards        REPLICA IDENTITY FULL;
ALTER TABLE public.transactions REPLICA IDENTITY FULL;

-- 3. Publication apenas com as tabelas capturadas (nunca FOR ALL TABLES em prod)
CREATE PUBLICATION bootcamp_cdc_pub
    FOR TABLE public.accounts, public.cards, public.transactions;

-- 4. Verificar
SELECT * FROM pg_publication_tables WHERE pubname = 'bootcamp_cdc_pub';
SHOW wal_level;  -- esperado: logical
```

> O replication slot é criado pelo próprio conector no primeiro start (`slot.name`). Criá-lo manualmente só é necessário para cenários de failover.

### Conectividade com o Confluent Cloud

O conector gerenciado roda **na infra do Confluent Cloud** — ele precisa alcançar o Postgres. Para os labs, as opções em ordem de preferência:

| Opção | Quando usar |
|-------|-------------|
| Postgres em cloud (RDS/Cloud SQL) com IP público + allowlist dos [egress IPs do Confluent Cloud](https://docs.confluent.io/cloud/current/networking/static-egress-ip-addresses.html) | Lab guiado do módulo 3 (recomendado) |
| Túnel (`ngrok tcp 5432`) apontando para o Docker local | Demo rápida na gravação, sem provisionar banco |
| Fallback local (Kafka KRaft + Connect self-managed) | Sem créditos/sem internet — fora do escopo padrão dos labs |

---

## Mapeamento de tipos Postgres → Avro

| Postgres | Avro | Logical type | Notas |
|----------|------|--------------|-------|
| `SMALLINT`, `INTEGER` | `int` | `int32` | — |
| `BIGINT`, `SERIAL8` | `long` | `int64` | — |
| `NUMERIC(p,s)` | `bytes` | `decimal` | Valores monetários — usar `decimal.handling.mode=precise` |
| `REAL`, `DOUBLE PRECISION` | `float`/`double` | — | Nunca para dinheiro |
| `VARCHAR`, `TEXT` | `string` | — | — |
| `UUID` | `string` | `uuid` | Chaves de entidade (`account_id`, `transaction_id`) |
| `DATE` | `int` | `date` | Dias desde epoch |
| `TIMESTAMP` | `long` | `timestamp-micros` | Depende de `time.precision.mode` |
| `TIMESTAMPTZ` | `string` | `ZonedTimestamp` | ISO 8601 — preferir TIMESTAMPTZ na fonte |
| `JSONB` | `string` | `json` | Payloads flexíveis (metadata de transação) |
| `BOOLEAN` | `boolean` | — | — |
| `BYTEA` | `bytes` | — | — |

### Crítico: valores monetários

```json
{
  "decimal.handling.mode": "precise",
  "time.precision.mode": "connect"
}
```

**Sempre `precise`** para `amount`, `balance`, `fee`. O modo `double` introduz erro de ponto flutuante inaceitável para BRL — mesmo princípio da regra `Decimal` em [rules/python.md](../rules/python.md).

---

## Conector gerenciado (PostgresCdcSourceV2)

O Confluent Cloud oferece o Debezium Postgres como **fully-managed connector** (`PostgresCdcSourceV2`, baseado no Debezium 2.x). Nada de Connect worker, JVM tuning ou instalação de plugin — o trade é: configuração via UI/CLI/API e menos knobs expostos.

```json
{
  "name": "pg-payments-cdc",
  "connector.class": "PostgresCdcSourceV2",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "sa-xxxxxx",
  "database.hostname": "<host-alcancavel-pelo-confluent-cloud>",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "${PG_PASSWORD}",
  "database.dbname": "payments",
  "topic.prefix": "pg-payments",
  "table.include.list": "public.accounts,public.cards,public.transactions",
  "publication.name": "bootcamp_cdc_pub",
  "slot.name": "bootcamp_cdc_slot",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "precise",
  "tombstones.on.delete": "true",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "tasks.max": "1"
}
```

```bash
# Criar via CLI
confluent connect cluster create --config-file pg-payments-cdc.json

# Status e troubleshooting
confluent connect cluster list
confluent connect cluster describe <connector-id>
confluent connect event describe   # eventos de erro do conector
```

> Topics de saída default do Debezium: `{topic.prefix}.{schema}.{table}` (ex.: `pg-payments.public.transactions`). Para aderir à convenção `dev.payments.pg-payments.transactions.cdc.v1` de [rules.md](../rules.md), usar o SMT `RegexRouter` (abaixo).

---

## Snapshot modes

| Modo | Comportamento | Uso |
|------|---------------|-----|
| `initial` | Snapshot completo das tabelas, depois streaming | Primeiro deploy (default dos labs) |
| `initial_only` | Apenas snapshot, sem streaming | Export único |
| `no_data` | Só estrutura, streaming a partir do LSN atual | Ignorar histórico (era `never`/`schema_only` no Debezium 1.x) |
| `when_needed` | Snapshot se offsets ausentes/inválidos | Auto-recovery |
| `always` | Snapshot a cada start | Debug/labs descartáveis |

Durante o snapshot o Debezium emite eventos `op='r'` (read). O lab do módulo 3 usa isso para mostrar a diferença entre carga histórica e streaming ao vivo no mesmo topic.

---

## Envelope Debezium

### Estrutura

```json
{
  "payload": {
    "before": { "transaction_id": "a1b2...", "status": "PENDING",  "amount": "150.00" },
    "after":  { "transaction_id": "a1b2...", "status": "SETTLED",  "amount": "150.00" },
    "source": {
      "version": "2.5.4.Final",
      "connector": "postgresql",
      "name": "pg-payments",
      "ts_ms": 1767139200000,
      "snapshot": "false",
      "db": "payments",
      "schema": "public",
      "table": "transactions",
      "lsn": 24023128,
      "txId": 565
    },
    "op": "u",
    "ts_ms": 1767139200100
  }
}
```

- `source.ts_ms` = quando o commit ocorreu **no banco**; `payload.ts_ms` = quando o Debezium processou. A diferença entre os dois é o lag de CDC.
- `lsn` (Log Sequence Number) é a posição no WAL, usada como offset do conector.

### Operation types

| `op` | Operação | `before` | `after` |
|------|----------|----------|---------|
| `r` | Snapshot read | null | registro |
| `c` | Create (INSERT) | null | registro |
| `u` | Update | registro antigo¹ | registro novo |
| `d` | Delete | registro antigo¹ | null |

¹ Completo apenas com `REPLICA IDENTITY FULL`; caso contrário, só a PK.

---

## Deletes e tombstones

Um `DELETE` na fonte gera **dois** eventos no topic:

1. **Delete event** — `op='d'`, `before` preenchido, `after=null`. Consumers de negócio reagem a ele.
2. **Tombstone** — mesma key, value `null` (se `tombstones.on.delete=true`). Existe para que a **log compaction** do Kafka remova fisicamente o registro — essencial para direito ao esquecimento (LGPD, Live 3).

```
DELETE FROM transactions WHERE transaction_id = 'a1b2...';

topic: pg-payments.public.transactions
  ├── { key: "a1b2...", value: { op: "d", before: {...}, after: null } }
  └── { key: "a1b2...", value: null }          ← tombstone
```

Regras práticas:
- Consumers **devem** tolerar value `null` sem NPE (poison pill #11 em [poison-pill-patterns.md](poison-pill-patterns.md))
- Com o SMT de unwrap, controlar o destino dos deletes via `delete.handling.mode` (abaixo)
- Topics CDC compactados exigem tombstones ligados; topics de retenção por tempo podem desligá-los

---

## SMTs (Single Message Transforms)

### ExtractNewRecordState (unwrap)

O SMT mais importante do módulo 3: achata o envelope `{before, after, op, source}` em uma linha simples — o Flink SQL (módulo 4) lê um schema Avro plano, muito mais simples.

```json
{
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,source.ts_ms,source.lsn"
}
```

| Config | Efeito |
|--------|--------|
| `delete.handling.mode=rewrite` | Delete vira linha com `__deleted=true` (soft delete visível para o Flink) |
| `delete.handling.mode=drop` | Deletes somem do stream (só para dimensões append-only) |
| `drop.tombstones=false` | Preserva tombstones para compaction |
| `add.fields=op,source.ts_ms` | Reinjeta metadados úteis como colunas `__op`, `__source_ts_ms` |

**Trade-off ensinado no bootcamp**: unwrap simplifica o consumo, mas perde a semântica completa de before/after. O desafio final mantém o envelope no topic CDC cru e faz o unwrap **no Flink** (shift-left) — as duas abordagens são mostradas.

### RegexRouter (naming convention)

```json
{
  "transforms": "unwrap,route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "pg-payments\\.public\\.(.*)",
  "transforms.route.replacement": "dev.payments.pg-payments.$1.cdc.v1"
}
```

### Mascaramento de colunas (LGPD/PCI)

```json
{
  "column.mask.with.11.chars": "public.accounts.cpf",
  "column.mask.hash.SHA-256.with.salt.bootcamp2026": "public.accounts.email",
  "column.exclude.list": "public.cards.pan_encrypted"
}
```

PAN nunca chega a topic em claro — ver [rules.md](../rules.md) (PII/PCI).

---

## Dead Letter Queue (DLQ)

Padrão de resiliência do Curso 3, Aula 4. Registros que falham na conversão/SMT vão para um Dead Letter Topic em vez de derrubar o conector:

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dev.payments.pg-payments.dlq.v1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
```

- Nos **sink connectors gerenciados**, o Confluent Cloud cria DLQ automaticamente (`dlq-<connector-id>`); em source connectors o escopo do DLQ é menor (cobre conversão/SMT, não erros de leitura do banco).
- `context.headers.enable=true` grava topic/partição/offset/exception nos headers — é o que o lab usa para fazer o post-mortem da poison pill.
- Todo DLQ precisa de **consumer de inspeção + alerta de profundidade** (Metrics API, módulo 7). DLQ sem monitoramento é um buraco negro.
- Injeção de mensagens problemáticas para exercitar o DLQ: [poison-pill-patterns.md](poison-pill-patterns.md).

---

## Evolução de schema

### DDLs seguros vs. quebradores

| DDL na fonte | Compatibilidade | Notas |
|--------------|-----------------|-------|
| Adicionar coluna nullable | BACKWARD | Seguro — consumers ignoram campos desconhecidos |
| Adicionar coluna com default | BACKWARD | Seguro |
| Remover coluna | FORWARD only | Consumers precisam tolerar ausência |
| Renomear coluna | BREAKING | Exigir novo topic (`.v2`) |
| Alargar tipo (`INT`→`BIGINT`) | BACKWARD | Seguro |
| Estreitar tipo (`BIGINT`→`INT`) | BREAKING | Risco de perda de dados |

O Schema Registry com compatibilidade **BACKWARD** (default do bootcamp) rejeita evoluções quebradoras no registro do schema — o erro aparece no conector, não no consumer. Esse é o comportamento demonstrado no lab de schema drift.

---

## Monitoramento

### Lado Postgres

```sql
-- Replication slots: lag em bytes de WAL retido (a métrica que enche disco)
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

-- Conexões de replicação ativas
SELECT application_name, state, sent_lsn, replay_lsn
FROM pg_stat_replication;
```

### Lado Confluent Cloud

```bash
# Status do conector e eventos de erro
confluent connect cluster describe <connector-id>
confluent connect event describe

# Metrics API — throughput do conector (módulo 7)
curl -s -u "$CC_API_KEY:$CC_API_SECRET" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query" \
  -H "Content-Type: application/json" \
  -d '{"aggregations":[{"metric":"io.confluent.kafka.connect/sent_records"}],
       "filter":{"field":"resource.connector.id","op":"EQ","value":"<connector-id>"},
       "granularity":"PT1M","intervals":["PT1H/now"]}'
```

---

## Failure modes comuns

| Sintoma | Causa | Solução |
|---------|-------|---------|
| Disco do Postgres enchendo | **Slot growth**: conector parado/pausado, slot retém WAL indefinidamente | Monitorar `pg_replication_slots`; dropar slot de conector desativado (`SELECT pg_drop_replication_slot('bootcamp_cdc_slot')`); configurar `max_slot_wal_keep_size` (Postgres 13+) como stop-loss |
| Conector falha após pausa longa | WAL exigido já foi descartado (slot dropado ou `max_slot_wal_keep_size` estourado) | Re-snapshot (`snapshot.mode=when_needed`) — aceitar reprocessamento |
| Eventos param de fluir mas conector "RUNNING" | Tabela nova não está na publication | `ALTER PUBLICATION bootcamp_cdc_pub ADD TABLE ...` |
| Registro vai ao DLQ com erro de serialização | **Schema drift**: DDL incompatível com o subject BACKWARD no SR | Corrigir na fonte ou versionar topic; inspecionar headers do DLQ |
| `before` vem só com a PK | REPLICA IDENTITY default | `ALTER TABLE ... REPLICA IDENTITY FULL` |
| Slot avança devagar em banco ocioso | Sem tráfego nas tabelas capturadas, offset não avança | `heartbeat.interval.ms=10000` — o conector emite heartbeats que avançam o LSN confirmado |

### Heartbeat (por que importa no Postgres)

Sem mudanças nas tabelas capturadas, o conector não confirma consumo de WAL — em bancos com **outras** tabelas movimentadas, o slot segura WAL antigo e o disco cresce. O heartbeat força avanço periódico do LSN confirmado:

```json
{
  "heartbeat.interval.ms": "10000"
}
```

---

## Checklist pré-CDC (lab módulo 3)

```markdown
- [ ] Postgres 16 com `wal_level=logical` (+ max_wal_senders/max_replication_slots)
- [ ] Usuário `cdc_user` com REPLICATION + SELECT nas tabelas capturadas
- [ ] REPLICA IDENTITY FULL em accounts, cards, transactions
- [ ] Publication `bootcamp_cdc_pub` cobrindo exatamente as tabelas do lab
- [ ] Banco alcançável pelo Confluent Cloud (egress IPs allowlisted ou túnel)
- [ ] Service account do conector com ACLs mínimas (write nos topics CDC + DLQ)
- [ ] `decimal.handling.mode=precise` e output AVRO com Schema Registry
- [ ] SMT RegexRouter aplicando a convenção de naming de rules.md
- [ ] DLQ configurado com headers de contexto + consumer de inspeção
- [ ] Campos PII mascarados (CPF, email) — PAN excluído
- [ ] Teardown par do setup: deletar conector E dropar replication slot
```

---

**Última atualização**: 2026-07-11
