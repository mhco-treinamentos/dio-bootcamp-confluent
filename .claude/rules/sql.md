---
globs: "**/*.sql"
---

# SQL Rules (Flink SQL + Oracle + Trino)

## Style
- Todas as keywords em UPPERCASE: `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `MATCH_RECOGNIZE`, `MERGE`
- CTEs (`WITH`) em vez de subqueries aninhadas
- Uma cláusula por linha
- Comentar lógica complexa com `-- explicação`
- Qualificar toda coluna com o alias da tabela em queries multi-tabela

## Flink SQL (self-managed, Flink 1.19)

### Watermarks (OBRIGATÓRIO em toda source)
```sql
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```
- Escolher delay pelo caso de uso: 5s (anti-fraude bloqueante), 30s (balanceado), 5min (analítico)
- **Nunca** usar processing-time windows para lógica de negócio CDC-driven
- Para Oracle CDC: watermark em `source.ts_ms` ou no campo Debezium `ts_ms`

### State TTL (OBRIGATÓRIO)
```sql
SET 'table.exec.state.ttl' = '24 h';
```
No workshop, manter TTLs curtos (1–4h) para os atendentes verem state expirando dentro da janela da cena.

### CEP / MATCH_RECOGNIZE
- Sempre delimitar com `WITHIN INTERVAL` — patterns sem limite estouram a memória
```sql
PATTERN (T1 T2 T3 WITHIN INTERVAL '60' SECOND)
DEFINE
  T1 AS true,
  T2 AS T2.account_id = T1.account_id,
  T3 AS T3.account_id = T1.account_id
```

### Checkpointing
```sql
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3a://flink-checkpoints/';  -- MinIO no workshop
```

### Window Types
- `TUMBLE`: janela fixa não-sobreposta (volume de transações por minuto)
- `HOP`: janela deslizante (transações últimas 24h, atualizando a cada 1h)
- `SESSION`: gap-based (sessão de uso de cartão)
- `OVER`: per-row (features em tempo real por evento — score de fraude)

### Iceberg Sink (cena de lakehouse)
```sql
CREATE TABLE iceberg_transacoes (
  transaction_id STRING,
  account_id STRING,
  amount DECIMAL(15, 2),
  event_time TIMESTAMP_LTZ(3),
  PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (DAYS(event_time))
WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'nessie',
  'catalog-database' = 'payments',
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
);
```

## Oracle (source CDC)

### Supplemental Logging (OBRIGATÓRIO para CDC)
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER TABLE PAYMENTS.TRANSACOES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE PAYMENTS.CONTAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE PAYMENTS.CARTOES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### Schema CDC-friendly
- Toda tabela CDC'd PRECISA ter PK estável
- Evitar `ON UPDATE CASCADE` — quebra ordering assumptions do CDC
- Preferir `NUMBER`, `VARCHAR2`, `TIMESTAMP WITH TIME ZONE` (tipos portáveis)

### Oracle XE no workshop
- LogMiner mode (Confluent Oracle CDC default no XE) — não tenta XStream
- Definir `archivelog mode` ON antes de iniciar o connector
- Espaço de archivelog limitado em XE — rodar `RMAN` cleanup entre cenas se necessário

## Trino (lakehouse query)

### Catalog Iceberg
```sql
-- Sessão Trino
USE iceberg.payments;

-- Time travel: ver estado em um snapshot anterior
SELECT *
FROM transacoes FOR VERSION AS OF 8763421 -- snapshot_id do Iceberg
WHERE account_id = 'acc-001';

-- Ver histórico de snapshots
SELECT * FROM "transacoes$snapshots" ORDER BY committed_at DESC;
```

### Performance
- Usar particionamento Iceberg em colunas de baixa cardinalidade (DAYS(event_time), bandeira_cartao)
- `ANALYZE` periodicamente em tabelas grandes para statistics atualizadas
- Evitar `SELECT *` em produção — sempre projetar colunas

## Parameterized Queries
Sempre parametrizar — nunca interpolar string em SQL:
```python
# BOM
cursor.execute("SELECT * FROM contas WHERE cpf = :cpf", {"cpf": cpf})

# RUIM — risco de SQL injection
cursor.execute(f"SELECT * FROM contas WHERE cpf = '{cpf}'")
```
