---
globs: "**/*.sql"
---

# SQL Rules (Flink SQL — Confluent Cloud)

Todo SQL do bootcamp é **Flink SQL gerenciado no Confluent Cloud** (compute pools + statements). Não há SQL Client self-managed, Oracle nem Trino. Topics com schema no Schema Registry aparecem automaticamente como tabelas Flink — não é preciso `CREATE TABLE ... WITH ('connector' = 'kafka', ...)` manual.

## Style

- Todas as keywords em UPPERCASE: `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `MATCH_RECOGNIZE`, `TUMBLE`
- CTEs (`WITH`) em vez de subqueries aninhadas
- Uma cláusula por linha
- Comentar lógica complexa com `-- explicação`
- Qualificar toda coluna com o alias da tabela em queries multi-tabela
- Nomes de tabela/coluna em inglês (prosa dos comentários pode ser pt-BR)

## Watermarks (OBRIGATÓRIO para lógica event-time)

No Confluent Cloud toda tabela vem com watermark default sobre `$rowtime` (o timestamp do record Kafka). Para lógica de negócio, defina a estratégia explicitamente sobre o campo de evento:

```sql
ALTER TABLE `dev.payments.gen.transactions.created.v1`
MODIFY WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND;
```

- Escolher o delay pelo caso de uso: **5s** (anti-fraude bloqueante), **30s** (balanceado — default dos labs), **5min** (analítico)
- **Nunca** usar processing time para lógica de negócio orientada a eventos/CDC — resultados mudam a cada replay
- Para topics CDC: watermark no `ts_ms` do envelope Debezium (momento do commit na fonte), não no tempo de ingestão
- Documentar no lab o trade-off: delay maior = menos eventos late descartados, mais latência de fechamento de janela

## Window TVFs (TUMBLE / HOP / SESSION)

Usar sempre as table-valued functions (sintaxe atual) — não a sintaxe legada `GROUP BY TUMBLE(...)`:

```sql
-- TUMBLE: janela fixa não-sobreposta (volume de transações por minuto)
SELECT window_start, window_end, COUNT(*) AS txn_count
FROM TABLE(
  TUMBLE(TABLE `transactions_enriched`, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

-- HOP: janela deslizante (últimas 24h, atualizando a cada 1h)
SELECT window_start, window_end, SUM(amount) AS total_amount
FROM TABLE(
  HOP(TABLE `transactions_enriched`, DESCRIPTOR(event_time), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
GROUP BY window_start, window_end;

-- SESSION: gap-based (sessão de uso de cartão — encerra após 30 min sem evento)
SELECT window_start, window_end, card_id, COUNT(*) AS txns_in_session
FROM TABLE(
  SESSION(TABLE `transactions_enriched` PARTITION BY card_id, DESCRIPTOR(event_time), INTERVAL '30' MINUTE))
GROUP BY window_start, window_end, card_id;
```

- `OVER` window para features per-row em tempo real (ex.: média móvel do cartão para score de fraude)

## Joins

Escolher o tipo de join pelo comportamento de state — join regular em stream infinito acumula state para sempre:

```sql
-- Interval join: limita o state por tempo (transação ↔ estorno em até 7 dias)
SELECT t.transaction_id, r.refund_id
FROM transactions t
JOIN refunds r
  ON t.transaction_id = r.transaction_id
  AND r.event_time BETWEEN t.event_time AND t.event_time + INTERVAL '7' DAY;

-- Temporal join: enriquecimento com a versão vigente de uma tabela versionada
-- (transação ↔ estado da conta NO MOMENTO do evento — padrão para lookup em CDC)
SELECT t.transaction_id, a.credit_limit
FROM transactions t
JOIN accounts_cdc FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id;
```

- Regular join só quando ambos os lados são finitos ou há state TTL configurado
- Temporal join exige PRIMARY KEY e watermark na tabela versionada (topics CDC do Debezium atendem)

## MATCH_RECOGNIZE (CEP — regras determinísticas de fraude, módulo 4)

Sempre delimitar com `WITHIN INTERVAL` — patterns sem limite estouram o state:

```sql
SELECT *
FROM transactions_enriched
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY event_time
  MEASURES
    FIRST(T.event_time) AS first_txn_time,
    LAST(T.event_time) AS last_txn_time,
    COUNT(T.transaction_id) AS txn_count
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS T.amount > 0
);
```

- `PARTITION BY` sempre na chave de negócio (card_id, account_id) — nunca sem partição
- `AFTER MATCH SKIP PAST LAST ROW` como default (evita matches sobrepostos duplicando alertas)
- Quantificador no FIM do pattern precisa ser **reluctant** (`{3,}?`) — o Flink recusa greedy como último elemento (`Greedy quantifiers are not allowed as the last element`); o reluctant também emite o alerta na 3ª ocorrência, sem esperar a janela
- Entrada do MATCH_RECOGNIZE precisa ser **append-only** — sobre tabela derivada de CDC/join upsert, primeiro `ALTER TABLE ... SET ('changelog.mode' = 'append')` (validado no Confluent Cloud, 2026-09-04)
- Regras determinísticas (`RULE_VELOCITY`, `RULE_GEO_IMPOSSIBLE`) → MATCH_RECOGNIZE; classificação probabilística → `ML_PREDICT` (módulo 6)

## State TTL

```sql
SET 'sql.state-ttl' = '4 HOURS';
```

- OBRIGATÓRIO em statements com joins regulares ou agregações não-janeladas
- Nos labs, TTLs curtos (1–4h) para o aluno ver state expirando dentro da janela do lab — e para não pagar por state morto
- Checkpointing, savepoints e recovery são **gerenciados pelo Confluent Cloud** — não configurar `execution.checkpointing.*` nem `state.backend` (não existem em statements cloud); o conceito é ensinado no módulo 4 como "o que a plataforma faz por você"

## Changelog semantics (fontes CDC)

Topics do Debezium são streams de **changelog**, não append-only:

- Tabela Flink sobre topic CDC opera em modo *upsert*: eventos `op=u` retratam linhas anteriores, `op=d` gera DELETE
- Agregações sobre changelog emitem retrações — o sink precisa suportar upsert (topic compactado com chave, ou Tableflow/Iceberg)
- **Nunca** somar "eventos" de um topic CDC como se fossem transações de negócio: um UPDATE não é uma transação nova
- Para congelar um snapshot append-only a partir do changelog, materialize com janela (`TUMBLE` + última versão por chave) antes do sink
- `$rowtime` de um evento CDC é o tempo de ingestão no Kafka; o tempo de negócio é `ts_ms` do envelope — usar o campo certo na watermark

## ML_PREDICT (Real-Time AI — módulo 6)

Inferência de modelo remoto direto no Flink SQL:

```sql
-- 1) Registrar o modelo remoto (uma vez, com connection para o provider)
CREATE MODEL fraud_classifier
INPUT (amount DOUBLE, mcc STRING, uf_match BOOLEAN, txn_velocity INT)
OUTPUT (is_fraud STRING, confidence DOUBLE)
WITH (
  'provider' = 'openai',
  'task' = 'classification',
  'openai.connection' = 'openai-connection'
);

-- 2) Usar no pipeline
SELECT t.*, p.is_fraud, p.confidence
FROM transactions_enriched t,
LATERAL TABLE(ML_PREDICT('fraud_classifier', t.amount, t.mcc, t.uf_match, t.txn_velocity)) AS p;
```

- Credenciais do provider vivem numa **connection** do Confluent Cloud — nunca no statement
- `ML_PREDICT` chama um endpoint remoto: latência e custo por chamada — nos labs, aplicar só ao subconjunto filtrado (ex.: transações já sinalizadas por regra determinística), não ao stream inteiro
- Enriquecer ANTES de materializar no Tableflow (shift-left, módulo 5)

## Parameterized Queries (clients Python — ex.: seed do Postgres no lab de CDC)

Sempre parametrizar — nunca interpolar string em SQL:
```python
# BOM
cursor.execute("SELECT * FROM accounts WHERE cpf = %s", (cpf,))

# RUIM — risco de SQL injection
cursor.execute(f"SELECT * FROM accounts WHERE cpf = '{cpf}'")
```

---

**Última atualização**: 2026-07-11
