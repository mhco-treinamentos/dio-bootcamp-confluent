# Desafio Final · Demo — a resolução do zero, passo a passo

> **Guia de demonstração**: o instrutor executa na gravação; o aluno pode acompanhar
> em casa, comando a comando — todos são copy-paste, com IDs capturados
> automaticamente (nada de copiar `lkc-xxxxx` na mão).
>
> ⏱ ~60–90 min · ☁️ **Custo: US$ 3–6** (conector + Flink, derrubados no Passo 9)
> Fluxo: conta limpa → Postgres no Neon → CDC → contrato → Flink + fraude → medição → teardown

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| CLI `confluent` v4+ logada | `confluent login --save` |
| Conta Neon **sem projeto** ativo | O demo cria o projeto; free tier limita projetos |
| Docker | Só para o `psql` |
| Este repositório | Rode tudo **da pasta `repo/`** (os scripts SQL e schemas são relativos a ela) |

> ⚠️ **zsh**: não digite comentários `#` no meio dos comandos — os blocos abaixo já
> vêm limpos, cole como estão.

---

## Passo 1 — Environment e cluster *(~8 min)*

**Goal**: o tabuleiro do desafio, dedicado e nomeado — `desafio-final` / `desafio-basic`.

```bash
confluent environment create desafio-final --governance-package essentials
export ENV=$(confluent environment list -o json | python3 -c "import json,sys; print([e['id'] for e in json.load(sys.stdin) if e['name']=='desafio-final'][0])")
confluent environment use $ENV

confluent kafka cluster create desafio-basic --cloud aws --region us-east-1 --type basic
```

O cluster leva 1–5 min provisionando. Capture e selecione quando ficar `UP`:

```bash
export CLUSTER=$(confluent kafka cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='desafio-basic'][0])")
confluent kafka cluster use $CLUSTER
```

**Validação** — as duas listas com `*` na coluna `Current`:

```bash
confluent environment list
confluent kafka cluster list
```

---

## Passo 2 — Identidades e credenciais *(~8 min)*

**Goal**: uma service account de menor privilégio para consumir, e as 3 keys de
escopos diferentes (cluster, Schema Registry, cloud) — a conta pessoal fica só no setup.

```bash
confluent iam service-account create desafio-consumer --description "Consumidor do resultado"
export SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='desafio-consumer'][0])")
echo "SA=$SA"
```

A key do consumidor (o secret aparece **uma vez** — aqui vai direto para variáveis):

```bash
K=$(confluent api-key create --resource $CLUSTER --service-account $SA --description "desafio-consumer" -o json)
export AK=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
export AS=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
```

As ACLs — tópico **e** consumer group (a segunda é a que todo mundo esquece):

```bash
confluent kafka acl create --allow --service-account $SA --operations read,describe --topic desafio --prefix
confluent kafka acl create --allow --service-account $SA --operations read,describe --consumer-group desafio- --prefix
```

A key do Schema Registry (repare: recurso `lsrc-`, não `lkc-` — cada recurso, sua
credencial) e a key cloud da Metrics API (o Passo 8 usa):

```bash
export SR=$(confluent schema-registry cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['cluster'])")
K=$(confluent api-key create --resource $SR --description "desafio-sr" -o json)
export SRK=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
export SRS=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")

K=$(confluent api-key create --resource cloud --description "desafio-metrics" -o json)
export MK=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
export MS=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
```

**Validação** — 3 keys, cada uma no seu escopo:

```bash
confluent api-key list
```

---

## Passo 3 — O banco legado no Neon *(~10 min)*

**Goal**: um Postgres público com o dataset de payments e replicação lógica ligada.

Pela UI do Neon (2 min):
1. [neon.com](https://neon.com) → **Create project** → `desafio-final`, região AWS us-east-1
2. **Project settings → Logical Replication → Enable** ← *o passo que mais gente esquece*
3. Copie a connection string do dashboard:

```bash
export PGURL='postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
docker run --rm postgres:16-alpine psql "$PGURL" -c "SHOW wal_level;"
```

**Output esperado**: `logical` (se vier `replica`, o toggle não aplicou — volte ao dashboard).

Schema, dados e preparação do CDC — três scripts prontos, na ordem:

```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/01_ddl.sql
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine psql "$PGURL" -v ON_ERROR_STOP=1 -q -f /sql/02_seed.sql
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/03_cdc_setup.sql
```

**Validação** — o seed é determinístico (`seed=42`): os SEUS números serão exatamente estes:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "SELECT (SELECT count(*) FROM payments.customers) AS customers, (SELECT count(*) FROM payments.accounts) AS accounts, (SELECT count(*) FROM payments.cards) AS cards, (SELECT count(*) FROM payments.merchants) AS merchants, (SELECT count(*) FROM payments.transactions) AS transactions;"
```

```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

Troque a senha da role de CDC e teste a conexão que o conector vai usar:

```bash
export DB_PASS='defina-uma-senha-forte-aqui'
docker run --rm postgres:16-alpine psql "$PGURL" -c "ALTER ROLE cdc_user WITH PASSWORD '$DB_PASS';"
export DB_HOST=$(echo "$PGURL" | python3 -c "import sys; print(sys.stdin.read().split('@')[1].split('/')[0])")
export CDCURL="postgresql://cdc_user:$DB_PASS@$DB_HOST/neondb?sslmode=require"
docker run --rm postgres:16-alpine psql "$CDCURL" -c "SELECT current_user, (SELECT count(*) FROM payments.transactions) AS pode_ler;"
```

**Output esperado**: `cdc_user | 2000`. Se este comando funciona, o conector funciona —
ele faz exatamente isto: conecta com esta role, por TLS, e lê estas tabelas.

---

## Passo 4 — O conector CDC *(~8 min)*

**Goal**: o Debezium gerenciado ligando banco e Kafka — com identidade mínima e nome próprio.

A key que o conector usa para escrever no cluster (a pessoal fica de fora):

```bash
K=$(confluent api-key create --resource $CLUSTER --description "desafio-cdc-writer" -o json)
export CCK=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
export CCS=$(echo "$K" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
```

O config — repare: env vars expandidas no heredoc, **nenhum secret digitado inline**:

```bash
cat > /tmp/cdc.json <<EOF
{
  "name": "desafio-cdc",
  "connector.class": "PostgresCdcSourceV2",
  "database.hostname": "$DB_HOST",
  "database.port": "5432",
  "database.dbname": "neondb",
  "database.user": "cdc_user",
  "database.password": "$DB_PASS",
  "database.sslmode": "require",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "$CCK",
  "kafka.api.secret": "$CCS",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "topic.prefix": "desafio",
  "table.include.list": "payments.accounts,payments.cards,payments.transactions",
  "publication.name": "bootcamp_cdc_pub",
  "heartbeat.interval.ms": "60000",
  "tasks.max": "1"
}
EOF
confluent connect cluster create --config-file /tmp/cdc.json
rm /tmp/cdc.json
```

> 💡 Se um dia recriar este conector, use **outro nome**: mesmo nome retoma os offsets
> e pula o snapshot.
>
> ⚠️ O `output.key.format: AVRO` não é opcional: sem ele a key do tópico não tem
> schema, o Flink não infere a coluna de distribuição, e o `ADD PRIMARY KEY` do
> Passo 7 falha com `Columns referenced in DISTRIBUTED BY must fully contain primary
> key columns`.

Provisioning leva ~2 min. **Validação**:

```bash
confluent connect cluster list
```

**Output esperado**: `desafio-cdc | RUNNING`. E os tópicos nasceram sozinhos, com schema:

```bash
confluent kafka topic list
```

---

## Passo 5 — O CDC ao vivo: create, update, delete *(~10 min)*

**Goal**: ver o envelope Debezium dos 3 tipos de operação, gerados agora — com o
consumidor **ligado antes**, em modo tail.

Primeiro, uma espiada no snapshot (Ctrl-C depois de algumas mensagens — o consumo
Avro lê ~2 msg/s, varrer as 2000 levaria ~20 min):

```bash
confluent kafka topic consume desafio.payments.transactions --from-beginning --group desafio-snap --api-key $AK --api-secret "$AS" --value-format avro --schema-registry-api-key $SRK --schema-registry-api-secret $SRS
```

**O que observar**: todo registro do snapshot vem com `op=r` — e o consumo funcionando
com a key do `desafio-consumer` prova as ACLs do Passo 2.

Agora o ao vivo. **Terminal 2** — deixe este tail rodando (sem `--from-beginning`: só
o que chegar a partir de agora):

```bash
confluent kafka topic consume desafio.payments.transactions --group desafio-tail1 --api-key $AK --api-secret "$AS" --value-format avro --schema-registry-api-key $SRK --schema-registry-api-secret $SRS
```

**Terminal 1** — gere um INSERT e um DELETE (colunas do DDL real; o delete é numa
transação, que não tem dependentes de FK):

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'CREDIT', 150.00, m.mcc, m.uf, now() FROM payments.cards c, payments.merchants m LIMIT 1;"
docker run --rm postgres:16-alpine psql "$PGURL" -c "DELETE FROM payments.transactions WHERE transaction_id = (SELECT transaction_id FROM payments.transactions ORDER BY created_at ASC LIMIT 1);"
```

**O que observar no tail**: em segundos, o `op=c` (before nulo, after com a linha) e o
`op=d` (after nulo). Troque o tail para `desafio.payments.accounts` e rode o UPDATE
(`SUSPENDED` é valor válido do CHECK; `BLOCKED` não é — só cards têm esse status):

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "UPDATE payments.accounts SET status = 'SUSPENDED', updated_at = now() WHERE account_id = (SELECT account_id FROM payments.accounts LIMIT 1);"
```

**O que observar**: o `op=u` com **`before` preenchido** (`status: ACTIVE` →
`SUSPENDED`) — `REPLICA IDENTITY FULL` em ação.

> 💡 Se o consumidor quebrar com `unknown magic byte` logo após um delete: você
> encontrou o **tombstone** (mensagem de valor nulo). É esperado — e é aula.

---

## Passo 6 — O contrato protege *(~6 min)*

**Goal**: provar a evolução BACKWARD nos dois sentidos — aceita **e** recusada.

Os schemas de demonstração já existem no repo (validados no Curso 2). Registre o v1 e
evolua com um campo novo **com default** — aceito:

```bash
confluent schema-registry schema create --subject desafio.pedidos-value --schema pedido-v1.avsc --type avro
confluent schema-registry schema create --subject desafio.pedidos-value --schema pedido-v2.avsc --type avro
```

Agora um campo obrigatório **sem default** — **o erro é o objetivo**:

```bash
confluent schema-registry schema create --subject desafio.pedidos-value --schema pedido-v3-quebra.avsc --type avro
```

**Output esperado**
```
Error: Schema being registered is incompatible with an earlier schema ...
READER_FIELD_MISSING_DEFAULT_VALUE ... {compatibility: 'BACKWARD'}; error code: 40901
```

**O que observar**: aceito nos dois casos = contrato que não protege nada. A metade
pedagógica é o Schema Registry dizendo **não** — com o campo, o motivo e a regra.

---

## Passo 7 — Flink: enriquecimento e a regra de fraude *(~15 min)*

**Goal**: um evento nascido no Postgres atravessar join e CEP e virar alerta.

```bash
confluent flink compute-pool create desafio-pool --cloud aws --region us-east-1 --max-cfu 5
```

Abra o **workspace** na Console (Environments → desafio-final → Flink → abrir
workspace no pool `desafio-pool`) — é onde as linhas aparecem. Statements na ordem:

```sql
ALTER TABLE `desafio.payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;
ALTER TABLE `desafio.payments.transactions` ADD PRIMARY KEY (transaction_id) NOT ENFORCED;
SET 'sql.tables.scan.idle-timeout' = '5 s';
```

O enriquecimento — temporal join materializado num tópico novo:

```sql
CREATE TABLE `desafio.transactions.enriched` AS
SELECT t.transaction_id, t.card_id, t.amount, a.account_id, a.status AS account_status
FROM `desafio.payments.transactions` t
JOIN `desafio.payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
```

> ⚠️ **Saiu 0 linhas? Não é bug.** É a watermark parada — o join só emite quando os
> DOIS lados andam. Gere um evento na dimensão e veja o represado jorrar:
> ```bash
> docker run --rm postgres:16-alpine psql "$PGURL" -c "UPDATE payments.accounts SET updated_at = now() WHERE account_id = (SELECT account_id FROM payments.accounts LIMIT 1);"
> ```

A regra `RULE_VELOCITY` — 3+ transações do mesmo cartão em menos de 60s. Antes, um
ajuste com lição dentro: o enriched é um **changelog** (o lado esquerdo do join é
upsert), e `MATCH_RECOGNIZE` exige entrada **append-only** — releia o tópico em modo
append:

```sql
ALTER TABLE `desafio.transactions.enriched` SET ('changelog.mode' = 'append');
```

```sql
CREATE TABLE `desafio.fraud.detected` AS
SELECT *
FROM `desafio.transactions.enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY `$rowtime`
  MEASURES FIRST(T.`$rowtime`) AS first_txn, LAST(T.`$rowtime`) AS last_txn, COUNT(T.transaction_id) AS txn_count
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS T.amount > 0
);
```

> ⚠️ O `?` em `T{3,}?` não é opcional: o Flink CEP **recusa quantificador greedy no
> fim do pattern** (`Greedy quantifiers are not allowed as the last element`). O
> reluctant emite o alerta na 3ª transação, sem esperar a janela fechar.

**O que observar primeiro — a enxurrada**: o tópico de fraude nasce com **centenas de
alertas**. Não é bug: o `$rowtime` é o tempo de **ingestão** no Kafka, e o snapshot
despejou 2000 transações em ~1 minuto — para a regra, tudo parece simultâneo. É a
regra do changelog em ação: *replay de snapshot não é tráfego de negócio*. Em produção,
o statement de fraude leria a partir do fim (`sql.tables.scan.startup.mode`), não do
histórico.

E o disparo ao vivo — 3 INSERTs do mesmo cartão, em sequência:

```bash
for i in 1 2 3; do docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'CREDIT', 999.00, m.mcc, m.uf, now() FROM payments.cards c, payments.merchants m LIMIT 1;"; done
```

Espere ~1 min e mande **uma 4ª transação inocente** — é ela que empurra a watermark e
descarrega o alerta do CEP (a mesma lição do join, pela quarta vez):

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'PIX', 10.00, m.mcc, m.uf, now() FROM payments.cards c, payments.merchants m LIMIT 1;"
```

**Validação — o clímax** (~1–2 min após a 4ª): um alerta NOVO com `txn_count = 3`
aparece em `desafio.fraud.detected` (no workspace,
`SELECT * FROM \`desafio.fraud.detected\`;` — os mais recentes no fim). Um INSERT de
banco virou decisão de negócio, atravessando CDC → contrato → join → CEP.

> 🧪 **Instrutor**: rode este passo uma vez na véspera (pré-voo). É o único com risco
> de surpresa ao vivo; custa centavos e o teardown do Passo 9 limpa.

---

## Passo 8 — Meça o que você construiu *(~6 min)*

**Goal**: as quatro respostas de operação, com números desta sessão.

① Observabilidade — o throughput do que acabamos de fazer (Metrics API, key cloud):

```bash
curl -s -u "$MK:$MS" -H "Content-Type: application/json" https://api.telemetry.confluent.cloud/v2/metrics/cloud/query -d @- <<EOF
{
  "aggregations": [{"metric": "io.confluent.kafka.server/received_bytes"}],
  "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "$CLUSTER"},
  "granularity": "PT1H",
  "intervals": ["PT6H/now"]
}
EOF
```

② Segurança — quem acessa o quê, e por quê:

```bash
confluent kafka acl list --service-account $SA
```

③ Custos — a fatura desta demo (consolida com 1–2 dias de atraso; no Linux, troque
`date -v-1d` por `date -d yesterday`):

```bash
confluent billing cost list --start-date $(date -v-1d +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

④ Confiabilidade — a tabela de garantias trecho a trecho está no
[README do desafio](README.md); o elo fraco é sempre o mesmo: **consumer → efeito**.
Idempotência ali é código seu, não configuração.

---

## Passo 9 — Teardown (a cena mais importante) *(~5 min)*

**Goal**: matar tudo que cobra por hora — e **provar**.

```bash
export LCC=$(confluent connect cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='desafio-cdc'][0])")
confluent connect cluster delete $LCC --cluster $CLUSTER --force
```

Os statements e o pool (os nomes dos statements aparecem na lista):

```bash
confluent flink statement list
```

Para cada um que estiver `RUNNING`: `confluent flink statement stop <nome>` e depois
`confluent flink statement delete <nome> --force`. Então:

```bash
export POOL=$(confluent flink compute-pool list -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['id'] if d else '')")
confluent flink compute-pool delete $POOL --force
```

O replication slot no Neon (leva ~1 min para ficar inativo após o delete do conector):

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "SELECT pg_drop_replication_slot('bootcamp_cdc_slot');"
```

**Validação final** — as três listas vazias:

```bash
confluent connect cluster list
confluent flink compute-pool list
docker run --rm postgres:16-alpine psql "$PGURL" -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'bootcamp_cdc_slot';"
```

**Output esperado**: `None found.`, `None found.`, `(0 rows)`. Cluster Basic parado e
Neon hibernando custam ~zero — pode dormir tranquilo. Para destruir TUDO ao final
(entrega corrigida): `confluent environment delete $ENV` e delete do projeto no Neon.

---

## Se algo não funcionou

| Sintoma | Onde está o fix |
|---|---|
| `wal_level = replica` | Toggle do Neon não aplicou — [Curso 3, Setup](../docs/curso3/setup/README.md) |
| Conector `FAILED` | [Curso 3, Aula 2 — troubleshooting](../docs/curso3/aula2/README.md) |
| `ADD PRIMARY KEY` falha com `DISTRIBUTED BY must fully contain...` | Faltou `output.key.format: AVRO` no conector (Passo 4) — sem key Avro não há coluna de distribuição |
| `Match Recognize doesn't support consuming update and delete changes` | Faltou o `ALTER ... SET ('changelog.mode' = 'append')` no enriched (Passo 7) |
| `Greedy quantifiers are not allowed as the last element` | Pattern termina em `{3,}` — troque por `{3,}?` (Passo 7) |
| Join roda e não emite | Watermark parada — o UPDATE do Passo 7 destrava ([Curso 4, Aula 3, Passo 6](../docs/curso4/aula3/README.md)) |
| `unknown magic byte` no consume | É o tombstone do delete — esperado |
| Consumo trava sem erro claro | Falta a ACL do consumer group (Passo 2) |
| Consumo Avro lento (~2 msg/s) | Normal na CLI — use tail (sem `--from-beginning`) para demos ao vivo |
| Statement `PENDING` para sempre | Pool saturado — pare statements antigos do workspace |

---

| ← O desafio | Setup do aluno | ↑ Repo |
|---|---|---|
| [README.md](README.md) | [SETUP.md](SETUP.md) | [repo/](../README.md) |
