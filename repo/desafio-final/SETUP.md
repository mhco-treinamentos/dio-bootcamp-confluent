# Desafio Final · Setup — Confluent Cloud + Neon

> Provisiona os dois lados do pipeline: o **Confluent Cloud** (environment, cluster,
> identidades) e o **Neon** (o Postgres "legado" que o CDC captura).
> ⏱ ~40 min · Feito uma vez; entre sessões, só o [teardown parcial](#7--fim-de-sessão-o-que-desligar) roda.

Cada passo tem uma **validação** — não avance sem ela passar. Se algo falhar, o guia
completo correspondente tem o troubleshooting: [Curso 2, Aula 0](../docs/curso2/aula0/README.md)
para o lado Confluent, [Curso 3, Setup](../docs/curso3/setup/README.md) para o Neon.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Conta Confluent Cloud com créditos | O relógio dos créditos corre em dias corridos — crie quando for começar |
| Conta Neon (free tier, sem cartão) | [neon.com](https://neon.com) |
| CLI `confluent` **v4+** | `brew install confluentinc/tap/cli` ou [instalador](https://docs.confluent.io/confluent-cli/current/install.html) |
| Docker | Só para rodar `psql` — não é o banco |
| Este repositório clonado | Os scripts SQL vivem em [`dataset/neon/`](../dataset/neon/) |

---

## 1 — Confluent Cloud: environment e cluster *(~8 min)*

Use um environment **dedicado ao desafio** — não reaproveite o dos labs; a nota de
reprodutibilidade depende de o seu `teardown.sh` poder varrer tudo sem medo.

**🖱 Pela Console**: **Environments → Add cloud environment** → nome `desafio-final`,
Stream Governance **Essentials** (traz o Schema Registry). Dentro dele, **Create
cluster** → tipo **Basic**, nome `desafio-basic` — anote cloud e região e **use a
mesma região em tudo** (inclusive no Neon).

**⌨️ Pela CLI** (após `confluent login --save`):

```bash
confluent environment create desafio-final --governance-package essentials
confluent environment list                        # copie o env-xxxxxx criado
confluent environment use env-xxxxxx

confluent kafka cluster create desafio-basic --cloud aws --region us-east-1 --type basic
confluent kafka cluster list                      # copie o lkc-xxxxxx
confluent kafka cluster use lkc-xxxxxx
```

**Validação** — as duas listas com `*` na coluna `Current`:

```bash
confluent environment list && confluent kafka cluster list
```

> 💡 O cluster pode ficar 1–5 min em `PROVISIONING`. Espere o `UP` antes de seguir.

---

## 2 — Identidades: uma service account por papel *(~10 min)*

A key da sua conta pessoal (admin) **não aparece em nenhum client** — é a regra do
menor privilégio do [Curso 7, Aula 3](../docs/curso7/aula3/README.md). O desafio usa
três identidades:

| Service account | Papel | ACLs |
|---|---|---|
| `desafio-producer` | O que escreve (se você tiver producer próprio) | `WRITE,DESCRIBE` nos tópicos `desafio-` |
| `desafio-consumer` | O que lê o resultado do pipeline | `READ,DESCRIBE` em tópicos + consumer group |
| (sua conta pessoal) | Só setup e teardown | — |

```bash
confluent iam service-account create desafio-producer --description "Producer do desafio"
confluent iam service-account create desafio-consumer --description "Consumidor do resultado"
confluent iam service-account list                # copie os sa-xxxxxx
```

Uma API key **por service account**, presa ao cluster:

```bash
confluent api-key create --resource lkc-xxxxxx --service-account sa-xxxxxx \
  --description "desafio-producer"
```

> 🔴 O secret aparece **uma única vez**. Guarde no `.env` (nunca no Git) na hora.

As ACLs (ajuste os nomes ao seu prefixo de tópicos):

```bash
# producer: escreve nos tópicos do desafio
confluent kafka acl create --allow --service-account sa-xxxxxx \
  --operations write,describe --topic desafio- --prefix

# consumer: lê tópicos + commita offset no grupo
confluent kafka acl create --allow --service-account sa-yyyyyy \
  --operations read,describe --topic desafio- --prefix
confluent kafka acl create --allow --service-account sa-yyyyyy \
  --operations read,describe --consumer-group desafio- --prefix
```

E a key do **Schema Registry** (recurso `lsrc-`, não `lkc-` — cada recurso, sua
credencial):

```bash
confluent schema-registry cluster describe        # copie o lsrc-xxxxxx
confluent api-key create --resource lsrc-xxxxxx --description "desafio-sr"
```

Por fim, a key de escopo **cloud** para a Metrics API — a Camada 5 (operação) precisa
dela para medir o pipeline:

```bash
confluent api-key create --resource cloud --description "desafio-metrics"
```

**Validação** — o inventário de identidades que vai para o seu README:

```bash
confluent iam service-account list
confluent api-key list
confluent kafka acl list --service-account sa-xxxxxx
```

---

## 3 — O `.env` do desafio *(~3 min)*

Consolide tudo num `.env` (e confirme que ele está no `.gitignore` **antes** do
primeiro commit):

```bash
# Confluent Cloud
export CC_ENV="env-xxxxxx"
export CC_CLUSTER="lkc-xxxxxx"
export CC_BOOTSTRAP="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"   # do cluster describe
export CC_PRODUCER_KEY="..."
export CC_PRODUCER_SECRET="..."
export CC_CONSUMER_KEY="..."
export CC_CONSUMER_SECRET="..."
export SR_URL="https://psrc-xxxxx.us-east-1.aws.confluent.cloud"     # do sr cluster describe
export SR_KEY="..."
export SR_SECRET="..."
export METRICS_KEY="..."
export METRICS_SECRET="..."

# Neon (preenchido na seção 4)
export PGURL=""      # owner — DDL, seed e escrita de eventos de teste
export CDCURL=""     # cdc_user — SÓ o conector usa
```

**Validação**: `git check-ignore .env` responde `.env`. Se não responder, pare e
corrija — secret commitado reprova a camada de segurança do desafio.

---

## 4 — Neon: o banco legado *(~10 min)*

### 4.1 Projeto

1. [neon.com](https://neon.com) → **Create project** → nome `desafio-final`,
   região **a mesma do cluster** (ex.: AWS us-east-1)
2. Copie a connection string do dashboard e guarde como `PGURL` no `.env`:

```
postgresql://USUARIO:SENHA@ep-xxxxx.REGIAO.aws.neon.tech/neondb?sslmode=require
```

### 4.2 Replicação lógica (o passo que todo mundo esquece)

Dashboard → **Project settings → Logical Replication → Enable**. Sem isso o conector
CDC não conecta.

**Validação**:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "SHOW wal_level;"
```
```
 wal_level
-----------
 logical
```

Se vier `replica`, o toggle não aplicou — volte ao dashboard.

### 4.3 Schema, dados e preparação do CDC

Os três scripts, na ordem, a partir da raiz de `repo/`:

```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/01_ddl.sql

docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -f /sql/02_seed.sql

docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/03_cdc_setup.sql
```

O terceiro script cria a role `cdc_user` (com `REPLICATION`, só `SELECT` nas tabelas),
liga `REPLICA IDENTITY FULL` e cria a publication `bootcamp_cdc_pub` — os três
pré-requisitos do Debezium, explicados no [Curso 3, Setup, Passo 4](../docs/curso3/setup/README.md).

> ⚠️ **Troque a senha do `cdc_user`** (o script traz `'troque-esta-senha'`):
> ```bash
> docker run --rm postgres:16-alpine psql "$PGURL" \
>   -c "ALTER ROLE cdc_user WITH PASSWORD 'sua-senha-forte';"
> ```

Monte a `CDCURL` no `.env` — **mesmo host** da `PGURL`, usuário `cdc_user` e a senha
nova:

```
postgresql://cdc_user:sua-senha-forte@ep-xxxxx.REGIAO.aws.neon.tech/neondb?sslmode=require
```

**Validação** — o dataset e a conexão que o conector vai usar:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c \
"SELECT (SELECT count(*) FROM payments.customers)    AS customers,
        (SELECT count(*) FROM payments.accounts)     AS accounts,
        (SELECT count(*) FROM payments.cards)        AS cards,
        (SELECT count(*) FROM payments.merchants)    AS merchants,
        (SELECT count(*) FROM payments.transactions) AS transactions;"

docker run --rm postgres:16-alpine psql "$CDCURL" -c \
  "SELECT current_user, (SELECT count(*) FROM payments.transactions) AS pode_ler;"
```
```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000

 current_user | pode_ler
--------------+----------
 cdc_user     |     2000
```

Os números batem exatamente — o seed é determinístico (`seed=42`). **Se o segundo
comando funcionar, o conector vai funcionar**: ele faz a mesma coisa (conecta com essa
role, por TLS, e lê essas tabelas).

---

## 5 — O contrato de custo (leia uma vez, lembre sempre)

| Recurso | Cobra | Pode ficar ligado entre sessões? |
|---|---|---|
| Environment + cluster Basic ocioso | por uso (~zero parado) | ✅ Sim |
| Neon sem conector | hiberna (não consome CU-hours) | ✅ Sim |
| **Conector CDC** | por task-hora **e mantém o Neon acordado** | ❌ Nunca |
| **Statement Flink** | por CFU-hora | ❌ Nunca |
| Compute pool Flink ocioso | nada | ✅ Sim |
| Tableflow habilitado | por hora | ❌ Nunca |

O conector é o caso duplo: queima crédito Confluent **e** impede o Neon de hibernar —
esquecido 24/7, os 100 CU-hours/mês do free tier estouram em ~4 dias.

---

## 6 — Checklist final do setup

```bash
confluent environment list          # * em desafio-final
confluent kafka cluster list        # * em desafio-basic, UP
confluent iam service-account list  # desafio-producer, desafio-consumer
confluent api-key list              # 1 key por SA + SR + cloud
docker run --rm postgres:16-alpine psql "$CDCURL" -c "SELECT 1;"   # conecta
```

Tudo verde → comece pelo pipeline ([README do desafio](README.md), Camada 2).

---

## 7 — Fim de sessão: o que desligar

O setup acima **fica de pé** entre sessões (custa ~zero). O que o seu `teardown.sh`
parcial derruba a cada sessão:

```bash
confluent connect cluster list      # deve terminar vazio → delete o que aparecer
confluent flink statement list      # idem
confluent tableflow topic list --cluster $CC_CLUSTER   # idem, se usou a Camada 6
```

O teardown **completo** (entrega feita, avaliação encerrada) apaga o environment
inteiro e o projeto Neon:

```bash
confluent environment delete env-xxxxxx   # ⚠️ apaga TUDO do environment
# Neon: dashboard → Project settings → Delete project
```

---

| ← O desafio | ↑ Repo |
|---|---|
| [README.md](README.md) | [repo/](../README.md) |
