# Curso 3 · Setup — O banco legado no Neon

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~30 min · ☁️ Cria um Postgres gratuito · pré-requisito das Aulas 2 a 4

O Curso 3 integra um **banco legado** ao streaming. Este guia provisiona esse banco: um Postgres no Neon, com replicação lógica ligada e o dataset de pagamentos carregado.

---

## Por que Neon (e não o Docker do Curso 1)

Esta é a pergunta certa, e a resposta define o curso inteiro:

> **O conector gerenciado roda na infraestrutura da Confluent, não na sua máquina.**

Ele precisa alcançar o Postgres pela internet, na porta 5432. Seu `docker compose` local é invisível para ele — não há rota, não há IP público.

O Neon resolve isso de graça: é um Postgres serverless com **endpoint público e TLS**. Sem VPN, sem túnel, sem allowlist.

```
   SUA MÁQUINA                    INTERNET                CONFLUENT CLOUD
   ┌──────────┐                                          ┌──────────────┐
   │ psql     │──────► Neon Postgres ◄───── 5432 ────────│  conector    │
   │ (docker) │        (público, TLS)                     │  CDC         │
   └──────────┘                                          └──────────────┘
```

---

## ⚠️ O custo escondido do CDC

Antes de começar, entenda uma regra que vale o curso inteiro:

> **Um conector CDC ligado impede o Neon de hibernar.**

O free tier do Neon dá **100 CU-hours/mês**, e o compute normalmente dorme quando ocioso. Mas uma conexão de replicação ativa mantém ele acordado:

| Cenário | Consumo | Resultado |
|---|---|---|
| Conector esquecido ligado 24/7 | 720 h/mês | **Estoura em ~4 dias** |
| Sessões de 3h, com teardown | 3 CU-h por sessão | ~33 sessões/mês — folgado |

Ou seja: no Curso 3, o teardown deixa de ser só disciplina de créditos Confluent — vira **condição de sobrevivência do seu banco**.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito o [Curso 2](../../curso2/README.md) | Cluster, service account e API keys |
| Conta no Neon | Free tier, sem cartão |
| Docker | Só para rodar `psql` — não é o banco |

> 💡 Não tem `psql` instalado? Não precisa. Todos os comandos deste guia usam
> `docker run --rm postgres:16-alpine psql`, que baixa a imagem na primeira vez e
> não deixa nada instalado.

---

## O que você vai saber fazer ao final

- Provisionar um Postgres acessível pela internet, de graça
- Ligar replicação lógica e verificar que funcionou
- Carregar um dataset com integridade referencial
- Criar a role de replicação e a publication que o CDC exige
- Explicar por que cada pré-requisito existe

---

## Passo 1 — Criar o projeto no Neon *(~5 min)*

1. Acesse [neon.com](https://neon.com) e faça o signup (free tier, sem cartão)
2. **Create project**:
   - Nome: `bootcamp-dio`
   - Região: **a mesma do seu cluster Confluent** (ex.: AWS us-east-1)
3. Copie a **connection string** que aparece no dashboard

Ela tem este formato:

```
postgresql://USUARIO:SENHA@ep-xxxxx.REGIAO.aws.neon.tech/neondb?sslmode=require
```

> 💡 **Sobre a região**: o conector vai atravessar a internet de qualquer forma, e
> funciona mesmo com regiões diferentes. Mas latência e (em produção) custo de
> transferência entre regiões pesam — alinhe se puder.

Guarde no ambiente:

> 🎬 A string abaixo é a do **ambiente de gravação** (projeto Neon destruído ao fim do
> curso — a URL do vídeo não funciona mais). **Aluno**: cole a SUA, copiada do
> dashboard.

```bash
export PGURL='postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
```

> 🔴 Essa string contém a senha do banco. Trate como API key: nunca no Git, nunca
> em chat. Se vazar, rotacione no dashboard do Neon.

---

## Passo 2 — Ligar a replicação lógica *(~3 min)*

**Este é o passo que mais gente esquece — e sem ele o conector da Aula 2 não conecta.**

No dashboard do Neon:

**Project settings → Logical Replication → Enable**

É um toggle de projeto. Ele liga o equivalente gerenciado do `wal_level=logical`: o WAL passa a carregar informação suficiente para reconstruir cada mudança linha a linha.

### Verifique

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "SHOW wal_level;"
```

**Output esperado**
```
 wal_level
-----------
 logical
```

**Se aparecer `replica`**, o toggle não foi aplicado. Volte ao dashboard — o conector vai falhar sem isso.

Confirme também que sua role pode replicar:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT rolname, rolreplication FROM pg_roles WHERE rolname = current_user;"
```
```
   rolname    | rolreplication
--------------+----------------
 neondb_owner | t
```

**O que observar**: `rolreplication = t`. O Neon concede isso à role padrão — é o que
permite criar a role de CDC no Passo 4.

---

## Passo 3 — Criar as tabelas e carregar o dataset *(~8 min)*

Os scripts vivem em [`dataset/neon/`](../../../dataset/neon/). Rode na ordem.

### O schema

```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/01_ddl.sql
```

Confira:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "\dt payments.*"
```

**Output esperado**
```
  Schema  |     Name     | Type  |    Owner
----------+--------------+-------+--------------
 payments | accounts     | table | neondb_owner
 payments | cards        | table | neondb_owner
 payments | customers    | table | neondb_owner
 payments | merchants    | table | neondb_owner
 payments | transactions | table | neondb_owner
```

**O que observar**: **toda tabela tem PRIMARY KEY**. Isso não é preciosismo de
modelagem — no CDC, a PK vira a **key do evento**, que decide a partição e, portanto,
a ordem (Curso 1, Aula 5). Tabela sem PK gera CDC sem garantia de ordem por linha.

### O dataset

```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -f /sql/02_seed.sql
```

Verifique as contagens:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c \
"SELECT (SELECT count(*) FROM payments.customers)    AS customers,
        (SELECT count(*) FROM payments.accounts)     AS accounts,
        (SELECT count(*) FROM payments.cards)        AS cards,
        (SELECT count(*) FROM payments.merchants)    AS merchants,
        (SELECT count(*) FROM payments.transactions) AS transactions;"
```

**Output esperado**
```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

**O que observar**: os números batem exatamente porque o seed é **determinístico**
(`seed=42`). Todo aluno tem o mesmo banco — e é esse conteúdo que o CDC vai capturar
como snapshot inicial na Aula 2.

---

## Passo 4 — Preparar o CDC *(~6 min)*

Três coisas que o Debezium exige — e o script faz as três:

```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -v ON_ERROR_STOP=1 -f /sql/03_cdc_setup.sql
```

**Output esperado** (o final):
```
     pubname      | schemaname |  tablename   | attnames ...
------------------+------------+--------------+----------
 bootcamp_cdc_pub | payments   | accounts     | {account_id,customer_id,...}
 bootcamp_cdc_pub | payments   | cards        | {card_id,account_id,...}
 bootcamp_cdc_pub | payments   | transactions | {transaction_id,account_id,...}
(3 rows)
```

### O que o script fez, e por quê

**1. A role dedicada**
```sql
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD '...';
GRANT SELECT ON payments.accounts, payments.cards, payments.transactions TO cdc_user;
```
O conector se autentica com esta role — **não** com o owner do banco. É o mesmo
princípio da service account do Curso 2: identidade de aplicação com escopo mínimo.

**2. `REPLICA IDENTITY FULL`**
```sql
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;
```
Sem isso, eventos de UPDATE e DELETE trazem **apenas a chave primária** no campo
`before`. Com `FULL`, a linha inteira vai para o WAL — e é isso que torna possível a
cena de before/after da Aula 2.

**3. A publication**
```sql
CREATE PUBLICATION bootcamp_cdc_pub FOR TABLE payments.accounts, payments.cards, payments.transactions;
```
A lista explícita das tabelas capturadas. **Nunca** `FOR ALL TABLES` em produção — você
acabaria replicando tabelas de auditoria, filas internas e o que mais existir.

> ⚠️ Troque a senha do `cdc_user` (o script traz `'troque-esta-senha'`). Você vai
> passá-la ao conector na Aula 2. *(No ambiente de gravação mantivemos a padrão — ele
> será destruído ao fim do curso.)*

---

## Passo 5 — Teste a conexão do CDC *(~3 min)*

Confirme que a role de replicação funciona de fora:

```bash
export CDCURL='postgresql://cdc_user:troque-esta-senha@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'

docker run --rm postgres:16-alpine psql "$CDCURL" -c \
  "SELECT current_user, (SELECT count(*) FROM payments.transactions) AS pode_ler;"
```

**Output esperado**
```
 current_user | pode_ler
--------------+----------
 cdc_user     |     2000
```

**Se este comando funcionar, o conector da Aula 2 vai funcionar.** Ele faz exatamente
a mesma coisa: conecta com essa role, por TLS, e lê essas tabelas.

---

## O que fica de pé

| Recurso | Estado | Custo |
|---|---|---|
| Projeto Neon `bootcamp-dio` | 5 tabelas + 2.820 registros | Grátis (hiberna quando ocioso) |
| Role `cdc_user` + publication | Prontas | — |
| Replicação lógica | Ligada | — |

**Sem conector ligado, o Neon hiberna** e não consome CU-hours. É seguro deixar assim
entre as sessões de estudo.

---

## Se algo não funcionou

**`SHOW wal_level` retorna `replica`**
O toggle de Logical Replication não foi aplicado. Dashboard → Project settings →
Logical Replication → Enable. Pode levar alguns segundos.

**`psql: could not connect to server`**
Confira a connection string. O Neon **exige** `?sslmode=require` no final.

**`ERROR: permission denied for schema payments` com o `cdc_user`**
O `GRANT SELECT` não pegou. Rode o `03_cdc_setup.sql` de novo com a role owner.

**`CREATE ROLE ... WITH REPLICATION` falhou**
Sua role não tem `rolreplication`. Confira com o comando do Passo 2 — no Neon, a role
padrão do projeto tem esse privilégio.

**A primeira query demora ~2s**
É o auto-suspend: o compute estava dormindo e acordou. Inofensivo — e é justamente o
comportamento que um conector CDC ligado impede.

**Quero recomeçar do zero**
```bash
docker run --rm -v "$PWD/dataset/neon:/sql:ro" postgres:16-alpine \
  psql "$PGURL" -f /sql/99_teardown.sql
```
⚠️ Apaga o schema inteiro. Depois refaça os Passos 3 e 4.

---

## Glossário

| Termo | Definição |
|---|---|
| **Neon** | Postgres serverless com free tier; o "banco legado" do bootcamp. |
| **Replicação lógica** | Recurso do Postgres que expõe as mudanças do WAL de forma consumível. |
| **WAL** | Write-Ahead Log: onde o Postgres registra toda mudança antes de aplicá-la. |
| **Publication** | Lista explícita de tabelas expostas para replicação. |
| **`REPLICA IDENTITY FULL`** | Faz o WAL guardar a linha inteira, não só a PK — necessário para `before`. |
| **CU-hour** | Unidade de compute do Neon; o free tier dá 100 por mês. |

---

## Para ir além

| Conceito | Volta em |
|---|---|
| O conector que lê esse WAL | [Aula 2](../aula2/README.md) |
| Escrever de volta no Neon | [Aula 3](../aula3/README.md) |
| Slot órfão retendo WAL | [Aula 2](../aula2/README.md) — teardown |

**Fontes**

- Neon — [Logical replication to Confluent](https://neon.com/docs/guides/logical-replication-kafka-confluent)
- Neon — [Free plan limits](https://neon.com/faqs/free-plan-limits-and-quotas)
- PostgreSQL — [Logical replication](https://www.postgresql.org/docs/current/logical-replication.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Curso 2 · Aula 5](../../curso2/aula5/README.md) | [Curso 3](../README.md) | [Aula 1 · Introdução ao Connect](../aula1/README.md) |
