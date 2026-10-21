# Curso 3 · Aula 2 — CDC com Debezium: o banco legado vira fonte de eventos

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~45 min · ☁️ **Conector + Neon acordado** · 2 terminais

Esta é a aula que muda a percepção do que é possível. Você vai fazer um banco Postgres publicar eventos no Kafka — **sem tocar em uma linha da aplicação que escreve nele**.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito o [Setup](../setup/README.md) | Neon com replicação lógica e `cdc_user` |
| 1 terminal | Qualquer um — a Preparação reconstrói o resto |

## Preparação — contexto do zero *(~5 min)*

Nada aqui depende de terminal antigo. Os únicos "colar" da aula: o secret da key nova
(P.2) e a connection string do Neon (P.3) — todo o resto é autocaptura.

### P.1 — Capture os IDs

**macOS / Linux**
```bash
CLUSTER=$(confluent kafka cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print(([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos']+[''])[0])")
echo "CLUSTER=$CLUSTER  SA=$SA"
```

**Windows (PowerShell)**
```powershell
$CLUSTER = (confluent kafka cluster describe -o json | ConvertFrom-Json).id
$SA = (confluent iam service-account list -o json | ConvertFrom-Json | Where-Object name -eq "app-pedidos").id
echo "CLUSTER=$CLUSTER  SA=$SA"
```

Se o `SA` vier **vazio**, crie **e capture no mesmo bloco** (não pule o segundo comando):

**macOS / Linux**
```bash
confluent iam service-account create app-pedidos --description "Aplicacao de pedidos - bootcamp DIO"
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos'][0])")
echo "SA=$SA"
```

**Windows (PowerShell)**
```powershell
confluent iam service-account create app-pedidos --description "Aplicacao de pedidos - bootcamp DIO"
$SA = (confluent iam service-account list -o json | ConvertFrom-Json | Where-Object name -eq "app-pedidos").id
echo "SA=$SA"
```

### P.2 — Key do cluster

```bash
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
```

> 🎬 O par abaixo é do **ambiente de gravação** (revogado ao fim do curso). **Aluno**:
> cole o par que o SEU comando acima imprimiu.

**macOS / Linux**
```bash
CC_API_KEY="SUA-API-KEY"
CC_API_SECRET="cflt-SEU-SECRET-AQUI"
```

**Windows (PowerShell)**
```powershell
$CC_API_KEY = "WD3TKALFA7V4X5OG"
$CC_API_SECRET = "cflt-SEU-SECRET-AQUI"
```

### P.3 — A connection string do CDC (cole a do Setup, Passo 4)

> 🎬 O bloco abaixo traz a connection string do **ambiente de gravação** (destruído ao
> fim do curso — a URL do vídeo não funciona mais). **Aluno**: troque pela SUA, gerada
> no [Setup](../setup/README.md), Passo 4.

**macOS / Linux**
```bash
CDCURL='postgresql://cdc_user:troque-esta-senha@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
DB_USER=$(echo "$CDCURL" | sed -E 's|postgresql://([^:]+):.*|\1|')
DB_PASS=$(echo "$CDCURL" | sed -E 's|postgresql://[^:]+:([^@]+)@.*|\1|')
DB_HOST=$(echo "$CDCURL" | sed -E 's|.*@([^/]+)/.*|\1|')
echo "DB_USER=$DB_USER  DB_HOST=$DB_HOST"
```

> ⚠️ O echo DEVE mostrar `DB_USER=cdc_user`. Se mostrar `neondb_owner`, você colou a
> URL errada (a do owner) — o conector usa a do **cdc_user**, e vai falhar com
> `password authentication failed`.

**Windows (PowerShell)**
```powershell
$CDCURL = 'postgresql://cdc_user:troque-esta-senha@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
$DB_USER = ($CDCURL -replace 'postgresql://([^:]+):.*','$1')
$DB_PASS = ($CDCURL -replace 'postgresql://[^:]+:([^@]+)@.*','$1')
$DB_HOST = ($CDCURL -replace '.*@([^/]+)/.*','$1')
echo "DB_USER=$DB_USER  DB_HOST=$DB_HOST"
```

O host e a senha saem **da própria string** — você não digita de novo no JSON.

E a segunda credencial — o **owner**, que faz o papel da *aplicação de negócio* nas
mudanças ao vivo do Passo 4 (o `cdc_user` só replica; ele **não escreve**, e isso é
proposital):

**macOS / Linux**
```bash
PGURL='postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
```

**Windows (PowerShell)**
```powershell
$PGURL = 'postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
```

### P.4 — Permissões do conector (idempotentes)

O CDC escreve nos tópicos `pg-payments*`, no heartbeat e no DLQ — e o consume precisa
de grupo:

```bash
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,READ,DESCRIBE --topic pg-payments --prefix
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,DESCRIBE --topic __debezium-heartbeat --prefix
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,DESCRIBE --topic dlq-lcc- --prefix
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group pg-payments. --prefix
```

Aguarde **~30 s** (propagação das ACLs) antes de criar o conector — a lição da Aula 1 vale em dobro aqui.

---

## 💸 O custo desta aula tem dois lados

| Lado | O que acontece |
|---|---|
| **Confluent** | Conector cobra por task/hora, mesmo sem tráfego |
| **Neon** | Conector ligado **impede o banco de hibernar** — queima CU-hours |

Com 100 CU-hours/mês no free tier, um conector esquecido ligado **estoura o Neon em
~4 dias**. Por isso o teardown desta aula tem **duas etapas** — e a segunda quase
ninguém lembra.

---

## O que você vai saber fazer ao final

- Criar um conector CDC apontando para um Postgres na internet
- Explicar cada config obrigatória e por que ela existe
- Ler o envelope Debezium: `op`, `before`, `after`, `source`
- Capturar INSERT, UPDATE e DELETE ao vivo
- Fazer o teardown completo — incluindo o replication slot

---

## Os conceitos em 90 segundos

**As três camadas** — a separação que evita toda a confusão de nomenclatura:

| Camada | Quem é | O que faz |
|---|---|---|
| **Framework** | Kafka Connect | Executa o plugin: tasks, offsets, entrega |
| **Plugin / motor** | **Debezium** | Sabe ler e interpretar o log de cada banco |
| **Tecnologia do banco** | Replicação lógica / WAL | O que o Postgres já faz para durabilidade |

```
 APLICAÇÃO LEGADA (intocada)
      │ INSERT / UPDATE / DELETE
      ▼
 POSTGRES ──► WAL (toda mudança, em ordem, durável)
                │  replicação lógica ◄── o toggle do Setup
                ▼
        ┌─ KAFKA CONNECT ──────────────┐
        │   DEBEZIUM lê o log,          │
        │   monta o evento,             │
        │   marca a posição (LSN)       │
        └───────────────────────────────┘
                ▼
        TÓPICO (key = primary key da linha)
```

**A epifania**: o WAL **é um log append-only de fatos imutáveis** — o banco relacional
sempre teve um "Kafka interno". O CDC só o publica.

> 📖 Log-based × query-based, dual write como anti-padrão e o ciclo snapshot → streaming
> estão **na aula gravada**.

---

## Passo 1 — Monte a configuração *(~8 min)*

**🖱 Pela Console**: **Connectors** → **Postgres CDC Source V2** — o formulário do wizard é o espelho campo a campo do JSON que você vai montar (hostname, port, dbname, user, senha, `topic.prefix`...). Vale abrir lado a lado na demo.

**⌨️ Pela CLI**:

```bash
confluent connect plugin describe PostgresCdcSourceV2 | head -30
```

**O que observar**: as configs com `Required = true`. São nove:
`connector.class`, `database.hostname`, `database.port`, `database.dbname`,
`database.user`, `name`, `output.data.format`, `tasks.max`, `topic.prefix`.

As credenciais entram **sozinhas** — `$SA` da P.1, host e senha derivados do `$CDCURL`
na P.3 (heredoc sem aspas interpola; confira com o `cat` no fim):

**macOS / Linux**
```bash
cat > pg-cdc.json <<EOF
{
  "name": "pg-payments-cdc",
  "connector.class": "PostgresCdcSourceV2",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",

  "database.hostname": "$DB_HOST",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "$DB_PASS",
  "database.dbname": "neondb",
  "database.sslmode": "require",

  "topic.prefix": "pg-payments",
  "table.include.list": "payments.transactions",
  "publication.name": "bootcamp_cdc_pub",
  "slot.name": "bootcamp_cdc_slot",

  "snapshot.mode": "initial",
  "decimal.handling.mode": "precise",
  "tombstones.on.delete": "true",
  "heartbeat.interval.ms": "10000",

  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "tasks.max": "1"
}
EOF
cat pg-cdc.json
```

**Windows (PowerShell)** — here-string com aspas duplas interpola igual:
```powershell
@"
{
  "name": "pg-payments-cdc",
  "connector.class": "PostgresCdcSourceV2",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",
  "database.hostname": "$DB_HOST",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "$DB_PASS",
  "database.dbname": "neondb",
  "database.sslmode": "require",
  "topic.prefix": "pg-payments",
  "table.include.list": "payments.transactions",
  "publication.name": "bootcamp_cdc_pub",
  "slot.name": "bootcamp_cdc_slot",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "precise",
  "tombstones.on.delete": "true",
  "heartbeat.interval.ms": "10000",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "tasks.max": "1"
}
"@ | Set-Content pg-cdc.json
Get-Content pg-cdc.json
```

As configs que merecem atenção:

| Config | Por que importa |
|---|---|
| `database.sslmode: require` | O Neon **exige** TLS; sem isso a conexão é recusada |
| `table.include.list` | Note o schema: **`payments.`**, não `public.` |
| `publication.name` | A publication que o Setup criou |
| `snapshot.mode: initial` | Captura o passado **e** o presente |
| `decimal.handling.mode: precise` | Dinheiro como decimal exato, **nunca** float |
| `heartbeat.interval.ms` | Sem isso, banco ocioso não avança o LSN e o slot retém WAL |
| `tombstones.on.delete` | Emite tombstone após delete (par com compaction) — você vai **vê-lo** no Passo 4 |

> ⚠️ `decimal.handling.mode` merece destaque: o default (`double`) introduz erro de
> ponto flutuante. Para valores monetários isso é inaceitável — R$ 20,13 vira
> 20.129999999999999.

---

## Passo 2 — Crie o conector *(~6 min)*

**🖱 Pela Console**: preencher o wizard e **Launch** cria o mesmo conector. O guia usa o JSON pela CLI para o lab ser copy-pasteável e ficar versionado.

**⌨️ Pela CLI**:

```bash
confluent connect cluster create --config-file pg-cdc.json
```
```
+------+-----------------+
| ID   | lcc-223xmyo     |
| Name | pg-payments-cdc |
+------+-----------------+
```

Acompanhe até RUNNING (CDC leva **~1–2 min** — mais que o Datagen):

Capture o id do conector — pelo nome, sem editar nada:

**macOS / Linux**
```bash
CONN=$(confluent connect cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='pg-payments-cdc'][0])")
echo "CONN=$CONN"
```

**Windows (PowerShell)**
```powershell
$CONN = (confluent connect cluster list -o json | ConvertFrom-Json | Where-Object name -eq "pg-payments-cdc").id
echo "CONN=$CONN"
```

```bash
confluent connect cluster describe $CONN | grep -i status
```
```
| Status | PROVISIONING |
   ↓
| Status | RUNNING      |
```

**O que observar**: um processo na infraestrutura da Confluent acabou de abrir uma
conexão de replicação com o **seu** Postgres, do outro lado da internet. Foi isso que
as configs de `database.*` autorizaram.

Confirme os tópicos criados:

```bash
confluent kafka topic list | grep pg-payments
```
```
  __debezium-heartbeat-lcc-223xmyo.pg-payments | false | 3 | 1
  pg-payments.payments.transactions            | false | 3 | 1
```

**O que observar**: o nome segue `{topic.prefix}.{schema}.{tabela}`. E há um tópico de
**heartbeat** — é o `heartbeat.interval.ms` em ação, mantendo o LSN avançando mesmo
com o banco parado.

### Veja o slot do lado do banco

```bash
docker run --rm postgres:16-alpine psql "$CDCURL" \
  -c "SELECT slot_name, active, plugin FROM pg_replication_slots;"
```
```
     slot_name     | active |  plugin
-------------------+--------+----------
 wal_proposer_slot | t      |
 bootcamp_cdc_slot | t      | pgoutput
```

**O que observar**: `bootcamp_cdc_slot` com `active = t` e plugin `pgoutput`. O
Postgres agora **retém WAL** até esse slot confirmar consumo. É o que garante zero
perda — e é o que enche o disco se você esquecer o conector ligado.

---

## Passo 3 — O snapshot: o passado entra no tópico *(~7 min)*

**🖱 Pela Console**: **Topics → `pg-payments.payments.transactions` → Messages** (offset: beginning) — o snapshot inteiro visível, com o envelope `before`/`after`/`op` em cada registro.

**⌨️ Pela CLI**:

```bash
confluent kafka topic consume pg-payments.payments.transactions --from-beginning \
  --value-format avro --print-offset \
  --group pg-payments.snapshot \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

Deixe rodar uns 30 s e encerre com **Ctrl+C**.

**Output esperado** (um evento, formatado para leitura):
```json
{
  "op": "r",
  "ts_ms": {"long": 1787160515436},
  "before": null,
  "after": {
    "pg_payments.payments.transactions.Value": {
      "account_id": "f343d9eb-7466-4f02-9e7b-72c8e097dedb",
      "amount": "2013/100",
      "authorized_at": {"string": "2026-06-16T12:08:45.000000Z"},
      "status": "SETTLED",
      ...
    }
  }
}
```

**O que observar** — três coisas:

1. **`op: "r"`** = *read*. É o **snapshot**: as 2.000 transações que já existiam estão
   entrando no tópico. O passado do banco virou stream, sem ninguém produzir nada.
2. **`before: null`** — no snapshot não há estado anterior; a linha simplesmente existe.
3. **`"amount": "2013/100"`** — o decimal preciso (R$ 20,13) como fração exata. É o
   `decimal.handling.mode: precise` funcionando. Se fosse `double`, você veria
   `20.129999999999999`.

---

## Passo 4 — Capture as mudanças ao vivo *(~10 min)*

Agora o coração da aula. Você vai precisar dos **dois terminais**.

### Terminal 1 — deixe o consumer escutando

**🖱 Pela Console**: deixe a aba **Messages** aberta enquanto roda os UPDATE/DELETE no Neon — os eventos `op=u` e `op=d` aparecem em segundos. Cena perfeita para a gravação.

**⌨️ Pela CLI**:

```bash
confluent kafka topic consume pg-payments.payments.transactions \
  --value-format avro --group pg-payments.live \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

> ⚠️ **Sem `--from-beginning`** — você quer ver só o que acontecer a partir de agora.
> Deixe rodando.

### Terminal 2 — mexa no banco

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "
INSERT INTO payments.transactions
  (transaction_id, account_id, merchant_id, channel, amount, mcc, uf_transacao, status, fraud_flag, created_at)
SELECT 'bbbbbbbb-0000-4000-8000-000000000002', a.account_id, m.merchant_id,
       'PIX', 777.77, '5411', 'SP', 'CREATED', false, now()
FROM payments.accounts a, payments.merchants m LIMIT 1;"
```

Olhe o Terminal 1. Em segundos:

```json
{
  "op": "c",
  "before": null,
  "after": { "...": { "amount": "77777/100", "status": "CREATED",
                      "transaction_id": "bbbbbbbb-0000-4000-8000-000000000002" }},
  "source": { "connector": "postgresql", "db": "neondb",
              "lsn": {"long": 30733432}, "schema": "payments" }
}
```

**O que observar**: `op: "c"` (create), `before: null`, e o `source` com o **LSN** — a
posição exata no WAL. A latência banco → evento é de **segundos**. CDC é tempo real,
não batch disfarçado.

### Agora UPDATE e DELETE

```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "
UPDATE payments.transactions SET status='AUTHORIZED'
  WHERE transaction_id='bbbbbbbb-0000-4000-8000-000000000002';
DELETE FROM payments.transactions
  WHERE transaction_id='bbbbbbbb-0000-4000-8000-000000000002';"
```

No Terminal 1, **três** mensagens — e a terceira derruba o consumer de propósito:

```
op=u   before.status=CREATED     after.status=AUTHORIZED
op=d   before.status=AUTHORIZED  after=null

Error: unknown magic byte
Suggestions: Check that all messages from this topic are in the avro format.
```

**O erro é esperado — você acabou de conhecer o TOMBSTONE.** Com
`tombstones.on.delete: "true"`, o Debezium emite, depois do `op=d`, uma mensagem com a
chave da linha e **valor NULO** — o sinal de remoção que a log compaction usa para
apagar a chave (Curso 1, Aula 3). O deserializer Avro do CLI não sabe ler valor nulo e
encerra. Numa aplicação real, o consumidor testa `value == null` antes de
desserializar; o CLI é só menos educado.

**O que observar** — e este é o momento-síntese do curso:

- **`op: "u"` traz `before` E `after` preenchidos.** Você não sabe só *como ficou* —
  sabe *o que mudou*. Auditoria e lógica de negócio agradecem.
- **`op: "d"` existe.** Um DELETE virou evento, com o estado anterior preservado. **É
  exatamente isso que polling por query perde para sempre**: quando a linha some da
  tabela, um `SELECT` não tem como saber que ela existiu.
- **E o tombstone fecha o ciclo**: `op=d` conta *a história* da remoção (auditoria);
  o tombstone permite *esquecê-la* (compaction). Os dois juntos são o design completo.

(O consumer já encerrou sozinho no tombstone — não precisa de Ctrl+C.)

> 💡 Se os eventos não aparecerem, é ordem: o consumer precisa estar **no ar antes**
> do comando SQL. Sem `--from-beginning`, ele só vê o que chega depois de subir.

---

## Passo 5 — Teardown em DUAS etapas *(~7 min)*

Aqui está a diferença do Curso 3 para os anteriores.

### Etapa 1 — delete o conector

**🖱 Pela Console**: etapa 1 — conector → **Settings** → **Delete connector**. A etapa 2 (o replication slot no Postgres) **não tem tela na Confluent**: é SQL no Neon, e esquecê-la é o que queima o free tier.

**⌨️ Pela CLI**:

```bash
confluent connect cluster delete $CONN --force
confluent connect cluster list
```
```
None found.
```

### Etapa 2 — o slot que fica para trás

```bash
docker run --rm postgres:16-alpine psql "$CDCURL" \
  -c "SELECT slot_name, active FROM pg_replication_slots;"
```

**Você provavelmente ainda vai ver:**
```
 bootcamp_cdc_slot | t
```

**O conector morreu, mas o slot continua ativo.** A conexão do lado da Confluent leva
alguns minutos para ser liberada. Se você tentar remover agora:

```
ERROR: replication slot "bootcamp_cdc_slot" is active for PID 1114
```

Espere alguns minutos e remova:

```bash
docker run --rm postgres:16-alpine psql "$CDCURL" \
  -c "SELECT pg_drop_replication_slot('bootcamp_cdc_slot');"
```

Confirme:
```bash
docker run --rm postgres:16-alpine psql "$CDCURL" \
  -c "SELECT slot_name, active FROM pg_replication_slots;"
```
```
     slot_name     | active
-------------------+--------
 wal_proposer_slot | t         ← este é interno do Neon, deixe
```

**Por que isso importa**: um slot órfão faz o Postgres reter WAL **indefinidamente**.
Você pode medir:

```bash
docker run --rm postgres:16-alpine psql "$CDCURL" -c \
"SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retido
 FROM pg_replication_slots;"
```

No free tier do Neon, WAL retido consome sua cota de storage. E enquanto o slot estiver
ativo, o banco **não hiberna** — queimando CU-hours.

> 🔴 **A regra do Curso 3**: teardown do CDC é *deletar o conector* **e** *confirmar
> que o slot sumiu*. Só a primeira metade não basta.

---

## Passo 6 — Fixe os conceitos *(~5 min)*

1. Por que "dual write" (a aplicação grava no banco E publica no Kafka) é anti-padrão?
2. Um analista pergunta: "qual era o status ANTES da última atualização?" Que campo responde?
3. O que aconteceria se você criasse as tabelas sem PRIMARY KEY?
4. Você deletou o conector e foi almoçar. Que problema pode estar acontecendo?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque não há transação entre o banco e o Kafka.** A aplicação pode gravar no
banco e falhar ao publicar (ou o contrário) — e os dois divergem **silenciosamente**.
Não existe rollback coordenado entre sistemas distintos sem transação distribuída.

O CDC elimina a classe do problema em vez de mitigá-la: existe **uma escrita só** (no
banco), e o evento é *derivado* dela. Se o commit aconteceu, o evento vai acontecer;
se não aconteceu, não há evento.

**2. O campo `before`.** É o que distingue o CDC de um "SELECT agendado": o envelope
carrega a **transição**, não só o estado final. E isso só funciona porque o Setup
configurou `REPLICA IDENTITY FULL` — sem isso, `before` traria apenas a chave primária.

**3. O CDC funcionaria, mas sem garantia de ordem por linha.** A PK vira a **key do
evento**; a key decide a partição; a partição garante a ordem (Curso 1, Aula 5). Sem
PK, os eventos da mesma linha podem cair em partições diferentes — e um `SETTLED`
pode ser processado antes do `CREATED`.

**4. O replication slot pode ter ficado órfão.** Se ele ainda estiver lá:
- o Postgres retém WAL indefinidamente (consumindo storage)
- o Neon **não hiberna**, queimando CU-hours do free tier

Foi por isso que o Passo 5 tem duas etapas.

</details>

---

## Se algo não funcionou

**`permission denied for table transactions` no INSERT/UPDATE**
Você usou `$CDCURL` numa escrita. O `cdc_user` é **least-privilege de propósito**
(replicação + SELECT, sem escrita) — as mudanças de negócio rodam como owner, com
`$PGURL` (P.3). O erro é a prova de que a separação de papéis funciona.

**`Error: Forbidden Access` no `api-key create`**
`$SA` está vazio (variável não capturada — no zsh, vazia ela **some** do comando) ou a
service account acabou de ser criada (propagação de ~10 s). Rode `echo "$CLUSTER $SA"`:
os dois devem aparecer. Vazio → refaça a captura do P.1.

**Fechei o terminal / perdi as variáveis**
Rode a **Preparação** de novo (autocaptura + key nova + colar o CDCURL).

**`FAILED` com `password authentication failed for user 'cdc_user'`**
O `$DB_PASS` entrou errado no JSON — o clássico é ter colado a URL do **owner** no
`CDCURL` (o sed não acha `cdc_user:` e devolve a string inteira, em silêncio). Confira
com `grep password pg-cdc.json`. Refaça o P.3 conferindo `DB_USER=cdc_user` no echo,
regenere o JSON (Passo 2) e delete/recrie o conector.

**Conector vai para `FAILED` logo após criar**
```bash
confluent connect cluster describe $CONN
```
Procure o campo `Trace`. Causas mais comuns, nesta ordem:
1. `wal_level` não é `logical` — volte ao [Setup](../setup/README.md), Passo 2
2. Senha do `cdc_user` errada
3. `database.sslmode` ausente (o Neon exige TLS)
4. `table.include.list` com schema errado (é `payments.`, não `public.`)

**`publication "bootcamp_cdc_pub" does not exist`**
O `03_cdc_setup.sql` não rodou. Volte ao Setup, Passo 4.

**Os eventos ao vivo não aparecem**
O consumer precisa estar no ar **antes** do comando SQL. Sem `--from-beginning`, ele
começa do fim do tópico.

**`before` vem só com a PK no UPDATE**
Falta `REPLICA IDENTITY FULL` naquela tabela:
```sql
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;
```

**`Error: unknown magic byte` após o DELETE**
Esperado — é o **tombstone** (valor nulo) que o `tombstones.on.delete` emite depois do
`op=d`. O CLI não desserializa valor nulo e encerra. A demo está completa; siga para o
teardown.

**`pg_drop_replication_slot` falha com "is active"**
Esperado logo após deletar o conector. Espere alguns minutos e repita.

---

## O que fica de pé

| Recurso | Estado |
|---|---|
| Tópico `pg-payments.payments.transactions` | **Fica** — a [Aula 4](../aula4/README.md) consome dele |
| Schema no Registry | Fica |
| Conector | **Deletado** |
| Replication slot | **Removido** |

---

## Glossário

| Termo | Definição |
|---|---|
| **CDC** | Capturar mudanças de um banco como fluxo de eventos. |
| **Log-based CDC** | Captura lendo o log de transações — completa, ordenada, barata. |
| **Debezium** | O plugin/motor de CDC que roda dentro do Connect. |
| **Envelope Debezium** | `before`, `after`, `op`, `source`, `ts_ms`. |
| **`op`** | `c`=create, `u`=update, `d`=delete, `r`=read (snapshot). |
| **LSN** | Log Sequence Number: a posição no WAL. |
| **Replication slot** | Marcador que faz o Postgres reter WAL até o consumo ser confirmado. |
| **Tombstone** | Mensagem de valor nulo que sinaliza remoção (par da compaction). |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Entregar o que o CDC capturou | [Aula 3](../aula3/README.md) — sink |
| Achatar o envelope (só o `after`) | [Aula 4](../aula4/README.md) — SMT |
| Enriquecer o fluxo CDC | Curso 4 — Flink |
| Changelog como tabela | Curso 5 — Tableflow |

**Fontes**

- Confluent — [Postgres CDC Source V2 (Debezium)](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/)
- Debezium — [PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- Neon — [Logical replication to Confluent](https://neon.com/docs/guides/logical-replication-kafka-confluent)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 1 · Introdução ao Connect](../aula1/README.md) | [Curso 3](../README.md) | [Aula 3 · Sink connectors](../aula3/README.md) |
