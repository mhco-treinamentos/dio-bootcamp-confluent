# Curso 3 · Aula 4 — SMT e Dead Letter Topic: pipeline que não cai

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~45 min · ☁️ **Conector cobra por hora** · 1 terminal

Esta aula fecha o curso com o que separa pipeline de demo de pipeline de produção: **transformar na passagem** (SMT) e **ter destino digno para a mensagem que não dá para processar** (DLQ).

E ela resolve algo que ficou pendente: na [Aula 3](../aula3/README.md), o sink funcionou com um tópico comum — mas ligá-lo direto ao tópico **CDC** exige mais. Aqui você faz essa emenda funcionar.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito as Aulas [2](../aula2/README.md) e [3](../aula3/README.md) | CDC e sink separadamente |
| 1 terminal | Com contexto e credenciais |

## Preparação — contexto do zero *(~6 min)*

Nada aqui depende de terminal antigo. Os únicos "colar": os secrets das duas keys
novas (P.2) e a connection string do Neon (P.3) — o resto é autocaptura.

### P.1 — Capture os IDs

**macOS / Linux**
```bash
CLUSTER=$(confluent kafka cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print(([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos']+[''])[0])")
ENV=$(confluent environment list -o json | python3 -c "import json,sys; print([e['id'] for e in json.load(sys.stdin) if e['is_current']][0])")
SR=$(confluent schema-registry cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['cluster'])")
SR_URL=$(confluent schema-registry cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['endpoint_url'])")
echo "CLUSTER=$CLUSTER SA=$SA ENV=$ENV SR=$SR"
```

**Windows (PowerShell)**
```powershell
$CLUSTER = (confluent kafka cluster describe -o json | ConvertFrom-Json).id
$SA = (confluent iam service-account list -o json | ConvertFrom-Json | Where-Object name -eq "app-pedidos").id
$ENV_ID = (confluent environment list -o json | ConvertFrom-Json | Where-Object is_current).id
$SR = (confluent schema-registry cluster describe -o json | ConvertFrom-Json).cluster
$SR_URL = (confluent schema-registry cluster describe -o json | ConvertFrom-Json).endpoint_url
echo "CLUSTER=$CLUSTER SA=$SA ENV=$ENV_ID SR=$SR"
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

### P.2 — As DUAS keys (cluster e Schema Registry)

```bash
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
confluent api-key create --resource $SR --service-account $SA --description "Key de Schema Registry - app-pedidos"
```

**macOS / Linux**
```bash
CC_API_KEY="SUA-API-KEY"
CC_API_SECRET="cflt-SEU-SECRET-AQUI"
SR_KEY="SUA-API-KEY"
SR_SECRET="cflt-SEU-SECRET-AQUI"
```

**Windows (PowerShell)**
```powershell
$CC_API_KEY = "COLE_A_KEY_DO_CLUSTER"
$CC_API_SECRET = "COLE_O_SECRET_DO_CLUSTER"
$SR_KEY = "COLE_A_KEY_DO_SR"
$SR_SECRET = "COLE_O_SECRET_DO_SR"
```

### P.3 — A connection string do Neon (a do OWNER, do Setup P.1)

> 🎬 A connection string abaixo é do **ambiente de gravação** (destruído ao fim do
> curso). **Aluno**: troque pela SUA, do Setup.

**macOS / Linux**
```bash
PGURL='postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
DB_HOST=$(echo "$PGURL" | sed -E 's|.*@([^/]+)/.*|\1|')
DB_PASS=$(echo "$PGURL" | sed -E 's|.*neondb_owner:([^@]+)@.*|\1|')
echo "DB_HOST=$DB_HOST"
```

**Windows (PowerShell)**
```powershell
$PGURL = 'postgresql://neondb_owner:SUA-SENHA-NEON@ep-SEU-HOST.us-east-2.aws.neon.tech/neondb?sslmode=require'
$DB_HOST = ($PGURL -replace '.*@([^/]+)/.*','$1')
$DB_PASS = ($PGURL -replace '.*neondb_owner:([^@]+)@.*','$1')
echo "DB_HOST=$DB_HOST"
```

### P.4 — Permissões da aula (idempotentes)

O sink lê os tópicos do CDC, escreve o DLQ e roda como grupo `connect-`; você
inspeciona o DLQ com a key do cluster:

```bash
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --topic pg-payments --prefix
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,READ,DESCRIBE --topic dlq_ --prefix
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group connect- --prefix
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group dlq_ --prefix
```

> 🔁 **Replay rápido**: esta aula consome os tópicos do CDC da [Aula 2](../aula2/README.md).
> Se você fez teardown do conector, recrie-o (Aula 2: Preparação + Passo 2, ~3 min) —
> os dados voltam via snapshot.

Aguarde ~15 s e a aula é sua.

---

## O que você vai saber fazer ao final

- Aplicar uma SMT para achatar o envelope CDC antes do sink
- Saber quais SMTs o Confluent Cloud **permite** (a lista é fechada)
- Configurar um Dead Letter Topic com headers de diagnóstico
- Reconhecer o tombstone como mensagem venenosa — e tratá-lo
- Decidir entre SMT e Flink

---

## Os conceitos em 90 segundos

**SMT (Single Message Transform)** — transformação aplicada **mensagem a mensagem, na
passagem pelo conector**. Sem estado, sem janela, sem processador externo:

```
 SOURCE:  fonte ─► [SMT 1] ─► [SMT 2] ─► ... ─► tópico
 SINK:    tópico ─► [SMT 1] ─► [SMT 2] ─► ... ─► destino
```

**A régua de decisão**:

> SMT é para transformação **1-para-1, sem estado, cosmética**: renomear, mascarar,
> achatar, rotear. No momento em que a necessidade envolve **juntar, agregar, janelar
> ou cruzar fluxos** — é Flink (Curso 4).

**DLQ (Dead Letter Topic)** — as três respostas à mensagem venenosa:

```
 errors.tolerance=none    → conector FALHA        (1 veneno derruba tudo)   ✗
 "ignora e segue"         → mensagem DESCARTADA   (perda silenciosa)        ✗
 errors.tolerance=all
   + DLQ configurada      → veneno DESVIADO       (vivo e diagnosticável)   ✓
```

> 📖 O ciclo de vida do veneno (desviado → diagnosticado → reprocessado) e o DLT como
> padrão geral de EDA estão **na aula gravada**.

---

## Passo 1 — A lista fechada de SMTs *(~6 min)*

Primeira lição prática, e ela surpreende quem vem do Kafka self-managed:

**O Confluent Cloud só permite SMTs de uma lista pré-aprovada.**

Tente usar a SMT clássica do Debezium para achatar CDC:

```json
{ "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState" }
```

**Output esperado — o erro é a lição**
```
Error: invalid transforms selected: allowed transforms for organization:
[io.confluent.connect.cloud.transforms.TopicRegexRouter
 org.apache.kafka.connect.transforms.ExtractField$Key
 org.apache.kafka.connect.transforms.ExtractField$Value
 org.apache.kafka.connect.transforms.Flatten$Value
 org.apache.kafka.connect.transforms.MaskField$Value
 io.confluent.connect.transforms.Drop$Value
 ...]
```

**O que observar**: `ExtractNewRecordState` **não está na lista**. É a fronteira de
gestão da Aula 1 do Curso 2 aparecendo de novo — no gerenciado, você ganha operação
e perde extensibilidade arbitrária.

As SMTs mais úteis do catálogo permitido:

| SMT | O que faz |
|---|---|
| `ExtractField$Value` | Extrai um campo (é como achatamos o CDC) |
| `Flatten$Value` | Achata estruturas aninhadas |
| `MaskField$Value` | Mascara valores sensíveis (CPF, e-mail — LGPD) |
| `InsertField$Value` | Injeta metadado (timestamp, origem) |
| `TopicRegexRouter` | Reescreve o nome do tópico destino |
| `Filter$Value` | Descarta mensagens por condição |

> 💡 Precisa de uma transformação que não está na lista? A resposta é **Flink**
> (Curso 4) — e quase sempre é a resposta certa mesmo quando a SMT existiria.

---

## Passo 2 — Achate o envelope CDC *(~8 min)*

Na [Aula 2](../aula2/README.md), o CDC produziu o envelope Debezium:

**🖱 Pela Console**: no wizard do conector, a seção **Transforms** monta o mesmo SMT clicando (`ExtractField$Value`, field `after`) — é o espelho visual do bloco `transforms` do JSON.

**⌨️ Pela CLI**:

```json
{"op":"c", "before":null, "after":{...}, "source":{...}, "ts_ms":...}
```

O sink JDBC **não sabe o que fazer com isso** — ele espera colunas planas. A SMT
extrai só o `after`:

```bash
# repare no \$Value: o \ protege o "$" do ExtractField\$Value da expansão do shell
cat > sink-cdc.json <<EOF
{
  "name": "cdc-sink",
  "connector.class": "PostgresSink",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",
  "topics": "pg-payments.payments.transactions",

  "transforms": "extractAfter",
  "transforms.extractAfter.type": "org.apache.kafka.connect.transforms.ExtractField\$Value",
  "transforms.extractAfter.field": "after",

  "table.name.format": "transacoes_espelho",

  "input.data.format": "AVRO",
  "input.key.format": "AVRO",
  "connection.host": "$DB_HOST",
  "connection.port": "5432",
  "connection.user": "neondb_owner",
  "connection.password": "$DB_PASS",
  "db.name": "neondb",
  "ssl.mode": "require",

  "insert.mode": "UPSERT",
  "pk.mode": "record_key",
  "pk.fields": "transaction_id",
  "delete.enabled": "true",
  "auto.create": "true",
  "auto.evolve": "true",

  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq_transacoes",
  "errors.deadletterqueue.context.headers.enable": "true",

  "db.timezone": "UTC",
  "date.timezone": "UTC",
  "timestamp.precision.mode": "microseconds",

  "schema.registry.url": "$SR_URL",
  "schema.registry.basic.auth.credentials.source": "USER_INFO",
  "schema.registry.basic.auth.user.info": "$SR_KEY:$SR_SECRET",
  "value.converter.schema.registry.url": "$SR_URL",
  "key.converter.schema.registry.url": "$SR_URL",
  "key.converter.schema.registry.basic.auth.credentials.source": "USER_INFO",
  "key.converter.schema.registry.basic.auth.user.info": "$SR_KEY:$SR_SECRET",

  "tasks.max": "1"
}
EOF
cat sink-cdc.json
```

Três configs merecem destaque:

| Config | Por que está aqui |
|---|---|
| `transforms.extractAfter.field: after` | **Achata o envelope** — o coração do passo |
| `table.name.format: transacoes_espelho` | O tópico CDC tem **pontos** no nome; sem isto, o sink falha em silêncio (Aula 3) |
| `delete.enabled: true` | **O tombstone** — veja o Passo 4 |

Não esqueça o RBAC do Schema Registry para este subject:

```bash
confluent iam rbac role-binding create \
  --principal User:$SA --role ResourceOwner \
  --resource 'Subject:pg-payments' --prefix \
  --environment $ENV --schema-registry-cluster $SR
```

Crie:

```bash
confluent connect cluster create --config-file sink-cdc.json
```

---

## Passo 3 — Veja o espelho encher *(~6 min)*

**🖱 Pela Console**: **Topics → `flat-payments...` → Messages** — os registros achatados, sem envelope, lado a lado com o tópico bruto.

**⌨️ Pela CLI**:

Capture o id do conector pelo nome:

**macOS / Linux**
```bash
CONN=$(confluent connect cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='cdc-sink'][0])")
echo "CONN=$CONN"
```

**Windows (PowerShell)**
```powershell
$CONN = (confluent connect cluster list -o json | ConvertFrom-Json | Where-Object name -eq "cdc-sink").id
echo "CONN=$CONN"
```

```bash
confluent connect cluster describe $CONN | grep -i status
```

Aguarde `RUNNING` e consulte o destino:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT count(*) FROM public.transacoes_espelho;"
```

**Output esperado**
```
 count
-------
  2000
```

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT transaction_id, channel, amount, status FROM public.transacoes_espelho LIMIT 3;"
```
```
            transaction_id            | channel | amount | status
--------------------------------------+---------+--------+---------
 89a2688b-12c1-46e0-9998-5f15ff002d4d | CREDIT  |  20.13 | SETTLED
 be0f051b-1b66-45a9-a3c4-36571d8cbbac | BOLETO  | 191.35 | SETTLED
 bdc14f1f-295d-4fbf-830f-801dfad409e2 | CREDIT  | 359.58 | SETTLED
```

**O que observar**: colunas **planas e limpas**. O envelope com `before`/`after`/`op`
virou uma linha de tabela. A SMT fez isso na passagem — sem processador, sem código,
sem tópico intermediário.

**E o ciclo fechou**: `payments.transactions` (Neon) → CDC → Kafka → sink →
`transacoes_espelho` (Neon). Um banco alimentando outro sistema, com o Kafka no meio
e **zero linhas de código de integração**.

---

## Passo 4 — O tombstone: a mensagem venenosa *(~8 min)*

Aqui está o veneno real desta aula — e ele aparece sozinho.

Se você configurar `delete.enabled: false` (o default), o conector vai a **FAILED**
assim que encontrar um DELETE do CDC. O erro é explícito:

```
The connector expected a record with a primary key and a record on the topic did not.
There are two common causes. First, a tombstone record with a null value while
delete.enabled=false: set delete.enabled=true together with pk.mode=record_key
to process deletes.
```

**Por que isso acontece**: quando você apagou uma linha na Aula 2, o Debezium emitiu
**dois** eventos — o `op: "d"` e um **tombstone** (mesma key, valor `null`). O
tombstone existe para a log compaction poder remover o registro fisicamente.

Para o sink, uma mensagem com valor nulo é indecifrável: qual coluna ele escreveria?
Sem `delete.enabled`, ele não sabe que aquilo significa "apague a linha".

| Config | Comportamento diante do tombstone |
|---|---|
| `delete.enabled: false` | **Conector FALHA** — um DELETE derruba o pipeline |
| `delete.enabled: true` + `pk.mode: record_key` | Vira `DELETE FROM ... WHERE pk = ...` |

> 💡 **Esta é a lição de produção da aula.** O pipeline funcionou perfeitamente com
> 2.000 INSERTs. Bastou **um DELETE** na origem para derrubá-lo. Veneno não é
> hipótese — é o comportamento normal do CDC.

---

## Passo 5 — O Dead Letter Topic *(~8 min)*

O `delete.enabled` resolve o tombstone. Mas e a mensagem que você **não** previu?

As três configs que separam pipeline frágil de resiliente:

**🖱 Pela Console**: a página do conector mostra o link do tópico **DLQ** dele; em **Topics → `dlq-lcc-...` → Messages**, os venenos desviados — cada um com os headers de erro.

**⌨️ Pela CLI**:

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq_transacoes",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

| Config | O que faz |
|---|---|
| `errors.tolerance: all` | Não derruba o conector; desvia o problema |
| `...topic.name` | Onde o veneno vai parar |
| `...context.headers.enable` | **Sempre `true`** — DLT sem contexto é cemitério |

Veja se o DLQ recebeu algo:

```bash
confluent kafka topic list | grep dlq
```

Se houver mensagens, inspecione com os headers:

```bash
confluent kafka topic consume dlq_transacoes --from-beginning \
  --value-format string --full-header \
  --group dlq_transacoes.viewer \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

**O que observar nos headers**: a mensagem original vai **intacta**, e os headers
carregam o diagnóstico:

```
__connect.errors.topic          → de onde veio
__connect.errors.partition      → posição exata
__connect.errors.offset
__connect.errors.exception.class    → o PORQUÊ
__connect.errors.exception.message
__connect.errors.stage          → onde no pipeline quebrou
```

**A diferença que isso faz**: sem DLQ, você teria um conector FAILED e um log para
caçar. Com DLQ, o dado está preservado, o motivo viaja junto, e **o resto do fluxo
continuou passando**.

> ⚠️ `errors.tolerance: all` **sem** DLQ configurada não é tolerância — é **perda
> silenciosa**. A mensagem some sem rastro. É o pior dos três cenários, porque
> parece que está tudo bem.

---

## Passo 6 — SMT ou Flink? *(~5 min)*

Classifique cada necessidade: **SMT** ou **Flink** (Curso 4)?

| # | Necessidade | SMT ou Flink? |
|---|---|---|
| 1 | Mascarar o CPF antes de entregar ao Elasticsearch | |
| 2 | Calcular o total gasto por cliente por hora | |
| 3 | Renomear o tópico `pg-payments.public.x` para `x` | |
| 4 | Marcar transações cujo valor é 10× a média do cartão | |
| 5 | Achatar o envelope CDC | |
| 6 | Juntar transações com o cadastro de contas | |

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

| # | Resposta | Por quê |
|---|---|---|
| 1 | **SMT** (`MaskField$Value`) | 1-para-1, sem estado, cosmética |
| 2 | **Flink** | Exige **agregação** e **janela** — SMT não tem memória |
| 3 | **SMT** (`TopicRegexRouter`) | Roteamento puro |
| 4 | **Flink** | Precisa da **média histórica** — estado |
| 5 | **SMT** (`ExtractField$Value`) | Foi o Passo 2 |
| 6 | **Flink** | **Join entre fluxos** — a SMT vê uma mensagem por vez |

**O critério**: SMT enxerga **uma mensagem, isolada, sem memória**. Se a
transformação precisa de *outra* mensagem — anterior, de outro tópico, ou de uma
janela de tempo — é Flink.

SMT com lógica de negócio é dívida técnica declarada.

</details>

---

## Limpar — teardown completo do curso *(~5 min)*

```bash
# 1. o conector
confluent connect cluster delete $CONN --force
confluent connect cluster list
```

- `confluent connect cluster list` — None found.

Se você recriou o CDC source nesta aula, faça também a **segunda etapa** (Aula 2):

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT slot_name, active FROM pg_replication_slots;"

# aguarde ficar inactive, então:
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT pg_drop_replication_slot('bootcamp_cdc_slot');"
```

Confirme o billing:

```bash
confluent billing cost list \
  --start-date $(date -v-1d +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

> 💸 **O que você aprendeu a evitar**, com números reais:
>
> | Conector | $/task-hora | Se esquecer ligado |
> |---|---|---|
> | Datagen | $0,0347 | ~$25/mês |
> | Postgres Sink | $0,1042 | ~$75/mês |
> | **CDC Debezium** | **$0,50** | **~$360/mês** |
>
> E no Neon: conector CDC ligado impede a hibernação — **~4 dias** para estourar o
> free tier.

A tabela `transacoes_espelho` pode ficar como evidência do que você construiu.

---

## Se algo não funcionou

**`Error: Forbidden Access` no `api-key create`**
`$SA` está vazio (variável não capturada — no zsh, vazia ela **some** do comando) ou a
service account acabou de ser criada (propagação de ~10 s). Rode `echo "$CLUSTER $SA"`:
os dois devem aparecer. Vazio → refaça a captura do P.1.

**Fechei o terminal / perdi as variáveis**
Rode a **Preparação** de novo (autocaptura; keys novas).


**`invalid transforms selected`**
A SMT não está na lista permitida. O erro lista as disponíveis — escolha entre elas ou
leve a transformação para o Flink.

**Conector vai a FAILED mencionando "tombstone" ou "primary key"**
É o Passo 4: `delete.enabled: true` junto com `pk.mode: record_key`.

**RUNNING, mas nada é escrito**
As três armadilhas (Aula 3): ponto no nome do tópico (use `table.name.format`), RBAC
do Schema Registry, e `pk.fields` ausente.

**O DLQ está vazio**
Boa notícia — nada falhou. Para vê-lo em ação, force um erro: aponte o sink para uma
tabela com tipo incompatível, ou remova `delete.enabled` e provoque um DELETE.

**`Failed to connect with Schema Registry`**
Confira o RBAC com `curl` antes de mexer no conector:
```bash
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects/SEU-SUBJECT/versions" -w "\nHTTP %{http_code}\n"
```

---

## Glossário

| Termo | Definição |
|---|---|
| **SMT** | Transformação 1-para-1, sem estado, aplicada na passagem pelo conector. |
| **`ExtractField$Value`** | SMT que extrai um campo do valor — como achatamos o CDC. |
| **Lista permitida** | O Confluent Cloud só aceita SMTs pré-aprovadas. |
| **Tombstone** | Mensagem com key e valor `null`; sinaliza remoção. |
| **`delete.enabled`** | Faz o sink traduzir tombstone em `DELETE`. |
| **DLT / DLQ** | Tópico de desvio para mensagens não processáveis. |
| **Context headers** | Metadados de diagnóstico anexados à mensagem desviada. |

---

## Você terminou o Curso 3 🎉

| Aula | O que você levou |
|---|---|
| Setup | Um Postgres na internet, com replicação lógica |
| 1 | Integração por configuração — e o teardown como hábito |
| 2 | O banco legado virou fonte de eventos, sem tocar na aplicação |
| 3 | Entrega idempotente com upsert |
| 4 | Transformação na passagem e destino digno para o veneno |

**No Curso 4**, os dados param de só *passar* e começam a ser **transformados de
verdade**: Apache Flink, com estado, janelas e joins.

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Transformação com estado | **Curso 4** — Flink |
| DLQ nas Data Quality Rules | Curso 2, Aula 3 — o mesmo padrão |
| Alerta sobre profundidade do DLT | Curso 7 |
| Mascaramento e LGPD | Curso 7 — governança |

**Fontes**

- Confluent — [Single Message Transforms for managed connectors](https://docs.confluent.io/cloud/current/connectors/single-message-transforms.html)
- Confluent — [Error handling and DLQ](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/)
- Apache Kafka — [Connect transforms](https://kafka.apache.org/documentation/#connect_transforms)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 3 · Sink connectors](../aula3/README.md) | [Curso 3](../README.md) | [Curso 4 · Apache Flink](../../curso4/README.md) |
