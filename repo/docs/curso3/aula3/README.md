# Curso 3 · Aula 3 — Sink connectors: entregando aos sistemas de destino

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~40 min · ☁️ **Conector cobra por hora** · 1 terminal

A [Aula 2](../aula2/README.md) trouxe dados do banco para o Kafka. Esta fecha o ciclo: leva do Kafka **de volta para um banco** — e mostra por que uma mensagem entregue duas vezes não estraga nada.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito o [Setup](../setup/README.md) | Neon acessível |
| Ter feito a [Aula 1](../aula1/README.md) | Você já criou um conector |
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

O sink **lê** o tópico e roda como um consumer group `connect-lcc-...`; você produz
com a key do cluster:

```bash
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,READ,DESCRIBE --topic pedidos_ --prefix
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group connect- --prefix
```

Aguarde ~15 s e a aula é sua.

---

## O que você vai saber fazer ao final

- Criar um sink que escreve num Postgres, com a tabela criada automaticamente
- Explicar por que um sink é "um consumer group com superpoderes"
- Provar que o **upsert torna a duplicata inofensiva**
- Reconhecer as três armadilhas que fazem um sink falhar em silêncio

---

## Os conceitos em 90 segundos

Um sink é, por baixo, **um consumer group** (Curso 1, Aula 4) com entrega para fora:

```
 TÓPICO ──► SINK CONNECTOR
              │ consumer group interno (offsets, lag — tudo vale)
              │ converter: Avro + Schema Registry → estrutura
              │ batching: acumula N eventos ou T ms
              ▼
           DESTINO (banco, DW, NoSQL, storage)
              │ escreve em lote
              ▼
           commit de offset  ◄── SÓ depois da escrita confirmada
```

**A ordem sagrada**: processa → escreve no destino → **então** commita. Se o sink
morrer entre a escrita e o commit, o lote é **relido e reescrito**.

Isso é o at-least-once do Curso 1, Aula 5 — e a resposta é a mesma: **não evitar a
duplicata, torná-la inofensiva**. No mundo dos sinks, isso tem nome: **upsert**.

> 📖 O panorama dos destinos (DW, NoSQL, object storage) e o spoiler do Tableflow
> estão **na aula gravada**.

---

## Passo 1 — Prepare o tópico de origem *(~6 min)*

> ⚠️ **Atenção ao nome do tópico** — e isto não é detalhe.

**🖱 Pela Console**: **Topics** → **+ Add topic** → `pedidos_sink`, 3 partições; depois aba **Schema** → **Set a schema** para o contrato.

**⌨️ Pela CLI**:

```bash
confluent kafka topic create pedidos_sink --partitions 3
```

Repare no `_`. Um tópico com **ponto** no nome (`lab.pedidos`) faz o sink JDBC
falhar **silenciosamente**: fica RUNNING, task ativa, e não escreve nada. O motivo é
que o nome da tabela é derivado do tópico, e `lab.pedidos` vira
`public."lab.pedidos"` — ambíguo para o Postgres.

Produza três pedidos:

```bash
cat > pedido.avsc <<'EOF'
{"type":"record","name":"Pedido","namespace":"bootcamp","fields":[
 {"name":"order_id","type":"string"},
 {"name":"customer_id","type":"string"},
 {"name":"status","type":"string"},
 {"name":"total","type":"double"}]}
EOF

confluent kafka topic produce pedidos_sink \
  --parse-key --delimiter : \
  --value-format avro --schema pedido.avsc \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" <<'FIM'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"CREATED","total":289.90}
ord-2:{"order_id":"ord-2","customer_id":"cli-42","status":"CREATED","total":150.00}
ord-3:{"order_id":"ord-3","customer_id":"cli-99","status":"PAID","total":75.50}
FIM
```

**Windows (PowerShell)**
```powershell
@'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"CREATED","total":289.90}
ord-2:{"order_id":"ord-2","customer_id":"cli-42","status":"CREATED","total":150.00}
ord-3:{"order_id":"ord-3","customer_id":"cli-99","status":"PAID","total":75.50}
'@ | confluent kafka topic produce pedidos_sink --parse-key --delimiter : --value-format avro --schema pedido.avsc --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

O comando termina sozinho — as 3 mensagens vão embutidas (sem Ctrl+D).

---

## Passo 2 — Autorize a service account no Schema Registry *(~6 min)*

Este passo existe por causa de um erro que consome horas de quem não sabe dele.

**API key válida no Schema Registry não é suficiente.** O SR tem autorização própria
(RBAC), separada das ACLs do Kafka. Sem ela, o sink autentica, fica RUNNING — e não
processa nada, porque não consegue desserializar.

Descubra o ID do seu Schema Registry:

**🖱 Pela Console**: ☰ → **Accounts & access** → a service account → aba **Access** → **+ Add role assignment** → role `DeveloperRead` com escopo no Schema Registry/subject. É o binding que o comando abaixo cria.

**⌨️ Pela CLI**:

```bash
confluent schema-registry cluster describe | grep Cluster
```

Conceda o acesso:

```bash
confluent iam rbac role-binding create \
  --principal User:$SA \
  --role ResourceOwner \
  --resource 'Subject:pedidos' --prefix \
  --environment $ENV \
  --schema-registry-cluster $SR
```

> As aspas em `'Subject:pedidos'` evitam que o shell interprete o texto. E `--prefix`
> faz valer para todo subject que **comece** com `pedidos` — sem ele, o nome é
> tratado como literal.

### Verifique antes de seguir

```bash
curl -s -u "$SR_KEY:$SR_SECRET" \
  "$SR_URL/subjects/pedidos_sink-value/versions" -w "\nHTTP %{http_code}\n"
```

**Output esperado**
```
[1]
HTTP 200
```

**Se vier `HTTP 403`** com `"User is denied operation Read on Subject"`, o role
binding não pegou. Não adianta criar o conector — ele vai ficar RUNNING sem fazer nada.

> 💡 **Guarde este diagnóstico.** Sempre que um conector com Avro ficar RUNNING sem
> processar, teste a leitura do subject com `curl` antes de investigar o conector.

---

## Passo 3 — Crie o sink *(~8 min)*

**🖱 Pela Console**: **Connectors** → **Postgres Sink** → wizard (tópico de origem, credenciais do Neon, `insert.mode: UPSERT`, `pk.mode: record_key`) → **Launch**.

**⌨️ Pela CLI**:

Tudo entra **sozinho** — `$SA`, host/senha do `$PGURL`, e as credenciais de SR
(heredoc sem aspas interpola; confira com o `cat`). No PowerShell, use here-string
`@"..."@` com o mesmo conteúdo e `Set-Content sink.json`:

```bash
cat > sink.json <<EOF
{
  "name": "pedidos-sink",
  "connector.class": "PostgresSink",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",
  "topics": "pedidos_sink",

  "input.data.format": "AVRO",
  "input.key.format": "STRING",

  "connection.host": "$DB_HOST",
  "connection.port": "5432",
  "connection.user": "neondb_owner",
  "connection.password": "$DB_PASS",
  "db.name": "neondb",
  "ssl.mode": "require",

  "insert.mode": "UPSERT",
  "pk.mode": "record_key",
  "pk.fields": "order_id",
  "auto.create": "true",
  "auto.evolve": "true",
  "delete.enabled": "false",

  "db.timezone": "UTC",
  "date.timezone": "UTC",
  "timestamp.precision.mode": "microseconds",

  "schema.registry.url": "$SR_URL",
  "schema.registry.basic.auth.credentials.source": "USER_INFO",
  "schema.registry.basic.auth.user.info": "$SR_KEY:$SR_SECRET",
  "value.converter.schema.registry.url": "$SR_URL",

  "tasks.max": "1"
}
EOF
cat sink.json

confluent connect cluster create --config-file sink.json
```

As configs que merecem explicação:

| Config | Por que importa |
|---|---|
| `insert.mode: UPSERT` | **O pulo do gato** — insere se novo, atualiza se existe |
| `pk.mode: record_key` | A chave do upsert vem da **key da mensagem** |
| `pk.fields: order_id` | Nomeia o campo — **obrigatório** com `record_key` |
| `auto.create: true` | O sink cria a tabela a partir do schema Avro |
| `timestamp.precision.mode` | Só aceita `microseconds`; `connect` é rejeitado |
| `schema.registry.*` | **Quatro** configs; faltar uma derruba o conector |

Aguarde ficar RUNNING. Capture o id do conector pelo nome:

**macOS / Linux**
```bash
CONN=$(confluent connect cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='pedidos-sink'][0])")
echo "CONN=$CONN"
```

**Windows (PowerShell)**
```powershell
$CONN = (confluent connect cluster list -o json | ConvertFrom-Json | Where-Object name -eq "pedidos-sink").id
echo "CONN=$CONN"
```

```bash
confluent connect cluster describe $CONN | grep -i status
```

---

## Passo 4 — Veja o ciclo fechar *(~6 min)*

**🖱 Pela Console (Neon)**: o **SQL Editor** do console do Neon roda o mesmo SELECT — dá para mostrar a tabela enchendo sem sair do browser.

**⌨️ Pela CLI**:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT * FROM public.pedidos_sink;"
```

**Output esperado**
```
 order_id | customer_id | status  | total
----------+-------------+---------+-------
 ord-1    | cli-99      | CREATED | 289.9
 ord-2    | cli-42      | CREATED |   150
 ord-3    | cli-99      | PAID    |  75.5
(3 rows)
```

**O que observar** — três coisas:

1. **A tabela não existia.** O `auto.create` a criou a partir do schema Avro: nomes e
   tipos de coluna vieram do contrato do Registry. **O Schema Registry acabou de
   ditar DDL num banco.**
2. Você não escreveu consumer, nem `INSERT`, nem mapeamento objeto-relacional. Foi
   configuração.
3. Se você fez a Aula 2, o ciclo fechou: **banco → Kafka → banco**, sem integração
   ponto a ponto.

> ⚠️ `auto.create` e `auto.evolve` são mágicos em aula e **perigosos em produção**:
> é o schema do tópico ditando estrutura no destino. Use com contrato BACKWARD sólido
> e saiba quem pode evoluir o quê (Curso 2, Aula 3).

---

## Passo 5 — A prova do upsert *(~7 min)*

Aqui está a lição da aula. Reenvie `ord-1` **duas vezes**, com status diferente:

**🖱 Pela Console**: produza pela aba **Messages** do tópico (Produce a new message, com a mesma chave) e rode o SELECT no SQL Editor do Neon — mesma linha, valor novo.

**⌨️ Pela CLI**:

```bash
confluent kafka topic produce pedidos_sink \
  --parse-key --delimiter : \
  --value-format avro --schema pedido.avsc \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" <<'FIM'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"SHIPPED","total":289.90}
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"SHIPPED","total":289.90}
FIM
```

**Windows (PowerShell)**
```powershell
@'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"SHIPPED","total":289.90}
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"SHIPPED","total":289.90}
'@ | confluent kafka topic produce pedidos_sink --parse-key --delimiter : --value-format avro --schema pedido.avsc --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

A **mesma chave, duas vezes, de propósito**. Aguarde ~45 s e consulte:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT * FROM public.pedidos_sink ORDER BY order_id;"
```

**Output esperado**
```
 order_id | customer_id | status  | total
----------+-------------+---------+-------
 ord-1    | cli-99      | SHIPPED | 289.9
 ord-2    | cli-42      | CREATED |   150
 ord-3    | cli-99      | PAID    |  75.5
(3 rows)
```

**O que observar** — e este é o momento da aula:

**Você enviou 5 mensagens no total. A tabela tem 3 linhas.** O `ord-1` foi entregue
duas vezes e o resultado é uma linha só, com o estado correto (`SHIPPED`).

Com `insert.mode: INSERT`, a segunda entrega teria criado linha duplicada — ou
estourado violação de chave primária. Com **upsert**, a duplicata é **inofensiva**.

> 💡 **A síntese**: *at-least-once na entrega + upsert no destino ≈ efeito
> exactly-once*. É a mesma equação da Aula 5 do Curso 1 — lá você escreveu o dedup em
> Python; aqui, foi uma linha de configuração.

---

## Passo 6 — O sink é um consumer group *(~4 min)*

**🖱 Pela Console**: no cluster, **Consumers** — o grupo `connect-lcc-...` listado com o lag dele. Todo sink é um consumidor como outro qualquer.

**⌨️ Pela CLI**:

```bash
confluent kafka consumer group list | grep connect
```

**Output esperado**
```
  lkc-xxxxx | connect-lcc-xxxxx | 4 | false | roundrobin | STABLE
```

**O que observar**: o sink aparece como **consumer group comum**. Tudo o que você
aprendeu no Curso 1 vale: ele tem offsets, sofre rebalance, e acumula **lag** se o
destino não acompanhar.

**Monitorar sink = monitorar lag.** Um lag crescente significa que o Postgres não
está dando conta do ritmo do tópico — semente do Curso 7.

---

## Passo 7 — Fixe os conceitos *(~4 min)*

1. O sink gravou um lote e morreu antes de commitar. O que acontece ao religar — e por que a tabela não duplica?
2. Sua empresa quer o mesmo fluxo no Snowflake (análise) e no Elasticsearch (busca). Quantos conectores?
3. Por que `auto.create` é perigoso em produção?
4. Um sink está RUNNING, com task ativa, e não escreve nada. Quais as três primeiras coisas a verificar?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. O lote é relido e reescrito** — at-least-once. A tabela não duplica porque o
`insert.mode=UPSERT` transforma a reescrita em `INSERT ... ON CONFLICT (order_id) DO
UPDATE`: N entregas, 1 efeito. Foi o Passo 5.

**2. Dois conectores**, um para cada destino, e **independentes entre si**. Cada um é
um consumer group próprio, com offset próprio — se o Elasticsearch cair, o Snowflake
continua. É o argumento econômico do streaming: capturou uma vez, entrega para todos
(Curso 1, Aula 2).

**3. Porque o schema do tópico passa a ditar DDL no destino.** Um campo adicionado por
outro time altera a tabela do seu banco sem revisão. Em aula é conveniência; em
produção exige governança: quem pode evoluir o schema, e com qual compatibilidade.

**4. Nesta ordem** — as três armadilhas que fazem falhar em silêncio:
- **Nome do tópico tem ponto?** `lab.pedidos` quebra o mapeamento da tabela
- **RBAC do Schema Registry?** Teste com `curl` no subject; 403 é o sintoma
- **`pk.mode=record_key` sem `pk.fields`?** A task fica vazia apesar do status RUNNING

</details>

---

## Se algo não funcionou

**`Error: Forbidden Access` no `api-key create`**
`$SA` está vazio (variável não capturada — no zsh, vazia ela **some** do comando) ou a
service account acabou de ser criada (propagação de ~10 s). Rode `echo "$CLUSTER $SA"`:
os dois devem aparecer. Vazio → refaça a captura do P.1.

**Fechei o terminal / perdi as variáveis**
Rode a **Preparação** de novo (autocaptura; keys novas — secrets não se recuperam).


**RUNNING, task ativa, mas nada é escrito**
As três armadilhas do gabarito, nesta ordem. Comece testando o subject:
```bash
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects/SEU-SUBJECT/versions" -w "\nHTTP %{http_code}\n"
```

**`Connector failed to connect with Schema Registry`**
Faltam configs de SR. São quatro (e mais três se usar `input.key.format: AVRO`).
Veja [`confluent_cloud/connect/`](../../../confluent_cloud/connect/README.md).

**`Value "connect" is not a valid "Timestamp Precision Mode"`**
Use `microseconds`.

**`"input.key.format" is required when "pk.mode==record_key"`**
Adicione `input.key.format` (`STRING` ou `AVRO`, conforme a key do tópico).

**Quero recomeçar**
```bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "DROP TABLE public.pedidos_sink;"
```

---

## Limpar

```bash
confluent connect cluster delete $CONN --force
confluent connect cluster list
```

- `confluent connect cluster list` — None found.

> 💸 O `PostgresSink` custa **$0,1042/task-hora** — cerca de **$75/mês** se ficar
> ligado. Deletar é o normal; recriar custa um comando.

A tabela no Neon pode ficar como evidência do que você construiu.

---

## Glossário

| Termo | Definição |
|---|---|
| **Sink connector** | Conector que consome tópicos e escreve em sistemas externos. |
| **Upsert** | Escrita idempotente por chave: insere se não existe, atualiza se existe. |
| **`pk.mode` / `pk.fields`** | De onde vem a chave do upsert e qual campo a nomeia. |
| **`auto.create` / `auto.evolve`** | Criação/evolução da tabela destino a partir do schema. |
| **Consumer group interno** | O grupo que o sink usa por baixo — offsets e lag valem normalmente. |
| **RBAC do Schema Registry** | Autorização própria do SR, separada das ACLs do Kafka. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Mensagem que o sink não consegue processar | [Aula 4](../aula4/README.md) — Dead Letter Topic |
| Achatar o envelope CDC antes do sink | [Aula 4](../aula4/README.md) — SMT |
| O caminho analítico sem sink | Curso 5 — Tableflow |
| Lag do sink como alerta | Curso 7 |

**Fontes**

- Confluent — [Postgres Sink (JDBC)](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html)
- Confluent — [JDBC Sink deep dive](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- Confluent — [Schema Registry RBAC](https://docs.confluent.io/cloud/current/stream-governance/schema-linking.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 2 · CDC com Debezium](../aula2/README.md) | [Curso 3](../README.md) | [Aula 4 · SMT e DLQ](../aula4/README.md) |
