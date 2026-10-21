# Configs de conectores validadas

Configurações **testadas na prática** contra Confluent Cloud + Neon Postgres, com as
armadilhas que só aparecem executando.

> Fonte da verdade para qualquer plugin:
> ```bash
> confluent connect plugin describe <PluginName>
> ```
> A coluna `Required` responde o que é obrigatório **hoje**, na sua conta. A
> documentação envelhece; este comando, não.

---

## ⚠️ A armadilha nº 1: ponto no nome do tópico

**Um tópico chamado `lab.pedidos` faz o sink JDBC falhar silenciosamente.**

O conector fica `RUNNING`, a task fica `RUNNING`, o consumer group fica `STABLE` — e
**nada é escrito**. Sem erro, sem trace, sem log.

A causa: o sink deriva o nome da tabela do nome do tópico, e `lab.pedidos` vira
`public."lab.pedidos"` — que o Postgres interpreta de forma ambígua (schema.tabela).

| Tópico | Resultado |
|---|---|
| `lab.pedidos` | RUNNING, **nada escrito** |
| `pedidos_sink` | ✅ tabela criada, dados gravados |

Isso foi comprovado trocando **apenas** o nome do tópico, com todo o resto idêntico.

**Solução**: use `_` em vez de `.` nos tópicos que alimentam um sink JDBC, ou defina
`table.name.format` explicitamente.

> 💡 Isso conflita com a convenção de nomes do bootcamp
> (`dev.payments.pg-payments.transactions.cdc.v1`), que usa pontos. Para tópicos
> destinados a sink JDBC, use a SMT `RegexRouter` para reescrever o nome, ou defina
> `table.name.format`.

---

## A armadilha nº 2: RBAC do Schema Registry

Service account com API key válida **não é suficiente**. O Schema Registry tem
autorização própria (RBAC), separada das ACLs do Kafka.

Sintoma: a key autentica (HTTP 200 em `/subjects`) mas retorna lista vazia, e o acesso
a um subject específico dá:

```
HTTP 403 {"error_code":40301,"message":"User is denied operation Read on Subject: xxx"}
```

O conector fica RUNNING e não processa — porque não consegue desserializar.

```bash
confluent iam rbac role-binding create \
  --principal User:sa-xxxxx \
  --role ResourceOwner \
  --resource 'Subject:pedidos' --prefix \
  --environment env-xxxxx \
  --schema-registry-cluster lsrc-xxxxx
```

> ⚠️ Aspas em `'Subject:pedidos'` — sem elas o shell expande. E `--prefix` é o que
> faz valer para todos os subjects que começam com aquele texto; sem ele, o nome é
> tratado como literal.

**Como diagnosticar rápido** (antes de culpar o conector):

```bash
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects/SEU-SUBJECT/versions" \
  -w "\nHTTP %{http_code}\n"
```

---

## As três armadilhas de configuração do sink JDBC

Descobertas depurando um `PostgresSink` que ficava `FAILED` com erro genérico
(*"unexpected error. Contact our Support team"*). Nenhuma delas está óbvia na
documentação.

### 1. O sink precisa de credenciais do Schema Registry

O erro real só aparece no `Trace`:

```
input.data.format: Connector failed to connect with Schema Registry
```

Enganoso, porque parece problema de formato ou de banco. São **quatro** configs, e
faltar uma derruba o conector:

```json
{
  "schema.registry.url": "https://psrc-xxxxx.us-east-1.aws.confluent.cloud",
  "schema.registry.basic.auth.credentials.source": "USER_INFO",
  "schema.registry.basic.auth.user.info": "SR_KEY:SR_SECRET",
  "value.converter.schema.registry.url": "https://psrc-xxxxx.us-east-1.aws.confluent.cloud"
}
```

Com `input.key.format: AVRO`, acrescente também as do **key converter**:

```json
{
  "key.converter.schema.registry.url": "https://psrc-xxxxx...",
  "key.converter.schema.registry.basic.auth.credentials.source": "USER_INFO",
  "key.converter.schema.registry.basic.auth.user.info": "SR_KEY:SR_SECRET"
}
```

### 2. `pk.mode=record_key` exige `pk.fields`

Quando a key é um **record Avro** (o caso do CDC, cuja key é `{"transaction_id": "..."}`),
não basta dizer `record_key` — é preciso nomear o campo:

```json
{
  "pk.mode": "record_key",
  "pk.fields": "transaction_id",
  "input.key.format": "AVRO"
}
```

Sem `pk.fields`, o conector sobe como RUNNING mas **a lista de tasks fica vazia** e
nada é escrito. Sintoma traiçoeiro: status verde, resultado zero.

> 💡 **Como diagnosticar**: `confluent connect cluster describe <lcc-id>` e procure a
> seção `Task Level Details`. Se ela estiver vazia apesar do status RUNNING, o
> conector não está processando.

### 3. `timestamp.precision.mode` não aceita `connect`

O validador rejeita:
```
Value "connect" is not a valid "Timestamp Precision Mode" type
```

Use `microseconds`.

---

## Sink JDBC a partir de um tópico CDC

O `PostgresSink` **não consome o envelope Debezium** (`before`/`after`/`op`
aninhados) — ele espera colunas planas. Duas saídas:

**a) Achatar na origem** (o que estes exemplos usam):
```json
{ "after.state.only": "true" }
```
no conector **CDC**. O tópico sai plano, com um marcador `__deleted`:
```json
{"__deleted":"false","account_id":"f343d9eb...","amount":"2013/100","channel":"CREDIT"}
```

**b) Achatar no caminho**, com a SMT `ExtractNewRecordState` — mais flexível, e o
assunto da Aula 4.

**O trade-off**: achatar na origem simplifica o consumo, mas **descarta o `before`**.
Se alguém precisar da transição (auditoria, lógica de compensação), o dado não existe
mais. Por isso o desenho recomendado mantém o tópico CDC cru **e** deriva um plano.

---

## Exemplos

| Arquivo | O que faz |
|---|---|
| [`datagen-source.json`](datagen-source.json) | Dados sintéticos, sem dependência externa |
| [`postgres-cdc-source.json`](postgres-cdc-source.json) | CDC log-based com envelope completo |
| [`postgres-cdc-flat.json`](postgres-cdc-flat.json) | CDC achatado, pronto para sink |
| [`postgres-sink.json`](postgres-sink.json) | JDBC sink com upsert |

Todos usam placeholders (`sa-xxxxx`, `SUA-SENHA`). **Nunca comite credenciais.**

---

## Teardown — a parte que quase todo mundo esquece

```bash
# 1. deletar o conector
confluent connect cluster delete <lcc-id> --force
confluent connect cluster list          # None found.
```

Para conectores **CDC**, há uma segunda etapa:

```bash
# 2. o replication slot fica para trás
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT slot_name, active FROM pg_replication_slots;"

# aguarde o slot ficar inactive (leva alguns minutos), então:
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT pg_drop_replication_slot('bootcamp_cdc_slot');"
```

Se você tentar antes da hora:
```
ERROR: replication slot "bootcamp_cdc_slot" is active for PID 1114
```

**Por que importa**: slot órfão faz o Postgres reter WAL indefinidamente. E no Neon
free tier, um slot ativo impede o compute de hibernar — queimando CU-hours (100/mês).

---

## Preços reais (medidos, não estimados)

Extraídos de `confluent billing cost list` após rodar os conectores:

| Conector | Preço | Se ficar 24/7 |
|---|---|---|
| `DatagenSource` | $0,0347 /task-hora | ~$25/mês |
| `PostgresSink` | $0,1042 /task-hora | ~$75/mês |
| **`PostgresCdcSourceV2`** | **$0,50 /task-hora** | **~$360/mês** |
| Rede (in/out) | $0,05 /GB | — |

**O CDC é 14× mais caro que o Datagen.** É o item que mais justifica a regra de
teardown — e o número que torna a regra concreta em vez de conselho.

```bash
confluent billing cost list \
  --start-date $(date -v-1d +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

---

**Validado em**: agosto/2026 · CLI `confluent` v4.35.0 · Neon PostgreSQL 18.4
