# Curso 3 · Aula 1 — Kafka Connect: integração por configuração

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~30 min · ☁️ **Cria um conector (cobra por hora ligada)** · 1 terminal

No Curso 2 você escreveu producers. Nesta aula você move dados **sem escrever uma linha de código** — e depois deleta o conector, porque essa é metade da lição.

> 💡 **Esta aula não precisa do Neon.** Se você ainda não fez o [Setup](../setup/README.md),
> pode fazer esta aula assim mesmo — o Datagen gera os próprios dados. As Aulas 2 a 4
> é que exigem o banco.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a Aula 0 do [Curso 2](../../curso2/README.md) | Environment + cluster + `confluent login` |
| 1 terminal | Qualquer um — a Preparação reconstrói o resto |

## Preparação — contexto do zero *(~4 min)*

Nada aqui depende de terminal antigo nem de editar arquivo: os comandos descobrem os
**seus** IDs sozinhos e reconstroem credencial e permissões de qualquer estado.

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

### P.2 — Key do cluster (para o consume do Passo 3)

```bash
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
```

Guarde o par:

**macOS / Linux**
```bash
CC_API_KEY="SUA-API-KEY"
CC_API_SECRET="cflt-SEU-SECRET-AQUI"
```

**Windows (PowerShell)**
```powershell
$CC_API_KEY = "COLE_A_KEY_AQUI"
$CC_API_SECRET = "COLE_O_SECRET_AQUI"
```

### P.3 — Permissões desta aula (idempotentes)

O conector vai se autenticar **como a service account** — ela precisa escrever no
tópico. E o consume precisa de grupo. Prefixo `lab.` cobre os dois de uma vez:

```bash
confluent kafka acl create --allow --service-account $SA --operations WRITE,READ,DESCRIBE,CREATE --topic lab. --prefix
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group lab. --prefix
```

Aguarde **~30 s** (propagação das ACLs) antes de criar o conector — criar cedo demais
pode travar a primeira task (o troubleshooting resolve, mas esperar é mais elegante).

---

## 💸 A regra de custo desta aula

> **Conector ligado cobra por hora, mesmo sem tráfego.**

Diferente do cluster Basic (que cobra por uso e fica ~zero parado), o conector
gerenciado cobra **por task/hora + throughput**. Pausar **não** zera — só deletar zera.

Por isso esta aula **termina deletando o conector**. Recriar custa um formulário de
1 minuto; esquecer ligado custa crédito todo dia.

---

## O que você vai saber fazer ao final

- Explicar a anatomia do Connect: connector, task, worker, converter
- Criar um conector pela CLI a partir de um arquivo de configuração
- Verificar que ele está RUNNING e produzindo
- Mostrar que o schema foi registrado **sem ninguém escrever producer**
- Fazer o teardown e explicar o que sobrevive a ele

---

## Os conceitos em 90 segundos

```
 MUNDO EXTERNO              KAFKA CONNECT                    KAFKA
┌────────────┐      ┌──────────────────────────┐      ┌──────────────┐
│  Postgres  │      │ CONNECTOR (a config:     │      │              │
│  S3        │ ───► │  o QUE integrar, de ONDE)│ ───► │   tópicos    │
│  MongoDB   │      │   └─ TASKS (paralelismo) │      │              │
│  SaaS ...  │ ◄─── │       └─ WORKERS (infra) │ ◄─── │              │
└────────────┘      │ CONVERTERS (Avro + SR)   │      └──────────────┘
                    └──────────────────────────┘
      SOURCE: externo → Kafka        SINK: Kafka → externo
```

| Peça | O que é |
|---|---|
| **Connector** | A configuração declarativa: plugin + parâmetros |
| **Task** | A unidade de trabalho real; `tasks.max` é o teto |
| **Worker** | O processo onde a task roda — **invisível no fully managed** |
| **Converter** | A ponte de formato; `AVRO` integra com o Schema Registry |

A regra da aula:

> **Integração de dados não é lugar de código criativo. É configuração sobre
> plataforma testada.**

> 📖 A comparação fully managed × self-managed e o catálogo de 90+ conectores estão
> **na aula gravada**.

---

## Passo 1 — Explore o catálogo *(~5 min)*

**🖱 Pela Console**: no cluster, menu lateral **Connectors** — o catálogo visual inteiro, com busca. Digite `Datagen` e veja o card do plugin.

**⌨️ Pela CLI**:

```bash
confluent connect plugin list
```

São dezenas. Filtre pelos que interessam ao bootcamp:

```bash
confluent connect plugin list | grep -iE "postgres|datagen"
```

**Output esperado**
```
  DatagenSource                 | source
  PostgresCdcSourceV2           | source
  PostgresSink                  | sink
  PostgresSource                | source
```

**O que observar**: quatro plugins, três papéis diferentes.

| Plugin | Papel |
|---|---|
| `DatagenSource` | Dados sintéticos — **o desta aula** |
| `PostgresCdcSourceV2` | CDC log-based (Debezium) — [Aula 2](../aula2/README.md) |
| `PostgresSink` | Kafka → Postgres — [Aula 3](../aula3/README.md) |
| `PostgresSource` | Polling por query — a alternativa *fraca* ao CDC |

Veja o que um plugin exige:

```bash
confluent connect plugin describe DatagenSource
```

**O que observar**: a coluna `Required`. Para o Datagen são só quatro:
`connector.class`, `kafka.topic`, `output.data.format` e `tasks.max`.

> 💡 **Este comando é a fonte da verdade.** A documentação envelhece; o
> `plugin describe` responde o que o plugin aceita **hoje**, na sua conta.

---

## Passo 2 — Crie o conector *(~8 min)*

Primeiro o tópico de destino (o sandbox e o Cloud não criam sozinhos):

**🖱 Pela Console**: **Connectors** → card **Datagen Source** → o wizard pede exatamente os campos do JSON abaixo (tópico, formato de saída, quantidade de tasks, API key) → **Launch**. Na demo a UI é ótima; o JSON é o que dá reprodutibilidade.

**⌨️ Pela CLI**:

```bash
confluent kafka topic create lab.transactions.datagen --partitions 3
```

Agora a configuração — o `$SA` da Preparação entra **sozinho** no JSON (o heredoc sem
aspas interpola a variável; confira o resultado no `cat`):

**macOS / Linux**
```bash
cat > datagen.json <<EOF
{
  "name": "datagen-transactions",
  "connector.class": "DatagenSource",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",
  "kafka.topic": "lab.transactions.datagen",
  "output.data.format": "AVRO",
  "quickstart": "TRANSACTIONS",
  "max.interval": "1000",
  "tasks.max": "1"
}
EOF
cat datagen.json
```

**Windows (PowerShell)**
```powershell
@"
{
  "name": "datagen-transactions",
  "connector.class": "DatagenSource",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "$SA",
  "kafka.topic": "lab.transactions.datagen",
  "output.data.format": "AVRO",
  "quickstart": "TRANSACTIONS",
  "max.interval": "1000",
  "tasks.max": "1"
}
"@ | Set-Content datagen.json
Get-Content datagen.json
```

Cada linha é um conceito:

| Config | O que faz |
|---|---|
| `connector.class` | Qual plugin usar |
| `kafka.auth.mode: SERVICE_ACCOUNT` | O conector se autentica como **aplicação** (Curso 2) |
| `output.data.format: AVRO` | Ativa o Schema Registry — contrato desde a ingestão |
| `quickstart: TRANSACTIONS` | Modelo de dados sintéticos do Datagen |
| `max.interval: 1000` | Uma mensagem por segundo — ritmo visível |
| `tasks.max: 1` | **Teto** de paralelismo, não garantia |

Crie:

```bash
confluent connect cluster create --config-file datagen.json
```

**Output esperado**
```
+------+----------------------+
| ID   | lcc-zm3xo60          |
| Name | datagen-transactions |
+------+----------------------+
```

Anote o `lcc-` — você vai precisar dele no teardown.

---

## Passo 3 — Veja o conector trabalhar *(~6 min)*

**🖱 Pela Console**: **Connectors** → clique no conector → status **Running** e o gráfico de mensagens processadas; em **Topics → `lab.transactions.datagen` → Messages**, os registros chegando ao vivo.

**⌨️ Pela CLI**:

Capture o id do conector — o comando descobre o **seu** `lcc-` pelo nome (nada para
editar):

**macOS / Linux**
```bash
CONN=$(confluent connect cluster list -o json | python3 -c "import json,sys; print([c['id'] for c in json.load(sys.stdin) if c['name']=='datagen-transactions'][0])")
echo "CONN=$CONN"
```

**Windows (PowerShell)**
```powershell
$CONN = (confluent connect cluster list -o json | ConvertFrom-Json | Where-Object name -eq "datagen-transactions").id
echo "CONN=$CONN"
```

```bash
confluent connect cluster describe $CONN
```

Repita até o status mudar (leva ~40 s):

```
| Status | PROVISIONING |
   ↓
| Status | RUNNING      |
```

**O que observar**: você não dimensionou worker, não instalou plugin, não configurou
JVM. Escreveu 10 linhas de JSON e um processo distribuído começou a rodar.

Consuma o que ele está produzindo:

```bash
confluent kafka topic consume lab.transactions.datagen --from-beginning \
  --value-format avro --print-offset \
  --group lab.datagen.viewer \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

> O `--group lab.datagen.viewer` entra pela ACL de prefixo `lab.` da Preparação. Sem
> `--group`, o CLI inventa um nome aleatório — que nenhuma ACL cobre.

**Output esperado**
```
Partition:2 Offset:0	{"card_id":5,"purchase_id":1,"store_id":4,"transaction_id":2,"user_id":"User_"}
% Headers: [task.generation="0" task.id="0" current.iteration="1"]
Partition:2 Offset:1	{"card_id":2,"purchase_id":2,"store_id":6,"transaction_id":3,"user_id":"User_8"}
```

Encerre com **Ctrl+C**.

**O que observar**: os headers (`task.id`, `current.iteration`) — o conector carimba
a origem em cada mensagem. É rastreabilidade que você não precisou programar.

---

## Passo 4 — O contrato que ninguém escreveu *(~5 min)*

**🖱 Pela Console**: tópico → aba **Schema** — o Avro registrado automaticamente pelo conector, sem você ter escrito uma linha dele.

**⌨️ Pela CLI**:

```bash
confluent schema-registry subject list
```

**Output esperado** (entre os subjects que você já tiver)
```
             Subject
----------------------------------
  lab.transactions.datagen-value
```

```bash
confluent schema-registry schema describe \
  --subject lab.transactions.datagen-value --version 1
```
```
Schema ID: 100001
Type: AVRO
Schema:
{
    "type": "record",
    "name": "transactions",
...
```

**O que observar** — e este é o momento da aula:

**Ninguém escreveu um producer. Ninguém registrou um schema. E existe contrato no
Registry.** O `output.data.format: AVRO` fez o conector inferir o schema dos dados e
registrá-lo automaticamente.

Compare com o Curso 2, Aula 3: lá você escreveu o `.avsc` na mão e rodou
`schema-registry schema create`. Aqui, uma linha de configuração fez o mesmo.

---

## Passo 5 — O teardown é a lição *(~5 min)*

Antes de deletar, olhe o custo. Na UI: **Connectors → datagen-transactions**, e veja
o valor por hora exibido.

> ⚠️ **Pausar não é deletar.** Um conector pausado continua existindo — e continua
> cobrando pelas tasks provisionadas. Só o delete zera.

**🖱 Pela Console**: página do conector → **Settings** → **Delete connector** (a Console pede o nome digitado para confirmar — a mesma proteção do `--force` consciente).

**⌨️ Pela CLI**:

```bash
confluent connect cluster delete $CONN --force
```
```
Deleted connector "lcc-zm3xo60".
```

Confirme:

```bash
confluent connect cluster list
```
```
None found.
```

Agora a parte que surpreende:

```bash
confluent kafka topic list
confluent schema-registry subject list
```
```
  lab.transactions.datagen | false | 3 | 3
  lab.transactions.datagen-value
```

**O que observar**: o conector morreu, mas **o tópico e o schema continuam lá**. Isso
não é acidente — é a economia da plataforma:

> **Dados são baratos. Conector ligado, não.**

Você pode deletar o conector no fim de toda sessão sem perder nada. Recriar é um
`connect cluster create` de 40 segundos.

---

## Passo 6 — Fixe os conceitos *(~4 min)*

1. Sua empresa precisa levar dados do MongoDB para o Kafka e do Kafka para o Snowflake. Quantos conectores, de que tipo?
2. Você pausou o conector no fim do expediente. Isso resolve o problema de custo?
3. `tasks.max: 4` garante 4 tasks rodando?
4. Por que o schema apareceu no Registry sem você registrar nada?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Dois conectores**: um **source** (MongoDB → Kafka) e um **sink** (Kafka →
Snowflake). Antes de escolher, três perguntas: existe versão fully managed no
catálogo? Que semântica de entrega ele oferece? Como trata mensagem com erro
([Aula 4](../aula4/README.md))?

**2. Não.** Conector pausado continua existindo e continua cobrando pelas tasks. É o
equívoco mais caro do curso. **Pausar ≠ deletar** — só o delete zera a conta.

**3. Não — é um teto.** O paralelismo real depende do plugin e da fonte. Um Datagen
simples roda com 1 task; um CDC de várias tabelas consegue dividir melhor. Pedir 4
não cria 4.

**4. Por causa do `output.data.format: AVRO`.** O converter Avro exige contrato, então
o conector inferiu o schema dos dados que produz e o registrou no subject
`{tópico}-value`. Com `JSON` (sem SR), não haveria contrato — e o preço disso aparece
lá na frente, quando alguém muda um campo e o consumidor quebra em produção.

</details>

---

## Se algo não funcionou

**`Group authorization failed` ao consumir**
O grupo usado não está coberto por ACL. Use `--group lab.<qualquer-nome>` (a ACL de
prefixo da Preparação cobre) — ou confira as ACLs com
`confluent kafka acl list --service-account $SA`. Esse erro engana: a mensagem fala de
autorização, e muita gente vai conferir a API key — que está correta.

**`Error: Forbidden Access` no `api-key create`**
`$SA` está vazio (variável não capturada — no zsh, vazia ela **some** do comando) ou a
service account acabou de ser criada (propagação de ~10 s). Rode `echo "$CLUSTER $SA"`:
os dois devem aparecer. Vazio → refaça a captura do P.1.

**Fechei o terminal / perdi as variáveis**
Rode a **Preparação** de novo (IDs por autocaptura; key nova — secret não se recupera).

**O conector fica em `PROVISIONING` por muito tempo**
Normal até ~1 min. Se passar disso:
```bash
confluent connect cluster describe $CONN
```
e procure o campo `Trace` com o erro.

**`FAILED` logo após criar**
Quase sempre é `kafka.service.account.id` errado ou sem ACL de escrita no tópico.

**`FAILED` com task em `SYSTEM_ERROR` e trace genérico ("unexpected error... contact Support")**
Task travada num erro transitório — o clássico é a primeira tentativa rodar **antes de
as ACLs propagarem** e a task não se recuperar sozinha. Pista de que as permissões
estão certas: o subject `lab.transactions.datagen-value` aparece no
`schema-registry subject list`. Destrave sem recriar:
```bash
confluent connect cluster pause $CONN
confluent connect cluster resume $CONN
```
Em ~30 s o status volta a `RUNNING`. Se persistir, delete e recrie (40 s).

**O tópico não recebe nada, mas o status é RUNNING**
Confirme o nome em `kafka.topic` — se o tópico não existir, o conector pode ficar
RUNNING sem entregar.

---

## Limpar

```bash
confluent connect cluster delete $CONN --force
confluent kafka topic delete lab.transactions.datagen --force
```

E o ritual de fim de sessão:

```bash
confluent connect cluster list
```

---

## Glossário

| Termo | Definição |
|---|---|
| **Kafka Connect** | Framework de integração por configuração, com escala e tolerância a falha embutidas. |
| **Source / Sink** | Conector que traz para o Kafka / que leva do Kafka. |
| **Task** | Unidade de trabalho paralela; `tasks.max` é o teto. |
| **Worker** | Processo que executa tasks — invisível no fully managed. |
| **Converter** | Componente de formato (Avro/JSON/Protobuf) integrado ao Schema Registry. |
| **Datagen** | Conector de dados sintéticos, para desenvolvimento e ensino. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Um conector lendo banco de verdade | [Aula 2](../aula2/README.md) — CDC |
| Levar do Kafka para um destino | [Aula 3](../aula3/README.md) — sink |
| O que fazer com mensagem quebrada | [Aula 4](../aula4/README.md) — DLQ |
| Conector como item de custo | Curso 7 — otimização |

**Fontes**

- Confluent — [Fully managed connectors](https://docs.confluent.io/cloud/current/connectors/index.html)
- Confluent — [Datagen Source](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html)
- Apache Kafka — [Kafka Connect](https://kafka.apache.org/documentation/#connect)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Setup · Neon](../setup/README.md) | [Curso 3](../README.md) | [Aula 2 · CDC com Debezium](../aula2/README.md) |
