# Curso 2 · Aula 2 — Primeiros passos: clusters, service accounts e API keys

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~35 min · ☁️ **Usa recursos na nuvem** · 1 terminal

Nesta aula você cria a **identidade de uma aplicação** e dá a ela uma credencial com escopo limitado — o padrão que separa "funciona" de "funciona com segurança". Ao final, uma aplicação vai produzir no seu cluster **sem nenhum login humano no caminho**.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 0](../aula0/README.md) | Environment, cluster e CLI autenticada |
| 1 terminal | Com o contexto já apontado |

Confira o tabuleiro antes de seguir:

```bash
confluent environment list
confluent kafka cluster list
```

O `bootcamp-dio` deve ter o `*` na primeira lista; o `bootcamp-basic`, o `*` e
status `UP` na segunda.

Se algum `*` estiver faltando, volte ao Passo 4 da Aula 0.

---

## O que você vai saber fazer ao final

- Navegar a hierarquia de recursos pela CLI e reconhecer os IDs
- Criar um tópico gerenciado e explicar por que ele já nasce com RF=3
- Criar uma **service account** — a identidade da aplicação
- Criar uma **API key escopada** para essa identidade
- Conceder ACLs e produzir/consumir com credencial de aplicação
- Fazer teardown parcial: apagar o que foi teste, manter o que os próximos usam

---

## Os conceitos em 90 segundos

**A hierarquia** — cada nível tem ID próprio, e você vai lê-los o curso inteiro:

```
Organization
└── Environment          env-537w6n     (bootcamp-dio)
    ├── Kafka Cluster    lkc-223x0gy    (bootcamp-basic)
    │   └── Topic        pedidos
    └── Schema Registry  lsrc-xxxxx     (Aula 3)
```

**Identidade × credencial** — a distinção central da aula:

| | O que é | Exemplo |
|---|---|---|
| **Service account** | *Quem* é a aplicação | `sa-gn8g9xr` (`app-pedidos`) |
| **API key** | *Como* ela prova quem é | `RZ7TOYPH...` + secret |
| **ACL** | *O que* ela pode fazer | WRITE no tópico `pedidos` |

Uma identidade pode ter várias credenciais (rotação!), e uma credencial sem ACL não faz nada.

> 📖 Os tipos de cluster (Basic/Standard/Enterprise/Dedicated/Freight) e o detalhe da
> anatomia da API key estão **na aula gravada**.

---

## Passo 1 — Navegue a hierarquia *(~5 min)*

**🖱 Pela Console**: Home → **Environments** → clique no seu environment → o cluster aparece dentro dele. O *breadcrumb* no topo da tela É a hierarquia (organização → environment → cluster).

**⌨️ Pela CLI**:

```bash
confluent environment list
```
```
  Current |     ID     |     Name     | Stream Governance Package
----------+------------+--------------+---------------------------
  *       | env-537w6n | bootcamp-dio | ESSENTIALS
```

> 💡 Se você tiver outros environments, eles aparecem aqui também. O `*` marca o
> **ativo** — é nele que os próximos comandos operam quando você não passa
> `--environment`.

```bash
confluent kafka cluster describe
```

**O que observar**: sem passar nenhum ID — o contexto da Aula 0 está funcionando. E
cada nível tem seu prefixo: `env-` para environment, `lkc-` para cluster Kafka.

---

## Passo 2 — Crie um tópico gerenciado *(~5 min)*

**🖱 Pela Console**: no cluster, menu lateral **Topics** → **+ Add topic** → nome `pedidos`, partições **3** → **Create with defaults**.

**⌨️ Pela CLI**:

```bash
confluent kafka topic create pedidos --partitions 3
```
```
Created topic "pedidos".
```

```bash
confluent kafka topic describe pedidos
```

**Output esperado**
```
+--------------------+---------+
| Name               | pedidos |
| Internal           | false   |
| Replication Factor | 3       |
| Partition Count    | 3       |
+--------------------+---------+
```

**O que observar** — e este é o contraste que vale a aula inteira:

**`Replication Factor: 3`, e você não pediu.** No sandbox do Curso 1 era RF=1, porque
havia um broker só e você teria que configurar réplicas na mão em produção. Aqui as 3
cópias em zonas distintas são o **default gerenciado**.

Aquele "RF=3 e `min.insync.replicas=2`" que o Curso 1 citava como conceito? Acabou de
virar realidade sem uma linha de configuração.

> ⚠️ E ninguém subiu broker nenhum. Compare com o `docker compose up` do Curso 1 —
> e agora imagine operar 3 brokers replicados você mesmo.

---

## Passo 3 — Crie a identidade da aplicação *(~5 min)*

Antes de qualquer credencial, a **identidade**:

**🖱 Pela Console**: menu ☰ (canto superior direito) → **Accounts & access** → aba **Service accounts** → **+ Add service account** → nome `app-pedidos`.

**⌨️ Pela CLI**:

```bash
confluent iam service-account create app-pedidos --description "Aplicacao de pedidos - bootcamp DIO"
```

**Output esperado**
```
+-------------+--------------------------------+
| ID          | sa-gn8g9xr                     |
| Name        | app-pedidos                    |
| Description | Aplicacao de pedidos -         |
|             | bootcamp DIO                   |
+-------------+--------------------------------+
```

**O que observar**: a service account existe e **ainda não tem senha nenhuma**.
Identidade e credencial são coisas separadas — é isso que permite rotacionar a chave
sem trocar a identidade, e revogar uma credencial vazada sem derrubar a aplicação.

### Veja o contraste na sua própria conta

```bash
confluent api-key list
```

Se você já usava o Confluent Cloud, provavelmente verá keys cujo `Owner` começa com
**`u-`** — usuário humano. É o padrão que se forma sozinho: alguém precisou testar,
criou uma key da própria conta, e ela virou credencial de produção.

O problema: quando essa pessoa sai da empresa e a conta é desativada, **a aplicação
para**. Com `sa-`, a identidade é da aplicação, não de quem a criou.

---

## Passo 4 — Crie a API key escopada *(~6 min)*

**🖱 Pela Console**: no cluster, **API Keys** → **+ Add key** → dono: **Service account** → escolha `app-pedidos` → escopo: este cluster → **Download and continue** (a Console também só mostra o secret UMA vez).

**⌨️ Pela CLI** — primeiro, capture os IDs em variáveis. Os comandos abaixo descobrem
os **seus** `lkc-` e `sa-` sozinhos (nada para editar) e valem até o fim da aula:

**macOS / Linux**
```bash
CLUSTER=$(confluent kafka cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos'][0])")
echo "CLUSTER=$CLUSTER  SA=$SA"
```

**Windows (PowerShell)**
```powershell
$CLUSTER = (confluent kafka cluster describe -o json | ConvertFrom-Json).id
$SA = (confluent iam service-account list -o json | ConvertFrom-Json | Where-Object name -eq "app-pedidos").id
echo "CLUSTER=$CLUSTER  SA=$SA"
```

O `echo` deve mostrar os seus dois IDs. Agora a key:

```bash
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
```

**Output esperado** (a SUA key será outra)
```
+------------+------------------------------------------------------------------+
| API Key    | SUA-API-KEY                                                 |
| API Secret | cflt-SEU-SECRET-AQUI |
+------------+------------------------------------------------------------------+
```

> 🔴 **O secret aparece UMA ÚNICA VEZ.** Não há como recuperá-lo depois — se perder,
> o caminho é criar outra key e revogar esta. Copie agora.
>
> E o óbvio que precisa ser dito: **nunca** comite isso. O `.gitignore` do projeto já
> cobre `.env`, mas a disciplina é sua.

Guarde no ambiente para os próximos comandos (com a SUA key):

**macOS / Linux**
```bash
CC_API_KEY="SUA-API-KEY"
CC_API_SECRET="cflt3ROOw0w/..."
```

**Windows (PowerShell)**
```powershell
$CC_API_KEY = "SUA-API-KEY"
$CC_API_SECRET = "cflt3ROOw0w/..."
```

Confirme o escopo:

```bash
confluent api-key list
```

**O que observar**: a coluna `Owner` mostra o seu `sa-` (não `u-`), e `Resource`
mostra o seu `lkc-`. Esta key **só funciona neste cluster** — vazada, o estrago é
limitado a ele.

---

## Passo 5 — Conceda permissões *(~5 min)*

A key prova *quem* é a aplicação. Falta dizer *o que* ela pode fazer:

**🖱 Pela Console**: **Accounts & access** mostra identidades e *role bindings* (RBAC) — mas no cluster **Basic** a concessão por tópico é via **ACL**, e ACL é território da CLI. Mostre a tela; conceda pelo comando:

**⌨️ Pela CLI**:

```bash
confluent kafka acl create --allow --service-account $SA --operations WRITE,READ,DESCRIBE --topic pedidos
```

**Output esperado**
```
     Principal    | Permission | Operation | Resource Type | Resource Name | Pattern Type
------------------+------------+-----------+---------------+---------------+--------------
  User:sa-gn8g9xr | ALLOW      | DESCRIBE  | TOPIC         | pedidos       | LITERAL
  User:sa-gn8g9xr | ALLOW      | READ      | TOPIC         | pedidos       | LITERAL
  User:sa-gn8g9xr | ALLOW      | WRITE     | TOPIC         | pedidos       | LITERAL
```

Para consumir, o grupo também precisa de permissão:

```bash
confluent kafka acl create --allow --service-account $SA --operations READ --consumer-group pedidos-app
```

**O que observar**: `Resource Name: pedidos` e `Pattern Type: LITERAL` — a permissão
vale **só para este tópico**. Se a aplicação for comprometida, ela não alcança os
outros.

> 💡 **Sem ACL, a key não faz nada.** Se você pular este passo, o produce falha com um
> erro de autorização — e o mais comum é procurar o problema na key, que está correta.

---

## Passo 6 — Use a credencial da aplicação *(~6 min)*

Agora o momento em que tudo se junta:

**🖱 Pela Console**: no tópico `pedidos` → aba **Messages** → **Produce a new message** permite produzir pela UI — e a mesma aba mostra as mensagens chegando em tempo real (ótimo para a demo).

**⌨️ Pela CLI**:

Um comando só — as 3 mensagens embutidas (o `ord-1` aparece duas vezes de propósito):

**macOS / Linux**
```bash
confluent kafka topic produce pedidos \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" \
  --parse-key --delimiter : <<'FIM'
ord-1:{"order_id":"ord-1","total":289.90}
ord-2:{"order_id":"ord-2","total":150.00}
ord-1:{"order_id":"ord-1","total":289.90}
FIM
```

**Windows (PowerShell)**
```powershell
@'
ord-1:{"order_id":"ord-1","total":289.90}
ord-2:{"order_id":"ord-2","total":150.00}
ord-1:{"order_id":"ord-1","total":289.90}
'@ | confluent kafka topic produce pedidos --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" --parse-key --delimiter :
```

> 💡 O `--delimiter :` divide no **primeiro** dois-pontos — os `:` dentro do JSON do
> valor ficam intactos. Para digitar interativamente (bom ao vivo), rode sem o bloco
> de dados e encerre com **Ctrl+D**.

Consuma de volta, com grupo nomeado:

```bash
confluent kafka topic consume pedidos --from-beginning --print-key --print-offset --delimiter " | " --group pedidos-app --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

**Output esperado**
```
ord-1 | Partition:0 Offset:0	{"order_id":"ord-1","total":289.90}
ord-2 | Partition:0 Offset:1	{"order_id":"ord-2","total":150.00}
ord-1 | Partition:0 Offset:2	{"order_id":"ord-1","total":289.90}
```

Encerre com **Ctrl+C**.

**O que observar**:

1. **Nenhum login humano no caminho do dado.** A aplicação se autenticou com a própria credencial — é assim que um serviço em produção fala com o cluster.
2. As chaves e offsets são os mesmos conceitos do Curso 1 — só que agora num cluster gerenciado, replicado 3×.

> ⚠️ Se as suas mensagens caíram todas na mesma partição, é esperado: com poucas
> chaves distintas, o hash não distribui. Foi a lição da Aula 3 do Curso 1.

Confirme o grupo:

```bash
confluent kafka consumer group describe pedidos-app
```
```
| Consumer Group     | pedidos-app        |
| Partition Assignor | cooperative-sticky |
| State              | STABLE             |
```

---

## Passo 7 — Teardown parcial *(~3 min)*

Aqui você exercita o critério: **o que morre e o que fica**.

**🖱 Pela Console**: tópico → **Configuration** → **Delete topic**; service account em **Accounts & access** → ⋮ → **Delete**. Na demo, o CLI abaixo é mais rápido:

**⌨️ Pela CLI**:

```bash
confluent kafka topic delete pedidos --force
```

**O que fica** (a Aula 3 e a Aula 4 dependem):

| Recurso | Por quê |
|---|---|
| Environment + cluster | O tabuleiro do bootcamp |
| Service account `app-pedidos` | Identidade reutilizada |
| API key + ACLs | Credencial reutilizada |

**O que morre**: só o tópico `pedidos`, que era teste. A Aula 3 vai recriá-lo — dessa
vez **com contrato**.

> 💡 Note que apagar o tópico **não** apaga as ACLs que o referenciam. Elas continuam
> lá, esperando um tópico com esse nome. Faz sentido: permissão é sobre o *nome*, não
> sobre o objeto.

---

## Se algo não funcionou

**`Error: unknown flag: --timeout` no consume**
Essa flag não existe. O consumer roda até você dar **Ctrl+C**.

**Produce falha com erro de autorização**
Falta a ACL (Passo 5), ou você usou o `sa-` errado. Confira com
`confluent kafka acl list`.

**`Error: Resource Not Found` ou `unsupported resource type` no `api-key create`**
As variáveis `$CLUSTER`/`$SA` estão vazias ou com IDs que não existem na sua conta.
Rode o bloco de captura do Passo 4 e confira com `echo "$CLUSTER $SA"` antes de seguir.

**`zsh: command not found: #`**
O zsh (padrão do macOS) não aceita comentário `#` colado em comando no modo interativo
— e pior: `VAR=x # comentário` **não** define a variável. Copie os comandos sem
comentários (os blocos deste guia já vêm assim).

**`Error: unknown resource "lkc-..."`**
O ID do cluster está errado ou pertence a outro environment. Rode
`confluent kafka cluster list`.

**`error creating service account: Service name is already in use`**
Você já criou a `app-pedidos` numa passada anterior — não precisa recriar. Pegue o
`sa-` dela com `confluent iam service-account list` e siga do Passo 3 (variáveis).

**Produce falha logo após criar a ACL**
Propagação leva alguns segundos. Espere ~10s e repita.

**Perdi o secret da API key**
Não há recuperação. Crie outra (`confluent api-key create`) e revogue a antiga
(`confluent api-key delete <key>`).

**O consume não mostra nada**
Sem `--from-beginning`, só chega o que for produzido depois. E confirme que o produce
terminou com Ctrl+D.

---

## Glossário

| Termo | Definição |
|---|---|
| **Service account** (`sa-`) | Identidade de uma aplicação, independente de pessoas. |
| **API key / secret** | Credencial que prova a identidade; o secret aparece uma vez só. |
| **Escopo da key** | O recurso onde ela vale (um cluster, o Schema Registry, ou `cloud`). |
| **ACL** | Regra que autoriza uma operação sobre um recurso para um principal. |
| **`LITERAL`** | O padrão de nome da ACL vale exatamente para aquele nome. |
| **Teardown parcial** | Apagar só o que foi teste, preservando o que as próximas aulas usam. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Key de Schema Registry (escopo diferente!) | **Aula 3** — e é o erro nº 1 do curso |
| O tabuleiro completo em uso | Aula 4 |
| ACLs por prefixo e RBAC | Curso 7 — segurança |
| Service account para conectores | Curso 3 |

**Fontes**

- Confluent — [Service accounts](https://docs.confluent.io/cloud/current/access-management/identity/service-accounts.html)
- Confluent — [API keys](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html)
- Confluent — [ACLs](https://docs.confluent.io/cloud/current/security/access-control/acl.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 1 · O ecossistema Confluent](../aula1/README.md) | [Curso 2](../README.md) | [Aula 3 · Schema Registry](../aula3/README.md) |
