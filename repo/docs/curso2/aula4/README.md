# Curso 2 · Aula 4 — Mão na massa: sua primeira mensagem real

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~35 min · ☁️ **Usa recursos na nuvem** · 1 terminal + navegador

Esta aula não traz conceito novo — ela **junta tudo**. Você vai produzir um evento que passa por identidade (Aula 2), contrato (Aula 3) e chega no cluster gerenciado, e depois vai olhar esse mesmo evento pela UI, pelo consumer group e pelo Stream Lineage.

E vai ver, com os próprios olhos, o que viaja dentro de uma mensagem Avro.

---

## Preparação — reconstrua o contexto do zero *(~5 min)*

Esta aula junta as peças das Aulas 0, 2 e 3 — mas **não exige o terminal delas aberto**.
Os passos abaixo reconstroem tudo de qualquer estado: terminal novo, variáveis
perdidas, até recursos deletados. Só a Aula 0 (environment + cluster + `confluent
login`) precisa existir.

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

### P.2 — Key nova do cluster

Fechou o terminal da Aula 2? O secret **não se recupera** — o caminho certo é criar
outra key (e é uma boa demonstração de rotação):

```bash
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
```

Guarde o par que apareceu (o secret, só agora):

**macOS / Linux**
```bash
CC_API_KEY="COLE_A_KEY_AQUI"
CC_API_SECRET="COLE_O_SECRET_AQUI"
```

**Windows (PowerShell)**
```powershell
$CC_API_KEY = "COLE_A_KEY_AQUI"
$CC_API_SECRET = "COLE_O_SECRET_AQUI"
```

> 💡 Keys antigas da mesma SA que você não usa mais: `confluent api-key list` mostra;
> `confluent api-key delete <key> --force` revoga. Rotacionar é isto.

### P.3 — ACLs e tópico (idempotentes — rodar de novo não faz mal)

```bash
confluent kafka acl create --allow --service-account $SA --operations WRITE,READ,DESCRIBE --topic pedidos
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group pedidos- --prefix
confluent kafka topic create pedidos --partitions 3
```

Se o tópico já existir, o erro `Topic 'pedidos' already exists` é inofensivo — siga.
A ACL de grupo usa **prefixo** (`pedidos-` + `PREFIXED`): ela cobre o `pedidos-app` do
Passo 2 **e** o `pedidos-app-avro` do Passo 3 com uma regra só — o padrão de domínio
que o Curso 7 aprofunda.
O contrato Avro **não precisa de passo próprio**: o produce do Passo 1 registra o
schema sozinho se o subject não existir.

Aguarde ~15 s (propagação da key nova e das ACLs) e a aula é sua.

---

## O que você vai saber fazer ao final

- Produzir uma mensagem Avro validada contra o Schema Registry
- Ver os bytes crus e identificar o magic byte e o schema ID
- Consumir com deserialização e comparar com o dado bruto
- Inspecionar mensagem, consumer group e lineage pela UI
- Explicar por que a UI é "só mais um consumidor"

---

## O caminho completo

```
  producer                                    consumer
     │  key + value Avro                         ▲
     │                                           │
     ├──► [serializer] ──┐                ┌── [deserializer]
     │    consulta SR    │                │    consulta SR
     │                   ▼                │
     │            ┌─────────────┐         │
     └───────────►│   TÓPICO    │─────────┘
       key da SA  │  pedidos    │  key da SA
                  │  (RF=3)     │
                  └─────────────┘
                         │
                   Schema Registry
                   pedidos-value v1, v2
```

Cada peça desse desenho foi uma aula. Agora elas rodam juntas.

---

## Passo 1 — Produza com contrato *(~7 min)*

Crie o arquivo do schema (o mesmo v2 da Aula 3):

**🖱 Pela Console**: deixe **Topics → `pedidos` → Messages** aberto numa aba — as mensagens Avro produzidas pela CLI abaixo vão aparecer lá, já decodificadas.

**⌨️ Pela CLI**:

```bash
cat > pedido-v2.avsc <<'EOF'
{
  "type": "record",
  "name": "PedidoCriado",
  "namespace": "com.bootcamp.pedidos",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "total", "type": "double"},
    {"name": "canal", "type": ["null","string"], "default": null}
  ]
}
EOF
```

Produza — um comando só, com as duas mensagens (repare na sintaxe do union:
`{"string":"APP"}` para preencher, `null` para vazio):

**macOS / Linux**
```bash
confluent kafka topic produce pedidos \
  --value-format avro --schema pedido-v2.avsc \
  --parse-key --delimiter : \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" <<'FIM'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","total":289.90,"canal":{"string":"APP"}}
ord-2:{"order_id":"ord-2","customer_id":"cli-42","total":150.00,"canal":null}
FIM
```

**Windows (PowerShell)**
```powershell
@'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","total":289.90,"canal":{"string":"APP"}}
ord-2:{"order_id":"ord-2","customer_id":"cli-42","total":150.00,"canal":null}
'@ | confluent kafka topic produce pedidos --value-format avro --schema pedido-v2.avsc --parse-key --delimiter : --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

**Output esperado** (o número do ID varia por conta)
```
Successfully registered schema with ID "100002".
Starting Kafka Producer. Use Ctrl-C or Ctrl-D to exit.
```

**O que observar**: o CLI conferiu o schema no Registry antes de produzir. Se o subject
`pedidos-value` já tinha este schema (Aula 3), o ID foi **reaproveitado** — nenhuma
versão nova, porque o contrato é idêntico. Se o subject não existia, ele acabou de ser
**criado** por este produce. Nos dois casos: nenhum byte entra sem contrato.

> ⚠️ **Teste a rejeição** — produza um pedido sem `customer_id` e `total`:
>
> ```bash
> echo 'ord-3:{"order_id":"ord-3"}' | confluent kafka topic produce pedidos --value-format avro --schema pedido-v2.avsc --parse-key --delimiter : --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
> ```
>
> O erro acontece **no producer**, antes de qualquer byte chegar ao tópico. É o
> contrato barrando na origem — o consumidor de outro time nunca vê o dado quebrado.

---

## Passo 2 — Veja os bytes crus *(~7 min)*

Esta é a cena mais reveladora da aula. Consuma **sem** o deserializer Avro:

**🖱 Pela Console**: repare o contraste — a aba **Messages** mostra o registro *bonito* (decodificado). Os bytes crus de verdade, só a CLI mostra:

**⌨️ Pela CLI**:

```bash
confluent kafka topic consume pedidos --from-beginning \
  --value-format string \
  --group pedidos-app \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" | cat -v
```

> O `cat -v` torna visíveis os bytes não-imprimíveis. Sem ele, o terminal engole o
> binário e você não vê nada.

**Output esperado**
```
^@^@^AM-^F...
ord-1^Lcli-99fffff^^r@^B^FAPP

^@^@^AM-^F...
ord-2^Lcli-42^@^@^@^@^@\M-2b@^@
```

Encerre com **Ctrl+C**.

**O que observar** — decodificando o começo de cada mensagem:

| Bytes | Significado |
|---|---|
| `^@` | O **magic byte** `0x00` — marca "isto é wire format do Schema Registry" |
| `^@^AM-^F...` | Os **4 bytes do schema ID** (100002 em binário) |
| `ord-1^Lcli-99...` | O **payload Avro** — strings aparecem legíveis, números não |

É por isso que o consumidor sabe qual contrato usar: **o ID viaja dentro da mensagem**.
Ele não precisa adivinhar nem ser configurado.

Repare também que `total` (289.90) virou `fffff^^r@` — um `double` binário. Avro não é
JSON compactado: é **formato binário com schema**, e é isso que o torna eficiente.

---

## Passo 3 — Agora com o deserializer *(~5 min)*

O mesmo tópico, a mesma mensagem — mudando o `--value-format` e usando um **grupo
novo** (`pedidos-app-avro`):

```bash
confluent kafka topic consume pedidos --from-beginning \
  --value-format avro \
  --print-key --print-offset --delimiter ' | ' \
  --group pedidos-app-avro \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

> ⚠️ **Por que um grupo novo?** O Passo 2 commitou offsets no `pedidos-app` — e para um
> grupo com offset commitado, o `--from-beginning` é **ignorado** (ele só vale para
> grupo sem posição — Curso 1, Aula 4). Reusar o grupo aqui mostraria uma tela vazia.
> O grupo novo entra pela ACL de **prefixo** da Preparação.

**Output esperado**
```
ord-1 | Partition:0 Offset:0	{"canal":{"string":"APP"},"customer_id":"cli-99","order_id":"ord-1","total":289.9}
ord-2 | Partition:0 Offset:1	{"canal":null,"customer_id":"cli-42","order_id":"ord-2","total":150}
```

**O que observar**: o mesmo byte-a-byte do Passo 2, agora legível. O deserializer leu o
schema ID, buscou o contrato no Registry e reconstruiu o objeto.

**Sem contrato, aquilo era lixo binário. Com contrato, é um pedido.**

> 💡 A key (`ord-1`) continua sendo string simples — ela não tem schema neste tópico.
> O subject `pedidos-key` existiria se você desse contrato à chave também.

---

## Passo 4 — Olhe pela UI *(~6 min)*

Abra [confluent.cloud](https://confluent.cloud) → seu environment → cluster → **Topics
→ pedidos → Messages**.

**O que observar**:

1. As mesmas mensagens, com key, value deserializado, partição, offset e timestamp
2. O value aparece **legível** — a UI consultou o Schema Registry, igual ao Passo 3
3. É a anatomia do record do Curso 1 (Aula 3), agora renderizada pela plataforma

> 💡 **A UI é só mais um consumidor.** Ela não tem acesso privilegiado: usa o mesmo
> protocolo, o mesmo Registry e as mesmas permissões que o seu terminal. Se o value
> aparecesse como binário aqui, seria sinal de que o contrato não está registrado.

Vá em **Schema Registry → pedidos-value** e veja as duas versões da Aula 3, com o
diff entre elas.

---

## Passo 5 — O consumer group e o lineage *(~6 min)*

```bash
confluent kafka consumer group describe pedidos-app
```

**Output esperado**
```
| Cluster            | lkc-223x0gy        |
| Consumer Group     | pedidos-app        |
| Partition Assignor | cooperative-sticky |
| State              | STABLE             |
```

**O que observar**: `cooperative-sticky` — a estratégia de rebalance que você viu na
Aula 4 do Curso 1, aqui como default gerenciado.

Na UI, vá em **Consumers → pedidos-app** e veja o **lag por partição** — o mesmo número
que você inspecionava com `kafka-consumer-groups --describe` no Curso 1.

### Stream Lineage

Ainda na UI: **Stream Lineage** (menu lateral) e clique no tópico `pedidos`.

**O que observar**: o diagrama mostra produtores → tópico → consumidores. E o ponto
importante: **ninguém desenhou isso**. O mapa foi gerado a partir do tráfego real que
você acabou de gerar.

Em uma empresa com dezenas de tópicos, esse é o mapa que responde "quem depende deste
dado?" antes de alguém mudar um schema.

---

## Passo 6 — Fixe os conceitos *(~4 min)*

1. Por que o consumidor não precisa ser configurado com o schema?
2. No Passo 2, por que `ord-1` apareceu legível e `289.90` não?
3. A UI mostrou o value legível. Que serviço ela consultou para isso?
4. Você produziu com a key do cluster. Por que o comando não pediu a key do Schema Registry?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque o schema ID viaja na mensagem.** O deserializer lê os 4 bytes após o magic
byte, busca o schema correspondente no Registry (com cache local) e reconstrói o
objeto. É isso que permite ao mesmo consumidor ler mensagens escritas com v1 e v2 sem
reconfiguração.

**2. Porque Avro codifica strings como texto UTF-8 precedido do tamanho**, enquanto
`double` é codificado em 8 bytes binários (IEEE 754). O `fffff^^r@` é a representação
binária de 289.90 — ilegível para humanos, compacta para máquinas.

**3. O Schema Registry** — exatamente como o seu terminal fez no Passo 3. A UI não tem
canal privilegiado; ela é um cliente como outro qualquer.

**4. Porque o CLI usou a sua sessão de usuário** (`confluent login`) para falar com o
Registry. Numa aplicação real, você passaria as duas credenciais explicitamente: a do
cluster para o Kafka e a do Registry para o schema. É por isso que a Aula 3 criou
as duas — e por isso confundi-las é o erro nº 1 do curso.

</details>

---

## Se algo não funcionou

**`Error: Forbidden Access` no `api-key create`**
`$SA` está vazio (variável não capturada — no zsh, vazia ela **some** do comando) ou a
service account acabou de ser criada (propagação de ~10 s). Rode `echo "$CLUSTER $SA"`:
os dois devem aparecer. Vazio → refaça a captura do P.1.

**Fechei o terminal / perdi as variáveis**
Rode a **Preparação** de novo — ela reconstrói IDs, key (nova) e ACLs de qualquer
estado, em ~5 min.

**`Group authorization failed`**
Sua ACL cobre só o grupo `pedidos-app`. Para usar outro nome de grupo, crie a ACL:
```bash
confluent kafka acl create --allow --service-account $SA --operations READ --consumer-group NOME-DO-GRUPO
```
Esse erro é uma boa notícia: prova que as ACLs da Aula 2 estão funcionando.

**O consume não mostra nada**
O grupo já commitou offsets — e com offset commitado o `--from-beginning` é ignorado.
Use um **grupo novo** começando com `pedidos-` (a ACL de prefixo da Preparação já o
cobre): `--group pedidos-app-take2`, por exemplo.

**O produce falha com erro de schema**
O JSON não bate com o contrato. Campos `union` exigem a sintaxe
`{"string":"valor"}` ou `null` — é a pegadinha mais comum do Avro.

**Vejo binário na UI em vez do objeto**
O tópico tem dados que não foram escritos no wire format do Registry (ex.: as mensagens
JSON da Aula 2, se você não recriou o tópico). Isso é esperado — a UI só desserializa
o que tem contrato.

---

## O que fica de pé

**Mantenha tudo.** Este tabuleiro é a base dos Cursos 3 a 7:

| Recurso | Usado em |
|---|---|
| Environment + cluster Basic | Todos os cursos |
| Service account + 2 API keys | Todos |
| Tópico `pedidos` + subject com 2 versões | Curso 3 (Connect) |
| Consumer group `pedidos-app` | — |

Antes de encerrar a sessão, o ritual:

```bash
confluent connect cluster list
confluent flink compute-pool list
```

- `confluent connect cluster list` — deve ser "None found."
- `confluent flink compute-pool list` — deve ser "None found."

Se essas listas estiverem vazias, pode fechar o terminal tranquilo: **cluster Basic
ocioso não queima crédito relevante**.

---

## Glossário

| Termo | Definição |
|---|---|
| **Wire format** | `magic byte (0x00) + schema ID (4 bytes) + payload Avro`. |
| **Serializer / deserializer** | Componentes que traduzem objeto ↔ bytes consultando o Registry. |
| **Stream Lineage** | Mapa do fluxo gerado automaticamente a partir do tráfego real. |
| **`cooperative-sticky`** | Estratégia de rebalance incremental; default no Confluent Cloud. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Produzir sem escrever producer | **Curso 3** — Kafka Connect |
| O schema registrado automaticamente por um conector | Curso 3, Aula 2 |
| Transformar o fluxo | Curso 4 — Flink |
| Lineage como ferramenta de operação | Curso 7 |

**Fontes**

- Confluent — [Produce and consume](https://docs.confluent.io/cloud/current/client-apps/index.html)
- Confluent — [Stream Lineage](https://docs.confluent.io/cloud/current/stream-governance/stream-lineage.html)
- Confluent — [Wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 3 · Schema Registry](../aula3/README.md) | [Curso 2](../README.md) | [Aula 5 · Cluster Linking](../aula5/README.md) |
