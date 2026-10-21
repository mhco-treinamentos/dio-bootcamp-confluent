# Curso 2 · Aula 3 — Schema Registry e Data Contracts

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~40 min · ☁️ **Usa recursos na nuvem** · 1 terminal

Até aqui, o que você produziu era texto solto: o broker aceitava qualquer coisa. Nesta aula o tópico ganha **contrato** — e você vai ver o Schema Registry **rejeitar** uma mudança incompatível na sua cara. Esse é o momento da aula.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 2](../aula2/README.md) | Service account, API key e ACLs |
| 1 terminal | Com o contexto apontado |

```bash
confluent kafka cluster list
confluent iam service-account list
```

O `bootcamp-basic` deve estar com `*` e `UP`; a `app-pedidos` deve existir na lista.

> 💰 **Economizando créditos**: schema, compatibilidade e wire format funcionam
> idênticos num Schema Registry local (o `docker compose` do Curso 1 já sobe um em
> `localhost:8081`). O que **só existe no Cloud** é a UI de subjects e as Data Quality
> Rules. Se o crédito estiver curto, pratique os Passos 3–5 no local.

---

## O que você vai saber fazer ao final

- Explicar por que uma API key de cluster **não** serve para o Schema Registry
- Registrar um schema Avro e entender o que é um *subject*
- Evoluir um schema de forma compatível — e ver o Registry aceitar
- Tentar uma evolução incompatível — e ver o Registry **rejeitar**, com o motivo
- Enunciar a regra do BACKWARD sem decorar

---

## Os conceitos em 90 segundos

**O subject** é onde as versões de um schema vivem. A convenção padrão liga o subject
ao tópico:

```
tópico  pedidos
   └── subject  pedidos-value     ← o contrato do VALUE
   └── subject  pedidos-key       ← (se a key também tiver schema)
        ├── version 1 → schema ID 100001
        └── version 2 → schema ID 100002
```

**O wire format** — o que viaja de verdade em cada mensagem:

```
┌──────┬────────────────┬──────────────────────┐
│ 0x00 │  schema ID (4) │   payload Avro       │
└──────┴────────────────┴──────────────────────┘
  magic     qual contrato      os dados
```

Por isso o consumidor sabe qual schema usar: **o ID viaja junto**.

> 📖 A anatomia completa do Avro, os tipos (Protobuf/JSON Schema) e os modos de
> compatibilidade estão **na aula gravada**.

---

## Passo 1 — A segunda API key *(~6 min)*

Este passo existe por causa do **erro nº 1 do curso**.

O Schema Registry é um **recurso separado** do cluster Kafka — tem ID próprio (`lsrc-`)
e exige **credencial própria**. Usar a key do cluster nele (ou vice-versa) gera um erro
de autenticação que manda você procurar no lugar errado.

Descubra o ID do seu Schema Registry:

**🖱 Pela Console**: página do environment → painel **Stream Governance** (lado direito) → endpoint do Schema Registry e **Add key** para a credencial.

**⌨️ Pela CLI**:

```bash
confluent schema-registry cluster describe
```

**Output esperado**
```
| Cluster      | lsrc-0xjpwz6                                       |
| Endpoint URL | https://psrc-xxxxx.us-east-1.aws.confluent.cloud   |
```

**O que observar**: prefixo **`lsrc-`**, não `lkc-`. São recursos diferentes.

Crie a key apontando para ele. Primeiro, capture os IDs — os comandos descobrem os
**seus** `lsrc-` e `sa-` sozinhos (nada para editar):

**macOS / Linux**
```bash
SR=$(confluent schema-registry cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['cluster'])")
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos'][0])")
echo "SR=$SR  SA=$SA"
```

**Windows (PowerShell)**
```powershell
$SR = (confluent schema-registry cluster describe -o json | ConvertFrom-Json).cluster
$SA = (confluent iam service-account list -o json | ConvertFrom-Json | Where-Object name -eq "app-pedidos").id
echo "SR=$SR  SA=$SA"
```

```bash
confluent api-key create --resource $SR --service-account $SA --description "Key de Schema Registry - bootcamp"
```

```
+------------+------------------------------------------------------------------+
| API Key    | SUA-API-KEY                                                 |
| API Secret | cflt-SEU-SECRET-AQUI |
+------------+------------------------------------------------------------------+
```

```bash
export SR_API_KEY="SUA-API-KEY"
export SR_API_SECRET="cflt-SEU-SECRET-AQUI..."
```

Confirme que agora existem **duas** keys, com escopos diferentes:

```bash
confluent api-key list
```

**O que observar**: uma linha com `Resource = lkc-...` (cluster) e outra com
`lsrc-...` (Schema Registry). Mesma service account, credenciais distintas — porque
são **recursos** distintos.

---

## Passo 2 — Recrie o tópico *(~2 min)*

Você apagou `pedidos` no teardown da Aula 2. Recrie:

**🖱 Pela Console**: **Topics** → **+ Add topic** → `pedidos`, 3 partições.

**⌨️ Pela CLI**:

```bash
confluent kafka topic create pedidos --partitions 3
```

Desta vez ele vai ter contrato.

---

## Passo 3 — Registre o contrato (v1) *(~6 min)*

Crie o arquivo do schema:

**🖱 Pela Console**: tópico `pedidos` → aba **Schema** → **Set a schema** (value) → cole o Avro → **Create**. É a mesma operação do comando abaixo — a UI registra no mesmo subject `pedidos-value`.

**⌨️ Pela CLI**:

```bash
cat > pedido-v1.avsc <<'EOF'
{
  "type": "record",
  "name": "PedidoCriado",
  "namespace": "com.bootcamp.pedidos",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "total", "type": "double"}
  ]
}
EOF
```

Registre:

```bash
confluent schema-registry schema create \
  --subject pedidos-value \
  --schema pedido-v1.avsc \
  --type avro
```

**Output esperado**
```
Successfully registered schema with ID "100001".
```

**O que observar**: o **schema ID** (`100001`). É esse número que vai viajar dentro de
cada mensagem, nos 4 bytes após o magic byte.

Veja o subject criado:

```bash
confluent schema-registry subject list
confluent schema-registry subject describe pedidos-value
```
```
  Version
-----------
        1
```

**O que observar**: o contrato agora é um **recurso da plataforma** — versionado,
visível, fora do código. Qualquer time pode consultá-lo sem perguntar a ninguém.

> 💡 Abra a UI (**Environments → bootcamp-dio → Schema Registry**) e veja o mesmo
> subject renderizado. CLI e UI são duas portas para o mesmo lugar.

---

## Passo 4 — Evolua com sucesso (v2) *(~6 min)*

Regra de negócio nova: o pedido precisa registrar o **canal** de origem. Mas há
consumidores em produção lendo o schema v1 — eles não podem quebrar.

**🖱 Pela Console**: aba **Schema** do tópico → **Evolve schema** → adicione o campo com default → **Validate** (a checagem de compatibilidade roda na tela) → **Save**.

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

Repare nos dois detalhes do campo novo: tipo **union com `null`** e **`default`**.

```bash
confluent schema-registry schema create \
  --subject pedidos-value \
  --schema pedido-v2.avsc \
  --type avro
```

```
Successfully registered schema with ID "100002".
```

```bash
confluent schema-registry subject describe pedidos-value
```
```
  Version
-----------
        1
        2
```

**O que observar**: **aceito**. O subject tem duas versões, e mensagens escritas com v1
continuam legíveis — quem lê com o v2 encontra `canal = null` nelas.

**Por que passou**: o campo novo tem default. Um leitor do v2 diante de um dado v1
(que não tem `canal`) sabe o que usar. Nada quebra.

---

## Passo 5 — Seja rejeitado *(~8 min)*

Agora o momento da aula. Um time decide adicionar `cupom` como campo **obrigatório**:

**🖱 Pela Console**: a mesma tela **Evolve schema** — cole a versão que quebra e clique **Validate**: a Console recusa com o mesmo erro de compatibilidade que a CLI mostra abaixo.

**⌨️ Pela CLI**:

```bash
cat > pedido-v3-quebra.avsc <<'EOF'
{
  "type": "record",
  "name": "PedidoCriado",
  "namespace": "com.bootcamp.pedidos",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "total", "type": "double"},
    {"name": "cupom", "type": "string"}
  ]
}
EOF
```

```bash
confluent schema-registry schema create \
  --subject pedidos-value \
  --schema pedido-v3-quebra.avsc \
  --type avro
```

**Output esperado — o erro é o objetivo**
```
Error: Schema being registered is incompatible with an earlier schema for subject
"pedidos-value", details: [{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE',
description:'The field 'cupom' at path '/fields/3' in the new schema has no default
value and is missing in the old schema', additionalInfo:'cupom'}, ...
{compatibility: 'BACKWARD'}]; error code: 40901
```

**O que observar** — leia a mensagem inteira, ela é excelente:

- `READER_FIELD_MISSING_DEFAULT_VALUE` — o tipo exato do problema
- `The field 'cupom' ... has no default value` — **qual** campo e **por quê**
- `compatibility: 'BACKWARD'` — a regra que barrou

**Por que quebrou**: um consumidor com o schema novo, ao ler uma mensagem antiga (que
não tem `cupom`), não teria o que colocar no campo. Sem default, não há resposta — e o
Registry recusa antes que isso vire erro em produção.

> 💡 **O portão funcionou.** Este erro apareceu no seu terminal, em segundos. Sem
> Schema Registry, ele apareceria semanas depois, no consumidor de outro time, em
> produção, às 3 da manhã.

### A regra do BACKWARD, sem decoreba

Muita gente resume BACKWARD como "não pode remover campo" — **e isso está errado**.
Teste você mesmo: registre um schema **sem** o `order_id` e veja passar.

| Mudança | BACKWARD aceita? | Por quê |
|---|---|---|
| Adicionar campo **com default** | ✅ | O leitor novo preenche o default em dados antigos |
| Adicionar campo **sem default** | ❌ | O leitor novo não sabe o que fazer com dados antigos |
| **Remover** campo | ✅ | O leitor novo simplesmente ignora o campo nos dados antigos |
| Trocar o tipo | ❌ | Não há conversão segura |

A pergunta que resolve todos os casos:

> **"Um consumidor com o schema NOVO consegue ler dados escritos com o schema VELHO?"**
> Se sim, passa em BACKWARD.

É por isso que BACKWARD é o default do bootcamp: ele protege **os consumidores**, que
são quem quebra em produção.

---

## Passo 6 — Onde ver a compatibilidade *(~4 min)*

**🖱 Pela Console**: aba **Schema** → ícone de configurações → **Compatibility settings** — o modo `BACKWARD` que governa tudo.

**⌨️ Pela CLI**:

```bash
confluent schema-registry cluster describe
```
```
| Global Compatibility | BACKWARD |
```

Esse é o nível **global** do environment. Ele pode ser sobrescrito por subject — útil
quando um tópico específico precisa de regra diferente (ex.: `FORWARD` num tópico cujo
produtor é que precisa de liberdade).

> ⚠️ Baixar a compatibilidade para `NONE` "só para destravar o deploy" é o caminho
> mais rápido para um incidente. Se você precisar disso, o correto é versionar o
> tópico (`pedidos.v2`), não desligar o portão.

---

## Passo 7 — Fixe os conceitos *(~5 min)*

1. Sua key de cluster funciona no Schema Registry? Por quê?
2. Você precisa adicionar um campo obrigatório. Como fazer sem quebrar BACKWARD?
3. Um consumidor lê uma mensagem com schema ID que ele nunca viu. O que acontece?
4. Por que BACKWARD é o default recomendado, e não FORWARD?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Não.** São recursos diferentes (`lkc-` e `lsrc-`), cada um com credencial própria.
O erro que aparece é de **autenticação** — e é enganoso, porque a key está correta,
só que para o outro recurso. Foi por isso que o Passo 1 existe.

**2. Adicione com default** (e, se fizer sentido, união com `null`). Depois que todos
os produtores estiverem enviando o campo e todos os dados antigos tiverem expirado
pela retention, o campo pode virar obrigatório numa versão futura. Na prática, muita
gente simplesmente deixa o default para sempre — não custa nada.

**3. Ele busca o schema no Registry pelo ID** e faz cache local. É por isso que o ID
viaja na mensagem: o consumidor não precisa saber de antemão quais schemas existem.
Se o Registry estiver inacessível e o ID não estiver em cache, aí sim o consumo falha.

**4. Porque quem quebra em produção é o consumidor.** BACKWARD garante que um
consumidor atualizado leia dados antigos — permitindo atualizar consumidores primeiro
e produtores depois. FORWARD garante o contrário (produtor novo, consumidor velho), o
que é útil quando você não controla os consumidores, mas é o caso menos comum.

</details>

---

## Se algo não funcionou

**Erro de autenticação ao registrar schema**
O erro nº 1: key de cluster no Schema Registry. Confira `confluent api-key list` e
use a que tem `Resource = lsrc-...`.

**`Error: unknown flag: --api-key` no comando de schema**
Esse subcomando usa `--schema-registry-api-key`, ou simplesmente a sua sessão do CLI
se você estiver logado como usuário.

**O schema incompatível foi aceito**
Confira o nível de compatibilidade (`confluent schema-registry cluster describe`). Se
estiver `NONE`, o portão está desligado. E lembre: **remover campo passa em BACKWARD** —
para ver a rejeição, adicione um campo **sem default**.

**Quero apagar um schema de teste**
```bash
confluent schema-registry schema delete --subject pedidos-value --version 3 --force
```
Em produção isso é operação delicada — o subject guarda o histórico por um motivo.

---

## O que fica de pé

**Nada é destruído nesta aula.** A Aula 4 usa exatamente este tabuleiro:

| Recurso | Estado |
|---|---|
| Tópico `pedidos` | Criado, 3 partições |
| Subject `pedidos-value` | **2 versões** (v1 e v2) |
| Service account + 2 API keys | Cluster e Schema Registry |
| ACLs | Do Passo 5 da Aula 2 |

Se quiser encerrar a sessão de estudo agora, tudo bem: cluster Basic ocioso não queima
crédito relevante. Só confira que não há conector nem Flink ligado.

---

## Glossário

| Termo | Definição |
|---|---|
| **Schema Registry** (`lsrc-`) | Serviço que armazena e versiona contratos; recurso separado do cluster. |
| **Subject** | Onde as versões de um schema vivem; por convenção, `{tópico}-value`. |
| **Schema ID** | Identificador global do schema; viaja em cada mensagem. |
| **Wire format** | `magic byte (0) + schema ID (4 bytes) + payload Avro`. |
| **BACKWARD** | Consumidor novo lê dados antigos. O default do bootcamp. |
| **Data Contract** | Schema + metadados + regras de qualidade. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Produzir com serializer Avro | **Aula 4** — as duas keys em uso |
| Schema em conectores CDC | Curso 3 — o Debezium registra sozinho |
| Contrato como entrada do Flink | Curso 4 |
| Data Quality Rules (CEL) | Curso 7 — governança |

**Fontes**

- Confluent — [Schema Registry](https://docs.confluent.io/cloud/current/sr/index.html)
- Confluent — [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/avro.html)
- Confluent — [Data contracts](https://docs.confluent.io/cloud/current/sr/fundamentals/data-contracts.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 2 · Primeiros passos](../aula2/README.md) | [Curso 2](../README.md) | [Aula 4 · Primeira mensagem real](../aula4/README.md) |
