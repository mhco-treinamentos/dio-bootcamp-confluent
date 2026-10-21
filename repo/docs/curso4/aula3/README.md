# Curso 4 · Aula 3 — Janelas e joins: criando o "fim" de um stream infinito

> **Guia do aluno** · Bloco 3 — Processamento de Streams
> ⏱ ~50 min · ☁️ **Statement perpétuo cobra** · UI + CLI

`GROUP BY` num fluxo que não acaba nunca fecha. Esta aula ensina a **criar um fim**: a janela. E resolve uma armadilha que só aparece quando a fonte é CDC.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 2](../aula2/README.md) | Pool ativo, CTAS entendido |
| Fluxo append-only **contínuo** (Datagen) | O Passo 1 cria — janela parada não fecha |
| Workspace SQL + 1 terminal | |

```bash
confluent flink compute-pool list
confluent flink statement list | grep RUNNING
```

- `confluent flink statement list | grep RUNNING` — vazio antes de começar

---

## O que você vai saber fazer ao final

- Explicar por que janela sobre fonte CDC **não funciona** — e o que fazer
- Usar TUMBLE, HOP e SESSION, sabendo quando cada uma serve
- Entender watermark e o destino do evento atrasado
- Enriquecer um stream com um temporal join

---

## ⚠️ Passo 1 — A armadilha do CDC *(~8 min)*

Este passo evita meia hora de frustração. Tente janelar direto o tópico CDC:

```sql
SELECT window_start, window_end, COUNT(*) AS txns
FROM TABLE(
  TUMBLE(TABLE `pg-payments.payments.transactions`,
         DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
```

**O statement vai a `FAILED`**, com esta mensagem:

```
The metadata column(s): '$rowtime' in cdc source may cause wrong result or
error on downstream operators, please consider removing these columns
or use a non-cdc source that only has insert messages.
```

**Por que isso acontece** — e é conceito, não bug:

Um tópico CDC é um **changelog**, não um fluxo de fatos. Quando uma linha é
atualizada, o Debezium emite um evento que **retrata** o anterior. Contar eventos num
changelog não responde "quantas transações houve" — um UPDATE não é transação nova.

| Fonte | Natureza | Janela funciona? |
|---|---|---|
| Tópico CDC (`op=c/u/d`) | **Changelog** — eventos se retratam | ❌ |
| Tópico de eventos | **Append-only** — cada fato é definitivo | ✅ |

> 💡 A herança contamina: um CTAS derivado de CDC (como o `transacoes_grandes` da Aula
> 2) **também** é changelog, e também falha. A natureza se propaga.

### A saída — e a segunda regra das janelas

Use uma fonte **append-only**. Mas há mais um requisito, que morde quem usa dados
parados: **janela só fecha quando a watermark avança, e watermark só avança com fluxo
novo chegando**. Um tópico com 3 eventos estáticos deixa a janela aberta para sempre —
o SELECT roda e não emite **nada**.

Por isso os passos de TUMBLE/HOP usam o **Datagen** (1 msg/s, fluxo contínuo — o mesmo
do Curso 3, Aula 1). Recrie-o se não estiver rodando:

**macOS / Linux**
```bash
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos'][0])")
confluent kafka acl create --allow --service-account $SA --operations CREATE,WRITE,READ,DESCRIBE --topic lab. --prefix
confluent kafka topic create lab.transactions.datagen --partitions 3
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
confluent connect cluster create --config-file datagen.json
```

> 💸 O Datagen cobra ~US$ 0,035/hora enquanto ligado — o **Limpar** desta aula o
> deleta.

> Em produção, o caminho é **materializar um snapshot append-only** a partir do
> changelog: uma janela que pega a última versão por chave e emite como fato. Aí sim
> se janela em cima.

---

## Os conceitos em 90 segundos

**O problema**: `GROUP BY` precisa saber quando o grupo acabou. Num stream infinito,
nunca acaba. A **janela** cria esse fim artificialmente.

```
TUMBLE (fixa, sem sobreposição)     │──1──│──2──│──3──│
   "volume por minuto"

HOP (deslizante, com sobreposição)  │────1────│
   "média das últimas 24h,               │────2────│
    atualizando a cada hora"                  │────3────│

SESSION (por inatividade)           │─1─│    │──2──│  │─3─│
   "atividade de um cartão"          gap    gap    gap
```

**Watermark**: como o Flink decide que uma janela pode fechar. É a aposta *"não espero
mais nada com timestamp anterior a X"*. Evento que chega depois disso é **descartado**.

> 📖 A anatomia completa das janelas e o event time × processing time estão **na aula
> gravada**.

---

## Passo 2 — TUMBLE: volume por minuto *(~8 min)*

```sql
SELECT window_start, window_end, COUNT(*) AS txns
FROM TABLE(
  TUMBLE(TABLE `lab.transactions.datagen`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
```

Confirme que subiu:

```bash
confluent flink statement list | grep RUNNING
```

**O que observar**:

- **Espere o minuto virar**: uma linha nova aparece a cada janela que fecha — é o
  fluxo contínuo do Datagen avançando a watermark. (Com um tópico parado, você não
  veria NADA — a regra do Passo 1.)
- Cada linha é uma **janela fechada**, com resultado **definitivo** — diferente do
  `GROUP BY` da Aula 2, que mudava no lugar
- As janelas não se sobrepõem: `10:00–10:01`, `10:01–10:02`…
- **A janela criou o "fim"** que o stream não tinha

Compare com a Aula 2: lá o total de `SETTLED` era um número que nunca parava de mudar.
Aqui, a contagem de 10:00–10:01 **fecha e não muda mais**.

---

## Passo 3 — HOP: o mesmo evento em várias janelas *(~7 min)*

```sql
SELECT window_start, window_end, COUNT(*) AS pedidos
FROM TABLE(
  HOP(TABLE `lab.transactions.datagen`, DESCRIPTOR(`$rowtime`),
      INTERVAL '1' MINUTE,     -- de quanto em quanto tempo desliza
      INTERVAL '5' MINUTE))    -- o tamanho da janela
GROUP BY window_start, window_end;
```

**O que observar**: as janelas **se sobrepõem** — e o mesmo pedido é contado em várias
delas. Isso não é erro: é a pergunta que HOP responde.

| Pergunta | Janela |
|---|---|
| "Quantas transações por minuto?" (relatório) | **TUMBLE** |
| "A média móvel das últimas 24h subiu?" (alerta) | **HOP** |
| "Quanto tempo durou a sessão deste cartão?" | **SESSION** |

> ⚠️ HOP multiplica o trabalho: janela de 5 min deslizando a cada 1 min significa que
> cada evento é processado **5 vezes**. Em volume alto, isso pesa — e aparece no CFU.

---

## Passo 4 — SESSION: a janela que os dados definem *(~7 min)*

TUMBLE e HOP têm tamanho fixo, escolhido por você. **SESSION** é diferente: quem define
o fim é o próprio fluxo — a janela fecha quando passa um **gap** sem eventos. É a
janela de "rajada de atividade": a sessão de uso de um cartão, a sequência de pedidos
de um cliente.

Para o SESSION, o Datagen contínuo é a fonte **errada** — sem gap, a sessão nunca
fecha! Aqui a fonte certa é o `pedidos_sink` de produção **manual** (rajadas com
silêncio entre elas). Crie e semeie, se não tiver:

```bash
confluent kafka topic create pedidos_sink --partitions 3
cat > pedido.avsc <<'EOF'
{"type":"record","name":"Pedido","namespace":"bootcamp","fields":[
 {"name":"order_id","type":"string"},
 {"name":"customer_id","type":"string"},
 {"name":"status","type":"string"},
 {"name":"total","type":"double"}]}
EOF
confluent kafka topic produce pedidos_sink --parse-key --delimiter : --value-format avro --schema pedido.avsc --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET" <<'FIM'
ord-1:{"order_id":"ord-1","customer_id":"cli-99","status":"CREATED","total":289.90}
ord-2:{"order_id":"ord-2","customer_id":"cli-42","status":"CREATED","total":150.00}
ord-3:{"order_id":"ord-3","customer_id":"cli-99","status":"PAID","total":75.50}
FIM
```

```sql
SELECT window_start, window_end, customer_id, COUNT(*) AS pedidos
FROM TABLE(
  SESSION(TABLE pedidos_sink PARTITION BY customer_id,
          DESCRIPTOR(`$rowtime`), INTERVAL '2' MINUTE))
GROUP BY window_start, window_end, customer_id;
```

**O que observar** — três diferenças em relação às janelas anteriores:

1. **`PARTITION BY customer_id` dentro da TVF** — cada cliente tem as SUAS sessões,
   com fronteiras próprias. TUMBLE/HOP cortam o tempo igual para todos; SESSION corta
   por chave.
2. Com os pedidos do Passo 1 (produzidos juntos), você vê **uma sessão por cliente**:
   `cli-99` com 2 pedidos, `cli-42` com 1 — todos dentro do mesmo gap.
3. **Agora prove o gap**: espere mais de 2 minutos e produza outro pedido
   (o bloco acima, trocando o `order_id` para `ord-9`). No momento em que ele chega,
   a watermark avança — e as sessões **antigas fecham e aparecem** na tela. É a
   mecânica nua: quem fecha a sessão não é o relógio da parede, é o **próximo evento**
   provando que o gap passou.

| Janela | Quem define o fim | Pergunta típica |
|---|---|---|
| TUMBLE | você (tamanho fixo) | "quantos por minuto?" |
| HOP | você (tamanho + deslize) | "média móvel subiu?" |
| **SESSION** | **os dados** (gap de silêncio) | "quanto durou a atividade?" |

> ⚠️ SESSION é a janela mais cara das três: o Flink mantém a sessão **aberta em
> estado** até o gap vencer — e chaves de alta cardinalidade multiplicam isso. No
> antifraude real (regras de velocity por cartão), é ela que modela a rajada — com
> `PARTITION BY card_id` e gap curto.

---

## Passo 5 — O evento atrasado *(~8 min)*

Esta é a cena mais importante da aula.

Um evento pode chegar **depois** que sua janela já fechou — POS offline que sincroniza,
partição lenta, retry. O que acontece com ele?

```sql
SELECT
  window_start,
  COUNT(*) AS eventos,
  MAX(TIMESTAMPDIFF(SECOND, `$rowtime`, CURRENT_TIMESTAMP)) AS atraso_max_s
FROM TABLE(
  TUMBLE(TABLE `lab.transactions.datagen`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE))
GROUP BY window_start;
```

**O que observar**: a coluna de atraso mostra a distância entre o timestamp do evento
e agora. É esse número que decide o quanto sua watermark precisa tolerar.

**A regra**:

```
watermark = INTERVAL '30' SECOND
   │
   ├── evento com 20s de atraso  → ENTRA na janela  ✅
   └── evento com 45s de atraso  → DESCARTADO       ❌
```

**O trade-off que não tem resposta única**:

| Delay | Consequência | Quando usar |
|---|---|---|
| 5 s | Fecha rápido, descarta mais | Antifraude bloqueante |
| 30 s | Equilíbrio | Default dos labs |
| 5 min | Quase nada se perde, resultado demora | Relatório analítico |

> 💡 **Cauda longa não é problema de watermark.** Se um evento chega horas depois, não
> adianta esperar — a solução é reprocessamento em batch, ou um destino idempotente
> que aceite a correção tardia (o upsert do Curso 3).

---

## Passo 6 — Temporal join: enriquecer com o cadastro *(~10 min)*

Janela resolve agregação. **Join** resolve enriquecimento: a transação tem
`account_id`, mas você quer o *tier* da conta.

Este é o momento em que o CDC volta a ser útil — como **tabela versionada**. Mas há um
pré-requisito que a tabela inferida não traz sozinha: **PRIMARY KEY**. A PK do Postgres
virou a key do evento (Curso 3), porém a tabela Flink nasce sem a constraint — e o
temporal join a exige nos dois lados. Declare:

```sql
ALTER TABLE `pg-payments.payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;
```
```sql
ALTER TABLE `pg-payments.payments.transactions` ADD PRIMARY KEY (transaction_id) NOT ENFORCED;
```

(Cada ALTER termina em `COMPLETED` na hora — é mudança de catálogo, não job. Com a PK,
o changelog vira **upsert**: agora o Flink sabe qual é "a versão vigente" de cada
chave — exatamente o que o temporal join consulta. E a mudança é permanente: rode uma
vez só.)

E o segundo pré-requisito — a watermark, pela **terceira vez** nesta aula: o temporal
join só emite uma linha quando a watermark **dos dois lados** passa do timestamp do
evento. Tópicos CDC parados (conector desligado) = watermarks travadas = join roda
**sem retornar nada**, para sempre. Garanta:

1. **O conector CDC `RUNNING`** (é o Replay rápido da [Aula 2 do Curso 3](../../curso3/aula2/README.md) —
   o próprio snapshot empurra as watermarks ao chegar)
2. **`sql.tables.scan.idle-timeout`** no statement — a válvula para a dimensão que
   fica em silêncio depois do snapshot (contas mudam raramente):

```sql
SET 'sql.tables.scan.idle-timeout' = '5 s';
```

Agora o join:

```sql
SELECT
  t.transaction_id,
  t.amount,
  a.tier         AS account_tier,
  a.status       AS account_status
FROM `pg-payments.payments.transactions` t
JOIN `pg-payments.payments.accounts`
  FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
```

> 💡 **Por que o Passo 1 continua falhando e este funciona?** A PK destrava o
> `$rowtime` para o *join* (o changelog virou upsert versionado), mas **janela** sobre
> changelog segue proibida — agregar eventos que se retratam continua sendo a pergunta
> errada. Os dois erros são o mesmo guarda, protegendo de coisas diferentes.

> Se o tópico `pg-payments.payments.accounts` não existir, seu conector CDC captura só
> `transactions`. Ajuste o `table.include.list` (Curso 3, Aula 2) para:
> `payments.transactions,payments.accounts`

**O que observar** — e por que `FOR SYSTEM_TIME AS OF` importa:

Ele casa cada transação com a versão da conta **vigente naquele momento**, não a atual.
Se a conta virou PREMIUM ontem, uma transação de anteontem é enriquecida com o tier
**antigo** — que é o correto para auditoria.

Um join comum daria a resposta errada: reescreveria a história com o estado de hoje.

| Tipo de join | O que faz | Cuidado |
|---|---|---|
| **Temporal** (`FOR SYSTEM_TIME`) | Casa com a versão vigente no evento | Exige PK e watermark |
| **Interval** (`BETWEEN ... AND`) | Correlaciona dois streams numa janela | Limita o estado |
| **Regular** | Join clássico | **Acumula estado para sempre** |

> ⚠️ Join regular entre dois streams infinitos é a maneira mais rápida de estourar o
> CFU: o Flink guarda os dois lados **indefinidamente**. Sempre limite — por tempo
> (interval), por versão (temporal) ou por `sql.state-ttl`.

### Materialize o resultado

```sql
CREATE TABLE transacoes_enriquecidas AS
SELECT t.transaction_id, t.amount, t.status, a.tier
FROM `pg-payments.payments.transactions` t
JOIN `pg-payments.payments.accounts`
  FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;
```

**O que observar**: este tópico é o **insumo dos próximos cursos** — o Lakehouse
(Curso 5) e a IA (Curso 6) vão consumir dados já enriquecidos.

---

## Passo 7 — Fixe os conceitos *(~5 min)*

1. Por que a janela falhou sobre o tópico CDC?
2. Você quer "quantas transações por hora". Qual janela?
3. Sua watermark é 30s e um evento chega 2 minutos atrasado. O que acontece?
4. Por que `FOR SYSTEM_TIME AS OF` e não um join comum?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque CDC é changelog, não fluxo de fatos.** Os eventos se retratam — um UPDATE
invalida o estado anterior. Janela pressupõe que cada evento é um fato definitivo que
se soma aos outros. Contar eventos de changelog responderia "quantas *mudanças de
linha*", não "quantas transações".

**2. TUMBLE de 1 hora.** Janelas fixas, sem sobreposição, cada transação contada
exatamente uma vez. HOP daria contagens sobrepostas (útil para média móvel, errado
para relatório).

**3. É descartado.** A janela dele já fechou e o resultado foi emitido. Para não
perder, as opções são: aumentar a watermark (custa latência), reprocessar em batch, ou
usar um destino idempotente que aceite a correção. **Não existe almoço grátis** —
esperar mais sempre custa latência.

**4. Porque um join comum usaria o estado ATUAL da conta**, reescrevendo a história.
Uma transação de janeiro apareceria com o tier de hoje. O temporal join casa com a
versão **vigente no momento do evento** — que é o que auditoria, cálculo de limite e
análise histórica exigem.

</details>

---

> 💡 O `$POOL` vem da captura da [Aula 1](../aula1/README.md):
> `POOL=$(confluent flink compute-pool list -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['id'] if d else '')")`

## Se algo não funcionou

**`'$rowtime' in cdc source may cause wrong result`**
É o Passo 1: fonte CDC não janela. Use um tópico append-only.

**A janela não emite nada**
Ela só fecha quando a watermark avança — e a watermark avança com **dados novos
chegando**. Fonte parada = janela aberta para sempre. Para TUMBLE/HOP, confirme o
Datagen `RUNNING` (`confluent connect cluster list`); para SESSION, produza o próximo
evento — é ele que fecha a sessão anterior.

**`Temporal Table Join requires primary key in versioned table`**
A tabela inferida não tem PK — rode os dois `ALTER TABLE ... ADD PRIMARY KEY` do
Passo 6 (uma vez só; a mudança fica no catálogo).

**`'$rowtime' in cdc source` no JOIN (depois do primeiro ALTER)**
Faltou a PK do **outro** lado — o erro aponta a tabela sem constraint (veja o
`changelogMode=[I,UB,UA,D]` no plano). São DOIS ALTERs, um por tabela.

**O join roda mas não retorna NADA**
Watermarks travadas — os tópicos CDC estão parados (conector desligado) ou a dimensão
está em silêncio. Confira o conector (`confluent connect cluster list` → RUNNING),
adicione o `SET 'sql.tables.scan.idle-timeout' = '5 s';` e, para destravar na hora,
gere um evento (um UPDATE em `payments.accounts` no Neon empurra a watermark da
dimensão).

**O join não retorna linhas**
Confirme que o tópico da dimensão existe e tem dados:
```bash
confluent kafka topic list | grep accounts
```
Se estiver vazio, ajuste o `table.include.list` do conector CDC.

**O statement consome muito CFU**
Provavelmente um join regular ou agregação sem janela acumulando estado. Adicione
`SET 'sql.state-ttl' = '1 HOURS';` antes da query.

---

## Limpar

```bash
confluent flink statement list
confluent flink statement stop NOME
confluent flink compute-pool describe $POOL | grep "Current CFU"
CONN=$(confluent connect cluster list -o json | python3 -c "import json,sys; print(([c['id'] for c in json.load(sys.stdin) if c['name']=='datagen-transactions']+[''])[0])")
confluent connect cluster delete $CONN --force
```

- `confluent flink statement list` — veja o que está rodando
- `confluent flink compute-pool describe $POOL | grep "Current CFU"` — deve ser 0
- o delete do Datagen encerra a única peça que cobra por hora ligada

O tópico `transacoes_enriquecidas` **fica** — é insumo dos Cursos 5 e 6.

---

## Glossário

| Termo | Definição |
|---|---|
| **TUMBLE** | Janela fixa, sem sobreposição. |
| **HOP** | Janela deslizante; o mesmo evento entra em várias. |
| **SESSION** | Janela por inatividade (gap). |
| **Watermark** | Aposta de "não espero mais nada anterior a X". |
| **Evento atrasado** | Chega depois da janela fechar; é descartado. |
| **Temporal join** | Casa com a versão vigente **no momento do evento**. |
| **Changelog × append-only** | Eventos que se retratam × fatos definitivos. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Estado por trás da janela | [Aula 4](../aula4/README.md) |
| Enriquecido virando tabela Iceberg | Curso 5 — Tableflow |
| Features para modelo de fraude | Curso 6 — `ML_PREDICT` |
| Detecção de padrão (`MATCH_RECOGNIZE`) | Desafio Final |

**Fontes**

- Confluent — [Window aggregations](https://docs.confluent.io/cloud/current/flink/reference/queries/window-aggregation.html)
- Confluent — [Time and watermarks](https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html)
- Apache Flink — [Joins](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 2 · Tables e Streams](../aula2/README.md) | [Curso 4](../README.md) | [Aula 4 · Flink em produção](../aula4/README.md) |
