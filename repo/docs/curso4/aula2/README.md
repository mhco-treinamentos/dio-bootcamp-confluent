# Curso 4 · Aula 2 — Tables e Streams: a dualidade

> **Guia do aluno** · Bloco 3 — Processamento de Streams
> ⏱ ~35 min · ☁️ **Statement perpétuo cobra** · UI + CLI

Nesta aula você descobre que **tabela e stream são a mesma coisa vista de ângulos diferentes** — e cria seu primeiro statement que sobrevive a você fechar o navegador.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 1](../aula1/README.md) | O compute pool já criado |
| Workspace SQL aberto | Onde os resultados aparecem |
| 1 terminal | Para gerenciar statements |

```bash
confluent flink compute-pool list
confluent flink statement list | grep RUNNING
```

Capture o id do pool para os comandos da aula:

**macOS / Linux**
```bash
POOL=$(confluent flink compute-pool list -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['id'] if d else '')")
echo "POOL=$POOL"
```

**Windows (PowerShell)**
```powershell
$POOL = (confluent flink compute-pool list -o json | ConvertFrom-Json)[0].id
echo "POOL=$POOL"
```

> ⚠️ O echo deve mostrar um `lfcp-`. Vazio = não há pool neste environment — crie
> ([Aula 1](../aula1/README.md), Passo 1). A captura pega o **primeiro** pool da
> lista, qualquer que seja o nome; com mais de um pool, defina na mão: `POOL=lfcp-xxxxx`.


- `confluent flink compute-pool list` — seu pool, PROVISIONED
- `confluent flink statement list | grep RUNNING` — deve estar vazio

---

## O que você vai saber fazer ao final

- Navegar catálogos, bancos e tabelas do Flink
- Explicar por que seus tópicos viraram tabelas sem `CREATE TABLE`
- Distinguir consulta **append** de consulta **updating**
- Criar um tópico derivado com `CREATE TABLE AS SELECT`
- Reconhecer que um CTAS cria **dois** recursos — e dois custos

---

## Os conceitos em 90 segundos

**A dualidade**:

> Uma **tabela** é um stream parado no tempo.
> Um **stream** é uma tabela em movimento.

O saldo da sua conta (tabela) é o resultado de todas as transações (stream). E a lista
de transações é o histórico que produziu o saldo. Mesma informação, duas visões.

**O mapeamento no Confluent Cloud** — o que já existe vira SQL sem esforço:

| SQL | Confluent Cloud |
|---|---|
| catalog | environment (`bootcamp-dio`) |
| database | cluster Kafka |
| **table** | **tópico** |
| colunas | schema do Registry |

> 📖 A explicação completa da dualidade e os modos de tabela estão **na aula gravada**.

---

## Passo 1 — Navegue o catálogo *(~6 min)*

No workspace SQL:

```sql
SHOW CATALOGS;
SHOW DATABASES;
SHOW TABLES;
```

**O que observar**: seus tópicos aparecem como tabelas. **Quem escreveu esses
`CREATE TABLE`?** Ninguém — o Flink lê o Schema Registry e monta a estrutura.

Veja a estrutura de uma delas:

```sql
DESCRIBE `pg-payments.payments.transactions`;
```

**O que observar**: as colunas vieram do **contrato Avro** que o conector CDC
registrou no Curso 3. Cada decisão de schema lá virou coluna SQL aqui, de graça.

Repare também nas colunas de metadados, como `$rowtime` — o timestamp do record
Kafka. Ela vai importar (e dar problema) na [Aula 3](../aula3/README.md).

> 💡 **Crases** são obrigatórias em nomes com ponto: `` `pg-payments.payments.transactions` ``.
> Sem elas, o Flink interpreta os pontos como separadores de catálogo/database.

---

## Passo 2 — Append × updating *(~8 min)*

Duas consultas, dois comportamentos radicalmente diferentes.

### Append: linhas se acumulam

```sql
SELECT transaction_id, amount, status
FROM `pg-payments.payments.transactions`
WHERE amount > 500;
```

**O que observar**: linhas **novas aparecem embaixo**. Nada muda de lugar. É o modo
**append** — cada evento é uma linha que se soma às anteriores.

### Updating: valores mudam no lugar

```sql
SELECT status, COUNT(*) AS total
FROM `pg-payments.payments.transactions`
GROUP BY status;
```

**O que observar** — e é bem diferente: os números **mudam no lugar**. `SETTLED: 1204`
vira `1205`, depois `1206`. As linhas não se acumulam; elas são **atualizadas**.

É o modo **updating**: o resultado é uma *tabela viva* que reflete o estado atual do
agregado.

| Modo | Comportamento | Exemplo |
|---|---|---|
| **Append** | Linhas se acumulam | `SELECT ... WHERE` |
| **Updating** | Valores mudam no lugar | `GROUP BY` sem janela |

> ⚠️ Uma agregação sem janela **acumula estado para sempre** — a contagem por status
> nunca "fecha". Para produção, isso exige `sql.state-ttl` ou uma janela
> ([Aula 3](../aula3/README.md)). Em aula, é seguro porque você vai parar o statement.

---

## Passo 3 — CREATE TABLE AS SELECT *(~10 min)*

Até aqui os resultados só existiam na tela. Agora eles viram **tópico**:

```sql
CREATE TABLE transacoes_grandes AS
SELECT transaction_id, account_id, amount, status
FROM `pg-payments.payments.transactions`
WHERE amount > 500;
```

Verifique no terminal:

```bash
confluent flink statement list | grep -i grandes
```
```
| ctas-grandes | CREATE TABLE transacoes_grandes AS ... | lfcp-xxxxx | RUNNING
```

**O que observar**: `RUNNING` — e vai continuar assim **mesmo se você fechar o
navegador**. Este é o statement perpétuo.

### O tópico existe de verdade

Saia do Flink e olhe o cluster:

```bash
confluent kafka topic list | grep grandes
```
```
  transacoes_grandes | false | 3 | 6
```

Para o consume, uma credencial — se não tiver o par no terminal, crie em 30 s
(autocaptura + key nova; o secret aparece uma vez):

**macOS / Linux**
```bash
CLUSTER=$(confluent kafka cluster describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print(([s['id'] for s in json.load(sys.stdin) if s['name']=='app-pedidos']+[''])[0])")
confluent api-key create --resource $CLUSTER --service-account $SA --description "Key do app-pedidos"
CC_API_KEY="SUA-API-KEY"
CC_API_SECRET="cflt-SEU-SECRET-AQUI"
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --topic transacoes_grandes_v2
confluent kafka acl create --allow --service-account $SA --operations READ,DESCRIBE --consumer-group transacoes_ --prefix
```

```bash
confluent kafka topic consume transacoes_grandes_v2 --from-beginning \
  --value-format avro --group transacoes_grandes_v2.viewer \
  --api-key "$CC_API_KEY" --api-secret "$CC_API_SECRET"
```

**Output esperado**
```
{"account_id":"f41ea3ac-5fd2-4109-8140-752caa448259","amount":"5489/10","status":"SETTLED","transaction_id":"d4a057a7-b0cc-4b3b-9793-b9b413748146"}
```

E o schema foi registrado sozinho:

```bash
confluent schema-registry subject list | grep grandes
```
```
  transacoes_grandes-value
```

**O que observar** — três coisas que fecham o arco do bootcamp:

1. **O resultado do Flink É um tópico Kafka de primeira classe.** Qualquer consumidor,
   conector ou outro statement pode lê-lo.
2. **O schema foi registrado automaticamente** — o Flink inferiu do resultado.
3. **`"amount":"5489/10"`** = R$ 548,90. O decimal preciso do Curso 3 atravessou o
   Flink sem virar float.

> ⚠️ **Um CTAS cria DOIS recursos**: um **tópico** (barato, guarda dados) e um
> **statement perpétuo** (caro, ~$151/mês se esquecido). Deletar o tópico não para o
> statement — e vice-versa.

---

## Passo 4 — Pare o statement *(~4 min)*

**🖱 Pela Console**: aba **Flink** → **Statements** → o statement do CTAS → **Stop**. (No workspace, o botão ■ do card da query faz o mesmo.)

**⌨️ Pela CLI** — o nome do statement é **automático** (o workspace gera); capture o
que estiver RUNNING:

**macOS / Linux**
```bash
STMT=$(confluent flink statement list -o json 2>/dev/null | grep -v "^No Flink" | python3 -c "import json,sys; t=sys.stdin.read().strip(); d=json.loads(t) if t else []; print(([s['name'] for s in d if s.get('status')=='RUNNING']+[''])[0])")
echo "STMT=$STMT"
```

**Windows (PowerShell)**
```powershell
$STMT = ((confluent flink statement list -o json 2>$null | Where-Object {$_ -notmatch 'No Flink'}) -join "`n" | ConvertFrom-Json | Where-Object status -eq "RUNNING" | Select-Object -First 1).name
echo "STMT=$STMT"
```

```bash
confluent flink statement stop $STMT
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
```
| Current CFU | 0              |
```

**O que observar**: o tópico `transacoes_grandes` **continua existindo** com os dados
já processados. O que parou foi o processo que o alimentava.

É a mesma economia do Curso 3: **dados são baratos, processo ligado não é**.

---

## Passo 5 — Fixe os conceitos *(~5 min)*

1. Por que `GROUP BY status` atualiza no lugar em vez de acumular linhas?
2. Você criou um CTAS e fechou o navegador. O que está acontecendo agora?
3. Deletou o tópico `transacoes_grandes`. O statement parou?
4. Como um CTAS difere de uma VIEW de banco?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque o resultado é uma tabela, não um fluxo de eventos.** "Quantas transações
estão SETTLED" tem **uma resposta por vez** — quando chega uma nova, a resposta anterior
fica obsoleta. O Flink emite uma *retração* da linha antiga e uma nova. É o modo
**updating**.

Já `WHERE amount > 500` é append: cada transação aprovada é um fato independente, que
não invalida os anteriores.

**2. O statement continua rodando** na infraestrutura da Confluent, consumindo CFU e
alimentando o tópico. Fechar o navegador não muda nada — este é o principal risco de
crédito do curso. Só `statement stop` encerra.

**3. Não.** São recursos independentes: o statement é o processo, o tópico é o destino.
Deletar o tópico com o statement rodando pode até fazê-lo recriar ou falhar. **Pare o
statement primeiro**, depois delete o tópico.

**4. Uma VIEW é calculada sob demanda**, quando alguém consulta. Um CTAS é
**materializado continuamente**: o resultado é gravado num tópico em tempo real, antes
de qualquer um pedir. É por isso que ele custa — há um processo trabalhando o tempo
todo.

</details>

---

## Se algo não funcionou

**`Object 'pg-payments.payments.transactions' not found`**
Faltam as crases. Nomes com ponto exigem `` ` `` em volta.

**A query `GROUP BY` fica muito lenta**
Agregação sem janela mantém estado de tudo. Com muitos dados, é pesado — e é
exatamente o problema que a janela resolve ([Aula 3](../aula3/README.md)).

**O tópico do CTAS não aparece**
Confirme que o statement está `RUNNING`, não `FAILED`:
```bash
confluent flink statement describe NOME | grep -i status
```

**Quero recriar o CTAS**
Pare e delete o statement antes; nomes de statement são únicos:
```bash
confluent flink statement stop NOME
confluent flink statement delete NOME --force
```

---

## Limpar

Pare **tudo** que estiver rodando (nomes de workspace são automáticos — o loop acha):

```bash
for st in $(confluent flink statement list -o json 2>/dev/null | grep -v "^No Flink" | python3 -c "import json,sys; t=sys.stdin.read().strip(); d=json.loads(t) if t else []; [print(s['name']) for s in d if s.get('status')=='RUNNING']"); do confluent flink statement stop $st; done
confluent flink statement list | grep RUNNING
```

- `confluent flink statement list | grep RUNNING` — vazio

O tópico `transacoes_grandes` pode ficar — é insumo da [Aula 3](../aula3/README.md).

---

## Glossário

| Termo | Definição |
|---|---|
| **Dualidade tabela/stream** | Tabela é stream parado; stream é tabela em movimento. |
| **Append** | Modo em que linhas se acumulam. |
| **Updating** | Modo em que valores são atualizados no lugar (com retração). |
| **CTAS** | `CREATE TABLE AS SELECT`: cria tópico **e** statement perpétuo. |
| **`$rowtime`** | Coluna de metadado com o timestamp do record Kafka. |
| **Retração** | Evento que invalida um resultado anterior num agregado. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Dar "fim" ao agregado infinito | [Aula 3](../aula3/README.md) — janelas |
| O estado por trás do updating | [Aula 4](../aula4/README.md) |
| Tópico derivado virando tabela Iceberg | Curso 5 — Tableflow |

**Fontes**

- Confluent — [Flink SQL queries](https://docs.confluent.io/cloud/current/flink/reference/queries/overview.html)
- Confluent — [Dynamic tables](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html)
- Apache Flink — [Table API & SQL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 1 · A query que não termina](../aula1/README.md) | [Curso 4](../README.md) | [Aula 3 · Janelas e joins](../aula3/README.md) |
