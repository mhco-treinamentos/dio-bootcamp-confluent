# Curso 4 · Aula 1 — Flink: a query que não termina

> **Guia do aluno** · Bloco 3 — Processamento de Streams
> ⏱ ~35 min · ☁️ **Statement rodando cobra** · UI + CLI

Até aqui os dados **passaram** pelo Kafka. A partir desta aula eles são **transformados** — com estado, janelas e joins. E a primeira coisa que você vai ver é uma query SQL que **nunca acaba** — e isso ser o comportamento correto.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito o [Curso 3](../../curso3/README.md) | Tópicos com dados reais |
| Navegador | O workspace SQL da UI é onde os resultados aparecem |
| 1 terminal | Para criar e destruir recursos |

```bash
confluent kafka cluster list
confluent kafka topic list
```

- `confluent kafka cluster list` — * e UP
- `confluent kafka topic list` — deve haver tópicos com dados

---

## 💸 A regra de custo do Flink — diferente de tudo até agora

Boa notícia primeiro:

> **Compute pool ocioso custa ZERO.** Você só paga por statement rodando.

| Recurso | Cobra? |
|---|---|
| Pool provisionado, 0 CFU | **Não** |
| Statement `RUNNING` | **Sim** — $0,21/CFU-hora, mínimo 1 CFU-minuto |

Um statement esquecido rodando: **~$5/dia**, **~$151/mês**.

Compare com o Curso 3: lá o conector cobrava por existir e você deletava tudo. Aqui:

> **O pool pode ficar. O statement, nunca.**

---

## O que você vai saber fazer ao final

- Criar um compute pool e entender por que ele não custa parado
- Rodar uma query contínua e explicar por que ela não termina
- Ver o medidor de CFU subir de 0 para 1
- Distinguir statement de sessão de statement perpétuo
- Parar statements — o teardown desta aula

---

## Os conceitos em 90 segundos

O Curso 1 mostrou consumidores; o Curso 3, conectores. Nenhum dos dois **lembra** de nada — cada mensagem é processada isolada. Flink adiciona **estado**:

| Pergunta | Precisa de estado? |
|---|---|
| "essa transação é > R$ 500?" | Não — só a mensagem |
| "quantas transações nesta conta na última hora?" | **Sim** — memória |
| "esse valor é 10× a média do cartão?" | **Sim** — histórico |
| "junte a transação com o cadastro da conta" | **Sim** — a outra tabela |

**A diferença fundamental**: num banco, `SELECT` percorre um conjunto **finito** e termina. Num stream, o conjunto **não acaba** — então a query vira um **processo vivo**.

> 📖 A anatomia do Flink (JobManager, TaskManagers, RocksDB) e a comparação com Kafka
> Streams e ksqlDB estão **na aula gravada**.

---

## Passo 1 — Crie o compute pool *(~6 min)*

**🖱 Pela Console**: no environment, aba **Flink** → **Create compute pool** → nome `bootcamp-flink`, região `us-east-1`, Max CFU **5** → **Create**. (O nome é livre — a captura do Passo 2 acha o pool seja qual for.)

**⌨️ Pela CLI**:

```bash
confluent flink compute-pool create bootcamp-flink \
  --cloud aws --region us-east-1 --max-cfu 5
```

> Use a **mesma região** do seu cluster Kafka.

**Output esperado**
```
| ID          | lfcp-57mk88n   |
| Name        | bootcamp-flink |
| Current CFU | 0              |
| Max CFU     | 5              |
| Status      | PROVISIONING   |
```

**O que observar** — a linha mais importante da aula:

**`Current CFU: 0`**. O pool não é um servidor ligado esperando trabalho. É um
**envelope elástico**: capacidade que só é alocada quando há statement rodando.

`Max CFU: 5` é o teto — a proteção contra uma query mal escrita consumir capacidade
sem limite.

Aguarde ficar `PROVISIONED`:

Capture o id do pool — pelo nome, sem editar nada:

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

```bash
confluent flink compute-pool describe $POOL | grep -E "Status|CFU"
```
```
| Current CFU | 0              |
| Max CFU     | 5              |
| Status      | PROVISIONED    |
```

**Pronto e cobrando zero.** Guarde esse número — você vai vê-lo mudar no Passo 3.

---

## Passo 2 — Abra o workspace SQL *(~5 min)*

> ⚠️ **Aqui o Curso 4 muda de estilo.** Nos Cursos 1 a 3 tudo era CLI. No Flink, o
> CLI cria e gerencia statements, mas **não mostra as linhas do resultado** — e o
> ponto desta aula é justamente *ver dados atualizando ao vivo*.
>
> Por isso: **UI para consultar, CLI para gerenciar**.

Na UI: **seu environment → Flink → Open SQL workspace** (escolha o pool que você criou).

Comece reconhecendo o terreno:

```sql
SHOW TABLES;
```

**O que observar**: seus **tópicos Kafka aparecem como tabelas** — sem que ninguém
tenha escrito um `CREATE TABLE`. O Flink lê o Schema Registry e monta a estrutura
sozinho. O contrato do Curso 2 virou schema SQL de graça.

---

## Passo 3 — A query que não termina *(~8 min)*

No workspace, rode:

```sql
SELECT * FROM `pg-payments.payments.transactions`;
```

**O que observar** — e este é o momento da aula:

1. Os resultados aparecem
2. **A query não termina.** Não há "3 rows returned". O indicador continua girando.
3. Se novos dados chegarem, eles **aparecem sozinhos** na tela

Num banco, isso seria um travamento. Aqui é o comportamento correto: **a tabela é
infinita**, então a query é um processo vivo.

O próprio Flink avisa. Ao criar um statement de agregação, aparece:

```
[Warning] Your query performs...
```

Agora veja o efeito no medidor, em outro terminal:

```bash
confluent flink compute-pool describe $POOL | grep "Current CFU"
```

**Output esperado**
```
| Current CFU | 1              |
```

**De 0 para 1.** O pool alocou capacidade porque há trabalho. Quando você parar o
statement, volta a 0 — e para de cobrar.

> 💡 **É isto que "serverless" significa aqui**: você não dimensionou máquina, não
> escolheu instância. Pediu uma query; a plataforma alocou 1 CFU.

### Um filtro contínuo

```sql
SELECT transaction_id, amount, status
FROM `pg-payments.payments.transactions`
WHERE amount > 500;
```

**O que observar**: a sintaxe é o SQL de sempre — `SELECT`, `WHERE`. O que mudou é o
**comportamento**: em vez de responder e encerrar, o filtro fica de plantão.

---

## Passo 4 — Statement é recurso, não comando *(~6 min)*

```bash
confluent flink statement list
```

**Output esperado**
```
  Creation Date | Name       | Statement                          | Compute Pool | Status
----------------+------------+------------------------------------+--------------+---------
  2026-08-20... | cnt-tx     | SELECT COUNT(*) AS total FROM ...  | lfcp-57mk88n | RUNNING
```

**O que observar**: `RUNNING`, não "executado". O statement tem **ciclo de vida** —
como o conector do Curso 3. Ele existe, consome recursos e precisa ser encerrado.

Há dois tipos, e a diferença é o que separa centavos de dezenas de dólares:

| Tipo | Exemplo | Vive enquanto |
|---|---|---|
| **Sessão** | `SELECT` no workspace | Você mantém a aba aberta |
| **Perpétuo** | `CREATE TABLE AS ...` / `INSERT INTO ...` | **Até você parar** — sobrevive a fechar o navegador |

O perpétuo é o assunto da [Aula 2](../aula2/README.md) — e o principal risco de crédito
do curso.

---

## Passo 5 — Teardown *(~5 min)*

**🖱 Pela Console**: aba **Flink** → **Statements**: pare os que estiverem `Running`; o pool pode ficar (ocioso não cobra) ou ser removido em ⋮ → **Delete**.

**⌨️ Pela CLI**:

```bash
confluent flink statement stop NOME-DO-STATEMENT
```

Confirme que nada ficou:

```bash
confluent flink statement list | grep RUNNING
```

Sem saída = nenhum statement rodando. Verifique o medidor:

```bash
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
```
| Current CFU | 0              |
```

**De volta a zero — de volta a custo zero.**

> 💡 **O pool pode ficar.** Diferente do conector do Curso 3, um pool em 0 CFU não
> cobra. Você o reaproveita nas Aulas 2 a 4. O que **nunca** pode ficar é statement
> rodando.

Se quiser encerrar de vez:
```bash
confluent flink compute-pool delete $POOL --force
```

---

## Passo 6 — Fixe os conceitos *(~5 min)*

1. Por que a query `SELECT * FROM transactions` não termina?
2. Você fechou o navegador. O statement `CREATE TABLE ... AS SELECT` parou?
3. Seu pool está em 0 CFU há três dias. Quanto custou?
4. Quando usar Flink em vez de um consumidor Python?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Porque a tabela é infinita.** Ela mapeia um tópico Kafka, que não tem fim — novas
mensagens podem chegar a qualquer momento. Terminar a query significaria decidir
arbitrariamente que "acabou". Para ter um fim, é preciso **criar** um: é o que a
janela faz ([Aula 3](../aula3/README.md)).

**2. Não.** Statement perpétuo roda na infraestrutura da Confluent, não no navegador.
Fechar a aba não para nada — e é assim que se esquece um statement queimando ~$151/mês.
Só `confluent flink statement stop` encerra.

**3. Zero.** Pool ocioso não cobra. Só statement rodando consome CFU. É por isso que a
regra do Curso 4 é "o pool pode ficar, o statement não" — diferente do Curso 3, onde o
conector cobrava por existir.

**4. Quando precisar de estado.** Um consumidor Python processa mensagem a mensagem —
serve para filtrar e transformar. No momento em que a lógica exige **memória** (média
das últimas 24h, contagem por janela, join com outro fluxo), você teria que construir
sozinho o que o Flink já resolve: estado distribuído, checkpoint, recuperação,
reprocessamento ordenado.

</details>

---

## Se algo não funcionou

**`No Flink endpoint is specified`**
Aviso inofensivo — o CLI usa o endpoint público da região.

**O statement fica `PENDING` por muito tempo**
O pool ainda está provisionando. Confira com `compute-pool describe`.

**`confluent flink shell` dá "failed to initialize console parser"**
O shell interativo exige terminal TTY. Use o **workspace SQL da UI** para consultas e
o CLI para gerenciar statements.

**O CLI não mostra os resultados da query**
Comportamento esperado: `statement create` devolve o status, não as linhas. Para ver
dados, use a UI.

**Não sei se algo está rodando**
```bash
confluent flink statement list | grep RUNNING
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
CFU em 0 = nada consumindo.

---

## Glossário

| Termo | Definição |
|---|---|
| **Compute pool** | Envelope elástico de capacidade; em 0 CFU não custa. |
| **CFU** | Confluent Flink Unit — a unidade de cobrança ($0,21/hora). |
| **Statement** | Uma query como **recurso com ciclo de vida**, não comando pontual. |
| **Statement de sessão** | Vive enquanto a aba do workspace estiver aberta. |
| **Statement perpétuo** | `CTAS`/`INSERT INTO` — roda até ser parado. |
| **`Max CFU`** | Teto de capacidade do pool; a proteção contra query descontrolada. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Tabela × stream e CTAS | [Aula 2](../aula2/README.md) |
| Como dar "fim" a um stream infinito | [Aula 3](../aula3/README.md) — janelas |
| O estado por dentro, checkpoints | [Aula 4](../aula4/README.md) |
| Flink alimentando o Lakehouse | Curso 5 |

**Fontes**

- Confluent — [Flink on Confluent Cloud](https://docs.confluent.io/cloud/current/flink/overview.html)
- Confluent — [Flink billing](https://docs.confluent.io/cloud/current/flink/concepts/flink-billing.html)
- Apache Flink — [Concepts](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Curso 3 · Aula 4](../../curso3/aula4/README.md) | [Curso 4](../README.md) | [Aula 2 · Tables e Streams](../aula2/README.md) |
