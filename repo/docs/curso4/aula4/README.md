# Curso 4 · Aula 4 — Flink em produção: estado, checkpoints e recuperação

> **Guia do aluno** · Bloco 3 — Processamento de Streams
> ⏱ ~35 min · ☁️ **Statement perpétuo cobra** · UI + CLI

Um statement acumula estado — contagens, janelas abertas, o lado de um join. Esta aula responde: **o que acontece com isso quando algo falha?** E fecha o curso com o teardown completo.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito as Aulas [1](../aula1/README.md) a [3](../aula3/README.md) | Pool, CTAS e janelas |
| Um tópico append-only com fluxo | O mesmo da Aula 3 |
| Workspace SQL + 1 terminal | |

---

## O que você vai saber fazer ao final

- Identificar onde vive o estado de um statement
- Parar e retomar um statement, vendo o estado sobreviver
- Ler CFU consumido e diagnosticar um statement problemático
- Explicar o que a plataforma faz por você (e o que continua sendo seu)
- Executar o teardown completo do curso

---

## Os conceitos em 90 segundos

**O estado é o tesouro acumulado.** Uma contagem por janela de 10 minutos guarda os
parciais; um join guarda os dois lados; um dedup guarda as chaves já vistas.

Perder isso significa recomeçar do zero — ou pior, emitir resultado errado.

**O checkpoint** é a foto consistente: o estado **mais** os offsets do Kafka, salvos
juntos. Ao falhar, o Flink restaura os dois em conjunto:

```
   estado (contagens, janelas)  ─┐
                                  ├─► CHECKPOINT ─► recuperação consistente
   offsets do Kafka             ─┘
```

Salvá-los **juntos** é o que garante que não haja duplicata nem buraco: o estado
restaurado corresponde exatamente à posição de leitura restaurada.

> 📖 A anatomia do checkpoint (barreiras, alinhamento) e o exactly-once de ponta a
> ponta estão **na aula gravada**.

---

## Passo 1 — Um statement com estado *(~8 min)*

Crie uma agregação com janela larga, para dar tempo de acumular:

```sql
CREATE TABLE volume_10min AS
SELECT window_start, window_end, COUNT(*) AS pedidos, SUM(total) AS valor_total
FROM TABLE(
  TUMBLE(TABLE pedidos_sink, DESCRIPTOR(`$rowtime`), INTERVAL '10' MINUTE))
GROUP BY window_start, window_end;
```

```bash
confluent flink statement list | grep volume
```
```
| volume_10min | CREATE TABLE volume_10min AS ... | lfcp-xxxxx | RUNNING
```

**Onde está o tesouro**: enquanto a janela de 10 minutos não fecha, o Flink mantém em
memória (e no state backend) os parciais — contagem e soma acumuladas. Se o processo
morresse agora sem checkpoint, esses parciais sumiriam.

Veja o consumo:

```bash
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
```
| Current CFU | 1              |
```

---

## Passo 2 — A falha simulada *(~8 min)*

Pare o statement — o nome é o **gerado pelo workspace** (capture o RUNNING):

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
```

Aguarde alguns segundos e retome:

```bash
confluent flink statement resume volume_10min
```

> Se o seu CLI não tiver `resume`, use a UI: **Flink → Statements →** o statement **→
> Resume**.

**O que observar** — e este é o ponto da aula:

Ao voltar, o statement **continua de onde estava**. Ele não reprocessa o tópico
inteiro nem perde os parciais da janela aberta. O checkpoint restaurou estado e
offsets juntos.

**E você nunca configurou checkpoint nenhum.**

Compare com o que isso exigiria em Flink self-managed:

| Configuração | Self-managed | Confluent Cloud |
|---|---|---|
| Intervalo de checkpoint | você define | **plataforma** |
| Storage do checkpoint (S3/HDFS) | você provisiona | **plataforma** |
| State backend (RocksDB, tuning) | você configura | **plataforma** |
| Estratégia de restart | você escolhe | **plataforma** |
| Savepoint antes de upgrade | você executa | **plataforma** |

> 💡 É a **fronteira de gestão** do Curso 2 aparecendo pela última vez. Aqui ela é
> especialmente valiosa: checkpoint mal configurado é uma das causas mais comuns de
> perda de dados em Flink self-managed.

---

## Passo 3 — O statement como recurso observável *(~7 min)*

```bash
confluent flink statement describe $STMT
```

**O que observar**:

| Campo | Para que serve |
|---|---|
| `Status` | RUNNING / FAILED / STOPPED |
| `Status Detail` | **A mensagem de erro** — leia sempre em caso de falha |
| `Compute Pool` | Onde roda |
| `Latest Offsets` | Até onde leu |

Na UI (**Flink → Statements**) você vê ainda:

- **CFUs consumidos** ao longo do tempo
- O **autoscaling** do pool — o gráfico subindo conforme o volume
- Mensagens de exceção com stack trace

**O sinal de alerta**: CFU subindo sem que o volume de dados tenha subido. Quase sempre
significa **estado crescendo sem limite** — join regular sem TTL ou agregação sem
janela (Aula 2).

A correção:
```sql
SET 'sql.state-ttl' = '1 HOURS';
```

---

## Passo 4 — O que continua sendo seu *(~5 min)*

A plataforma cuida da mecânica. **Quatro decisões continuam suas** — e são as que
causam incidente:

| Decisão | Consequência de errar |
|---|---|
| **Watermark** (Aula 3) | Muito curta descarta eventos; muito longa atrasa tudo |
| **Chave de partição** | Chave errada = ordem errada (Curso 1, Aula 5) |
| **Limite de estado** (TTL, janela) | Estado infinito = CFU infinito |
| **Parar o statement** | Esquecido = **~$151/mês** |

> A plataforma garante que seu statement **não perca dados**. Ela não garante que ele
> **calcule a coisa certa** — nem que você lembre de desligá-lo.

---

## Passo 5 — Teardown completo do curso *(~7 min)*

Este é o fechamento do Curso 4. Ordem importa.

### 1. Pare todos os statements

**🖱 Pela Console**: **Flink → Statements** (parar/deletar todos) e **Flink → Compute pools** → ⋮ → **Delete** no pool. Confira depois pela CLI — a prova de teardown é grep, não tela:

**⌨️ Pela CLI**:

```bash
confluent flink statement list
```

Para cada um em `RUNNING`:
```bash
confluent flink statement stop NOME
```

Confirme:
```bash
confluent flink statement list | grep RUNNING
```

- `confluent flink statement list | grep RUNNING` — sem saída

### 2. Confirme o pool em zero

```bash
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
```
| Current CFU | 0              |
```

**Zero CFU = zero custo.** O pool pode ficar.

### 3. Pare o que alimenta os tópicos

Se você deixou um conector rodando (CDC ou Datagen do Curso 3):

Capture o pool e delete **todos** os conectores que houver — um loop resolve:

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
confluent connect cluster list
for c in $(confluent connect cluster list -o json | python3 -c "import json,sys; [print(x['id']) for x in json.load(sys.stdin)]"); do confluent connect cluster delete $c --force; done
```

> 💸 Lembre da tabela de custos: o **CDC custa $0,50/task-hora** — mais caro que
> qualquer statement Flink.

### 4. Confira o billing

```bash
confluent billing cost list \
  --start-date $(date -v-1d +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

### O que fica

| Recurso | Fica? | Por quê |
|---|---|---|
| Compute pool (0 CFU) | ✅ | Não custa |
| `transacoes_enriquecidas` | ✅ | Insumo dos Cursos 5 e 6 |
| Statements | ❌ | Custam por hora |
| Conectores | ❌ | Custam por task-hora |

Quer zerar de vez?
```bash
confluent flink compute-pool delete $POOL --force
```

---

## Passo 6 — Fixe os conceitos *(~5 min)*

1. Você parou e retomou um statement. Por que ele não reprocessou tudo?
2. Por que estado e offsets precisam ser salvos **juntos**?
3. O CFU do pool subiu de 1 para 4 sem aumento de volume. O que investigar?
4. O que a plataforma **não** faz por você?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Por causa do checkpoint.** Ele guarda estado e offsets periodicamente. Ao retomar,
o Flink restaura ambos e continua do ponto salvo — nem do zero, nem do fim.

**2. Porque salvá-los separados criaria inconsistência.** Se o offset avançasse sem o
estado, os eventos entre um e outro seriam perdidos. Se o estado avançasse sem o
offset, seriam contados duas vezes. Salvos juntos, a foto é coerente: *"processei até
o offset X, e este é o estado resultante"*.

**3. Estado crescendo sem limite.** As duas causas comuns: **join regular** entre
streams infinitos (guarda os dois lados para sempre) ou **agregação sem janela** (a
Aula 2). Investigue com `describe`, e corrija com `sql.state-ttl` ou trocando por uma
janela.

**4. Quatro coisas** — todas de design, não de infraestrutura:
- **Watermark certa** (Aula 3): a plataforma não sabe quanto atraso seu negócio tolera
- **Chave de partição certa**: ela não sabe qual ordem você precisa
- **Limitar estado**: um join sem TTL é decisão sua
- **Desligar o statement**: ninguém desliga por você — e é o erro mais caro

</details>

---

## Se algo não funcionou

**`resume` não existe no meu CLI**
Use a UI: **Flink → Statements →** selecione **→ Resume**. Ou recrie o statement.

**O statement volta como FAILED**
`confluent flink statement describe NOME` e leia o `Status Detail` — a mensagem quase
sempre diz exatamente o que fazer (como fez na Aula 3, com o CDC).

**O CFU não volta a zero**
Ainda há statement rodando:
```bash
confluent flink statement list | grep RUNNING
```

**Não sei se sobrou algo ligado**
```bash
confluent flink statement list | grep RUNNING
confluent connect cluster list
confluent flink compute-pool describe $POOL | grep "Current CFU"
```
As três vazias/zeradas = nada consumindo.

---

## Glossário

| Termo | Definição |
|---|---|
| **Estado** | O que o statement acumula: parciais, janelas, lados de join. |
| **Checkpoint** | Foto consistente de estado **+** offsets. |
| **State backend** | Onde o estado vive (RocksDB) — gerenciado no Cloud. |
| **`sql.state-ttl`** | Tempo de vida do estado; a defesa contra crescimento infinito. |
| **Savepoint** | Checkpoint manual, para upgrade — gerenciado no Cloud. |

---

## Você terminou o Curso 4 🎉

| Aula | O que você levou |
|---|---|
| 1 | A query que não termina — e o CFU como medidor |
| 2 | Tabela e stream são a mesma coisa; CTAS cria dois recursos |
| 3 | Janela cria o "fim"; CDC não janela; temporal join enriquece |
| 4 | Estado sobrevive a falhas — sem você configurar nada |

**No Curso 5**, esses tópicos enriquecidos viram **tabelas Iceberg** com Tableflow —
sem conector, sem pipeline batch.

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Enriquecido → Lakehouse | Curso 5 — Tableflow |
| Features para modelo | Curso 6 — `ML_PREDICT` |
| Custo e dimensionamento | Curso 7 |
| `MATCH_RECOGNIZE` (CEP) | Desafio Final |

**Fontes**

- Confluent — [Flink statements](https://docs.confluent.io/cloud/current/flink/concepts/statements.html)
- Apache Flink — [Stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
- Confluent — [Flink billing](https://docs.confluent.io/cloud/current/flink/concepts/flink-billing.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 3 · Janelas e joins](../aula3/README.md) | [Curso 4](../README.md) | Curso 5 · Lakehouse *(em breve)* |
