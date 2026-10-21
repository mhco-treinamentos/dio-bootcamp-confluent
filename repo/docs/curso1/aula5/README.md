# Curso 1 · Aula 5 — Ordem por chave, idempotência e semânticas de entrega

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~45 min · 🐍 **Python** (`confluent-kafka`) · 1 terminal

Esta aula fecha a mecânica do Kafka com as duas perguntas que todo pipeline precisa
responder: *"minhas mensagens chegam na ordem certa?"* e *"o que acontece quando a
mesma mensagem chega duas vezes?"*

Você vai **quebrar a ordem de propósito**, **matar um consumidor no meio do lote** —
duas vezes, com uma linha de diferença — e depois **tornar a duplicata inofensiva**.
Tudo em código.

> 💻 Prefere a linha de comando? As cenas de ordem e duplicata também existem em versão
> CLI → [cli.md](cli.md).

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 4](../aula4/README.md) | Producer API, consumer groups e commit são a base |
| Docker rodando | Docker Desktop aberto |
| O venv instalado | `make install` (uma vez) |
| 1 terminal | |

**macOS / Linux**
```bash
make up
make status
```

**Windows (PowerShell)** — sem `make`, use o `docker compose` direto:
```powershell
docker compose -f deployment/docker/docker-compose.yml up -d
docker compose -f deployment/docker/docker-compose.yml ps
```

O arquivo da aula — feito para **ler tanto quanto rodar**:
[aula5_api_entrega.py](../../../demos/curso1/aula5_api_entrega.py). Pelo **PyCharm**,
as configs `Aula 5 API · *` rodam cada cena.

---

## O que você vai saber fazer ao final

- Provar que a ordem é garantida **por chave** — pelo delivery report, sem consumir
- Mostrar o que quebra quando a chave é omitida
- Provocar perda e duplicata com o MESMO crash — mudando só a posição do commit
- Explicar por que a duplicata **não é um bug**, e sim o contrato at-least-once
- Tornar a duplicata inofensiva com um consumidor idempotente

---

## Passo 1 — Ordem por chave *(~7 min)*

**Objetivo**: provar que eventos da mesma transação caem na mesma partição — na ordem.

A cena produz o ciclo de vida de **3 transações intercaladas** (como num sistema real,
onde várias correm em paralelo), e o delivery report entrega a prova:

```python
producer.produce(
    topic=topic,
    key=key.encode(),          # a chave decide a partição...
    value=value.encode(),
    on_delivery=report,        # ...e o callback mostra qual foi
)
```

```bash
make aula5-api-ordem
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega ordem`

**Output esperado**
```
Delivery reports — repare na coluna da partição:
  chave   txn-1001 → Partition:2  CREATED 150.00
  chave   txn-1001 → Partition:2  AUTHORIZED 150.00
  chave   txn-1001 → Partition:2  SETTLED 150.00
  chave   txn-1002 → Partition:0  CREATED 89.90
  chave   txn-1003 → Partition:0  CREATED 1250.00
  chave   txn-1002 → Partition:0  AUTHORIZED 89.90
  chave   txn-1003 → Partition:0  DECLINED 1250.00
  chave   txn-1002 → Partition:0  SETTLED 89.90

A lição: a MESMA chave caiu SEMPRE na mesma partição — o ciclo
CREATED → AUTHORIZED → SETTLED preserva a ordem.
```

**O que observar**: as mensagens foram produzidas **intercaladas** (olhe a ordem das
linhas), mas a `txn-1001` viveu inteira na partição 2, na sequência exata do negócio.
E `txn-1002`/`txn-1003` dividem a partição 0 — intercaladas entre si, cada uma
preservando **sua própria** ordem. Ordem é garantida por chave, não entre chaves.

> 💡 O mecanismo: `partição = hash(chave) % número_de_partições`. Mesma chave → sempre
> a mesma partição → e dentro da partição a ordem é a de escrita (Aula 3).

---

## Passo 2 — Quebre a ordem de propósito *(~5 min)*

**Objetivo**: ver o que acontece quando ninguém define a chave.

```bash
make aula5-api-sem-chave
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega ordem --sem-chave`

**Output esperado**
```
Delivery reports — repare na coluna da partição:
  chave     (nula) → Partition:0  CREATED 150.00
  chave     (nula) → Partition:2  AUTHORIZED 150.00
  chave     (nula) → Partition:1  SETTLED 150.00
  ...
```

**O que observar** — e este é o momento em que a aula "dói": siga o valor `150.00` (a
txn-1001). **Uma fase em cada partição.** Um consumidor lendo partição por partição
pode ver o `SETTLED` antes do `CREATED` — uma transação liquidada antes de existir. Em
pagamentos, isso é bug de produção com consequência financeira.

> ⚠️ **A lição**: a ordem não é um presente do Kafka — é uma consequência da chave que
> você escolheu. Sem chave, não há garantia nenhuma entre partições.

---

## Passo 3 — A duplicata do at-least-once *(~8 min)*

**Objetivo**: ver a mesma mensagem sendo processada duas vezes — e entender por quê.

A cena consome um lote de 8 mensagens (partição única, offsets 0–7) commitando
**depois** de processar — e **morre de propósito no offset 3**, entre o processamento
e o commit:

```python
process(msg)                      # processa ANTES...
# 💥 morre AQUI
consumer.commit(message=msg)      # ...de confirmar
```

Rode **o mesmo comando duas vezes**:

```bash
make aula5-api-crash-depois
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega crash --commit depois`

**Output esperado — 1ª execução**
```
grupo 'liquidacao-api-depois' — 1ª execução: o crash vai acontecer no offset 3
  processado Offset:0  txn-1001  CREATED 150.00
  processado Offset:1  txn-1002  CREATED 89.90
  processado Offset:2  txn-1001  AUTHORIZED 150.00
  processado Offset:3  txn-1003  CREATED 1250.00
💥 crash SIMULADO — offset 3 processado, NÃO commitado
```

**Output esperado — 2ª execução**
```
grupo 'liquidacao-api-depois' — 2ª execução: retomando do offset commitado (3)
  processado Offset:3  txn-1003  CREATED 1250.00     ← DE NOVO
  processado Offset:4  txn-1002  AUTHORIZED 89.90
  ...
  ✓ o offset 3 apareceu nas DUAS execuções: processado, morto antes
    do commit, reentregue → DUPLICADO
```

**O que observar**: o offset 3 foi processado **nas duas execuções**. Se cada
processamento debitasse uma conta, você teria cobrado o cliente duas vezes.

> **Isto não é um bug.** É o contrato **at-least-once** sendo cumprido: *"não perder
> nunca, ao custo de repetir às vezes"*. O Kafka preferiu entregar duas vezes a
> arriscar não entregar.

---

## Passo 4 — Por que não existe "exactly-once" fácil *(~8 min)*

A pergunta óbvia: *por que o Kafka não entrega exatamente uma vez?*

Porque entre "processei" e "confirmei" existe um instante — e o processo pode morrer
ali. Não importa a ordem que você escolha:

| Ordem | Se morrer no meio | Resultado |
|---|---|---|
| Commita → processa | Confirmado mas não processado | **Perdeu** (at-most-once) |
| Processa → commita | Processado mas não confirmado | **Repetiu** (at-least-once) |

O Passo 3 provou a linha de baixo. **Prove a de cima** — o mesmo crash, com o commit
do outro lado:

```python
consumer.commit(message=msg)      # commita ANTES...
# 💥 morre AQUI
process(msg)                      # ...de processar
```

```bash
make aula5-api-crash-antes
```

Rode **duas vezes**, como no Passo 3.

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega crash --commit antes`

**Output esperado (as duas execuções, resumidas)**
```
1ª execução:  processado Offset:0, 1, 2
              💥 crash SIMULADO — offset 3 commitado, NÃO processado

2ª execução:  retomando do offset commitado (4)
              processado Offset:4, 5, 6, 7
  ✗ o offset 3 NÃO aparece em execução nenhuma: foi commitado e morreu
    antes do efeito → PERDIDO
```

**O que observar** — o destino do offset 3 (`txn-1003 CREATED`), lado a lado:

| Modo | Offset 3 | Semântica |
|---|---|---|
| commit **antes** | não aparece em execução nenhuma — **perdido** | at-most-once |
| commit **depois** | aparece nas duas execuções — **duplicado** | at-least-once |

A diferença inteira entre perder e duplicar é **a posição de uma linha de código**. Em
pagamentos, duplicar (e deduplicar no destino) é recuperável; perder um `CREATED` não
é. Por isso o padrão é at-least-once.

Não existe terceira opção — a menos que processamento e commit sejam **a mesma
transação atômica** (é o que o Kafka Transactions faz, com custo, e é assunto do
Curso 7). A saída prática, que a indústria adotou: aceitar o at-least-once e tornar o
**efeito** idempotente. É o Passo 5.

> 💡 O crash só dispara na 1ª execução: o script detecta que o grupo nunca commitou
> (`committed < 0`). A 2ª execução é a "recuperação" — e é nela que o veredito aparece.
> Para repetir a cena do zero, `make aula5-api-teardown` (remove tópicos **e** grupos).

---

## Passo 5 — A duplicata inofensiva *(~7 min)*

**Objetivo**: ver o dedup funcionando — a releitura acontece igual, o efeito não.

Dedup exige **estado** ("o que eu já processei?"). O código, em três linhas:

```python
if event_id in seen:               # já processei este evento?
    continue                       # então ignora
seen.add(event_id)                 # senão, marca e processa
```

```bash
make aula5-api-dedup
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega dedup`

**Output esperado**
```
1ª passada — processa e 'morre' antes do commit:
  processado txn-1001:CREATED 150.00
  ...
▸ leu 8 · aplicou 8 · ignorou 0 duplicata(s)

2ª passada — o MESMO lote é reentregue:
  duplicata ignorada: txn-1001:CREATED 150.00
  ...
▸ leu 8 · aplicou 0 · ignorou 8 duplicata(s)
▸ a releitura aconteceu igual — mas o efeito não se repetiu
```

**O que observar** — a linha que resume a aula inteira:

```
leu 8 · aplicou 0 · ignorou 8
```

A releitura **aconteceu igual** ao Passo 3 — o Kafka reentregou tudo. Mas o efeito não
se repetiu, porque o consumidor reconheceu os eventos pela **identidade de negócio**
(chave + conteúdo), não pelo offset.

> 💡 Em produção, o `seen` não é um set em memória — é o **próprio destino** sendo
> idempotente: um `INSERT ... ON CONFLICT (id) DO UPDATE` no banco, ou o
> `insert.mode=upsert` de um sink connector (Curso 3, Aula 3). O princípio é o mesmo:
> **N entregas, 1 efeito.**

---

## Passo 6 — Fixe os conceitos *(~5 min)*

1. Você precisa garantir que os eventos de uma **conta** cheguem em ordem. Qual chave usar?
2. Um consumidor commitou o offset e morreu antes de processar. Perdeu ou repetiu?
3. Por que aumentar o número de partições de um tópico existente pode quebrar a ordem?
4. Seu consumidor grava num banco. Como torná-lo idempotente sem manter um set em memória?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. `account_id`.** A regra é: **a chave é aquilo cuja ordem você precisa garantir.**
Se o requisito é ordem por conta, a chave é a conta; se é por transação, é
`transaction_id`. Não existe chave "certa" no abstrato — existe a chave que atende ao
requisito de ordenação do seu negócio.

**2. Perdeu.** Commitar antes de processar é **at-most-once**: o Kafka acredita que a
mensagem foi tratada e nunca mais a entrega. Foi exatamente o `crash --commit antes`
do Passo 4 — o offset 3 que não aparece em execução nenhuma.

**3. Porque `partição = hash(chave) % N`.** Mudando o `N`, a mesma chave passa a cair
em outra partição. Os eventos antigos de `txn-1001` continuam na partição velha,
enquanto os novos vão para outra — e a ordem entre partições não existe. Por isso o
número de partições é uma decisão que se toma **cedo** (Aula 3).

**4. Deixando o banco resolver.** Um `INSERT ... ON CONFLICT (transaction_id) DO
UPDATE` (Postgres) aplica o mesmo evento N vezes com o mesmo resultado final. O estado
de "o que já processei" fica no destino, não na memória do consumidor — que pode
morrer e reiniciar sem perder a proteção. É exatamente o que o `insert.mode=upsert` de
um sink connector faz (Curso 3).

</details>

---

## Se algo não funcionou

**`ModuleNotFoundError`**
Venv não instalado — `make install`. Rode pelos alvos `make` ou pelo ▶ do PyCharm.

**O crash não dispara ("2ª execução" logo de cara)**
O grupo já tem offset commitado de uma passada anterior. `make aula5-api-teardown`
(remove tópicos **e** grupos) e rode de novo.

**`Connection refused` / `broker transport failure`**
Sandbox fora do ar — `make up`.

**Na cena `ordem`, as partições diferem das do guia**
Se você mudou as chaves, o hash muda — o que importa é o **padrão**: mesma chave,
mesma partição. Com as chaves do script, a distribuição é idêntica à do guia.

**`Failed to acquire idempotence PID ... retrying`**
Broker recém-subido carregando o coordinator; o retry resolve sozinho.

---

## Limpar

```bash
make aula5-api-teardown
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_api_entrega teardown`

Remove os 3 tópicos da aula **e os consumer groups** (sem isso, as cenas de crash
achariam que a "1ª execução" já aconteceu).

Vai parar por aqui? `make down` (Windows:
`docker compose -f deployment/docker/docker-compose.yml down -v`).

---

## Bônus

**A versão linha de comando** → [cli.md](cli.md): ordem por chave e ordem quebrada com
`kafka-console-producer`, e a duplicata provocada com `--reset-offsets`.

**As cenas empacotadas** — a versão com output colorido para gravação:

```bash
make aula5-setup
make aula5-cena1
make aula5-cena2
make aula5-cena3
make aula5-teardown
```

| Alvo | O que faz |
|---|---|
| `aula5-cena1` | ordem por chave (produz + mostra a ordem por partição) |
| `aula5-cena2` | at-least-once cru — **rode duas vezes** para ver a duplicata |
| `aula5-cena3` | consumidor idempotente |

> 🪟 No Windows, troque `make aula5-<alvo>` por
> `.venv\Scripts\python -m demos.curso1.aula5_ordem_e_idempotencia <subcomando>`
> — subcomandos: `setup`, `produce`, `ordem` (cena1 roda os dois), `crash` (cena2),
> `idempotente` (cena3), `teardown`.

---

## Glossário

| Termo | Definição |
|---|---|
| **Ordem por chave** | Garantia de que mensagens com a mesma chave são lidas na ordem de escrita. |
| **at-most-once** | Commita antes de processar: sem duplicata, com risco de perda. |
| **at-least-once** | Processa antes de commitar: sem perda, com risco de duplicata. **O padrão.** |
| **exactly-once** | Efeito único; exige transação atômica ou idempotência no destino. |
| **Idempotência** | Aplicar N vezes produz o mesmo resultado que aplicar uma vez. |
| **Identidade de negócio** | O id que o dedup usa — do domínio (`event_id`), não do transporte (offset). |
| **Upsert** | Escrita idempotente por chave: insere se novo, atualiza se existente. |
| **Producer idempotente** | `enable.idempotence=true`: o retry do próprio producer não duplica no broker. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Upsert como idempotência | Curso 3, Aula 3 — sink connectors |
| Chave de partição em CDC | Curso 3, Aula 2 — a PK vira a key do evento |
| Dedup em stream | Curso 4 — Flink |
| Mensagem que falha de verdade | Curso 3, Aula 4 — Dead Letter Topic |
| Exactly-once com transações | Curso 7 |

**Fontes**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.*, cap. 7 — [e-book gratuito](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- Confluent — [Exactly-once semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- Apache Kafka — [Producer Configs: `enable.idempotence`](https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 4 · Produtores e consumidores](../aula4/README.md) | [Curso 1](../README.md) | [Aula 6 · Do monólito ao EDA](../aula6/README.md) |
