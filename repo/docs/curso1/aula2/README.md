# Curso 1 · Aula 2 — Message Queue vs Data Streaming

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~35 min · 💻 Mão na massa · precisa de **3 terminais**

Aqui você digita os comandos do Kafka na mão — nada de script escondendo o que acontece. Ao final, a tese da aula estará provada na sua tela: **mesmo tópico, mesmas mensagens; muda só o `--group` e a semântica muda junto.**

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 1](../aula1/README.md) | O vocabulário de evento/mensagem é assumido |
| Docker rodando | Docker Desktop aberto (no Windows: backend WSL 2) |
| 3 terminais | Na raiz de `repo/` |

> 🪟 **Windows**: os comandos deste guia aparecem em dupla — a versão macOS/Linux
> quebra linhas com `\`; a versão PowerShell vem em **uma linha só** (mais seguro que
> a crase `` ` ``, que falha se houver um espaço depois dela). Comandos de uma linha
> são **idênticos** nos dois sistemas. Se você usa **Git Bash** ou **WSL**, siga as
> versões macOS/Linux sem mudar nada.

**macOS / Linux**
```bash
make up
make status
```

`make up` sobe o Kafka (~30s) — é o único atalho da aula; `make status` confirma
`bootcamp-kafka Up`.

**Windows (PowerShell)** — sem `make`, use o `docker compose` direto:
```powershell
docker compose -f deployment/docker/docker-compose.yml up -d
docker compose -f deployment/docker/docker-compose.yml ps    # confirme: bootcamp-kafka Up
```

> ⚠️ Sandbox de **um broker**: RF=1, sem ISR. As garantias de replicação do bootcamp
> (RF=3, `min.insync.replicas=2`) só aparecem no Confluent Cloud, a partir do Curso 2.

### O atalho que você vai usar o tempo todo

Todos os comandos rodam **dentro do container**, então começam com `docker exec bootcamp-kafka`. Para não repetir isso 20 vezes, crie um apelido no terminal:

**macOS / Linux**
```bash
alias kafka='docker exec bootcamp-kafka'
```

**Windows (PowerShell)**
```powershell
function kafka { docker exec bootcamp-kafka @args }
```

Assim `kafka kafka-topics --list --bootstrap-server localhost:9092` faz o que você espera — nos dois sistemas. Os comandos abaixo estão escritos por extenso; use o atalho se preferir.

---

## O que você vai saber fazer ao final

- Criar e inspecionar um tópico pela linha de comando
- Produzir e consumir mensagens sem escrever uma linha de código
- Provar que dois consumidores no **mesmo** grupo dividem o trabalho
- Provar que em grupos **diferentes** os dois recebem tudo
- Fazer replay do histórico e explicar por que numa fila isso seria impossível

---

## Passo 1 — Criar o tópico *(~4 min)*

**Objetivo**: criar o tópico explicitamente e ver o que o Kafka guarda sobre ele.

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-topics \
  --create --topic pagamentos \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-topics --create --topic pagamentos --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

**Output esperado**
```
Created topic pagamentos.
```

Agora inspecione o que foi criado:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-topics \
  --describe --topic pagamentos \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-topics --describe --topic pagamentos --bootstrap-server localhost:9092
```

**Output esperado**
```
Topic: pagamentos	TopicId: 7i40jexUScKiWwcfwFV5Mw	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1048576
	Topic: pagamentos	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: pagamentos	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: pagamentos	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

**O que observar**: o tópico não é uma coisa só — são **3 partições**, cada uma com
seu líder. `Replicas: 1` e `Isr: 1` é o sandbox de um broker; em produção você veria
`Replicas: 1,2,3`.

> O `TopicId` é gerado pelo cluster — o seu vai ser diferente. Idem para os IDs de
> transação mais adiante.

**Validação**: `docker exec bootcamp-kafka kafka-topics --list --bootstrap-server localhost:9092` mostra `pagamentos`.

---

## Passo 2 — Produzir e consumir na mão *(~6 min)*

**Objetivo**: mandar mensagens e lê-las de volta, sem código.

### Produza

Um comando só — as 6 mensagens vão no formato `chave:valor`, uma por linha:

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic pagamentos \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-1001:AUTHORIZED
txn-1002:AUTHORIZED
txn-1003:DECLINED
txn-1004:SETTLED
txn-1005:AUTHORIZED
txn-1006:DECLINED
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-1001:AUTHORIZED
txn-1002:AUTHORIZED
txn-1003:DECLINED
txn-1004:SETTLED
txn-1005:AUTHORIZED
txn-1006:DECLINED
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic pagamentos --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

O comando termina sozinho quando as mensagens acabam — sem Ctrl+D, sem digitação.

> ⚠️ Repare no **`-i` sem `-t`**: com dados vindo por pipe/heredoc não há terminal
> interativo, e o `-t` causaria `the input device is not a TTY`. No PowerShell, o
> `@'` precisa fechar a linha e o `'@` precisa **começar** a linha — é a regra da
> here-string.

> 💡 **Quer o modo interativo** (digitar mensagem a mensagem, bom para apresentar ao
> vivo)? Troque `-i` por `-it`, remova o bloco `<<'EOF'...EOF` e digite as linhas no
> cursor que fica esperando. Encerre com **Ctrl+D** — funciona igual no Windows,
> porque o `-it` conecta seu teclado direto ao container Linux.

> `parse.key=true` faz o Kafka tratar o que vem antes do `:` como a **chave** da
> mensagem. Guarde isso — é o que decide a partição, e a Aula 5 inteira é sobre isso.

### Consuma

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic pagamentos --from-beginning --timeout-ms 10000 \
  --property print.key=true --property print.partition=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic pagamentos --from-beginning --timeout-ms 10000 --property print.key=true --property print.partition=true --bootstrap-server localhost:9092
```

**Output esperado**
```
Partition:2	txn-1001	AUTHORIZED
Partition:1	txn-1004	SETTLED
Partition:1	txn-1005	AUTHORIZED
Partition:1	txn-1006	DECLINED
Partition:0	txn-1002	AUTHORIZED
Partition:0	txn-1003	DECLINED
```

**O que observar**: duas coisas.

1. As mensagens **se espalharam** pelas 3 partições — a chave decidiu o destino de cada uma.
2. A ordem de leitura **não é** a ordem que você digitou. O consumidor lê partição por partição; a ordem só é garantida **dentro** de cada uma. (Este é o assunto da Aula 3.)

> ⚠️ No fim vai aparecer um `ERROR ... TimeoutException`. **É esperado**: o
> `--timeout-ms 10000` manda o consumidor desistir após 10s sem mensagem nova. Sem
> esse parâmetro ele ficaria esperando para sempre — o que é o comportamento normal
> de um consumidor de verdade.

---

## Passo 3 — Cena 1: semântica de FILA *(~8 min)*

**Objetivo**: dois consumidores no **mesmo grupo** dividindo o trabalho.

O que muda em relação ao Passo 2 é **uma única flag**: `--group`.

### Terminal 1

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic pagamentos --group app-pagamentos \
  --property print.key=true --property print.partition=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic pagamentos --group app-pagamentos --property print.key=true --property print.partition=true --bootstrap-server localhost:9092
```

### Terminal 2 — o mesmo comando, o mesmo grupo

Cole **exatamente o mesmo comando** do Terminal 1 no segundo terminal.

Os dois ficam parados esperando. **Sem `--from-beginning`, eles só leem o que chegar de agora em diante** — por isso você produz depois.

### Terminal 3 — produza mais 6, num comando só

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic pagamentos \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-2001:AUTHORIZED
txn-2002:DECLINED
txn-2003:AUTHORIZED
txn-2004:SETTLED
txn-2005:AUTHORIZED
txn-2006:DECLINED
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-2001:AUTHORIZED
txn-2002:DECLINED
txn-2003:AUTHORIZED
txn-2004:SETTLED
txn-2005:AUTHORIZED
txn-2006:DECLINED
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic pagamentos --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

**O que observar**: olhe os terminais 1 e 2 lado a lado. Cada `txn` apareceu em **um
só** deles. Some as duas telas: dá 6, exatamente o que você produziu. Isso é uma fila
— trabalho distribuído entre workers.

> ⚠️ A divisão quase nunca é 3 e 3. Com 3 partições e 2 consumidores, um fica com 2
> partições e outro com 1 — então 4/2 ou 2/4 é o normal. O que importa **não é o
> equilíbrio**, é a ausência de sobreposição. O porquê é a Aula 4.

### Inspecione o grupo

Deixe os consumidores rodando e, num quarto terminal (ou pare um deles):

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-consumer-groups \
  --describe --group app-pagamentos \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```

**Output esperado**
```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
app-pagamentos  pagamentos      0          0               0               0               -               -               -
app-pagamentos  pagamentos      1          3               3               0               -               -               -
```

**O que observar**: o Kafka guarda **por partição** onde o grupo parou
(`CURRENT-OFFSET`) e onde o log terminou (`LOG-END-OFFSET`). A diferença é o **LAG** —
o número que vira o principal alerta de produção no Curso 7.

Encerre os dois consumidores (**Ctrl+C**) antes de seguir.

---

## Passo 4 — Cena 2: semântica de STREAMING *(~7 min)*

**Objetivo**: dois sistemas diferentes recebendo **o mesmo fato**.

Mesmíssimo comando do Passo 3. Muda **só o nome do grupo**.

### Terminal 1 — grupo `antifraude`

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic pagamentos --group antifraude \
  --property print.key=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic pagamentos --group antifraude --property print.key=true --bootstrap-server localhost:9092
```

### Terminal 2 — grupo `analytics`

O mesmo comando do Terminal 1, trocando `--group antifraude` por `--group analytics`.

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic pagamentos --group analytics \
  --property print.key=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic pagamentos --group analytics --property print.key=true --bootstrap-server localhost:9092
```

### Terminal 3 — produza mais 4, num comando só

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic pagamentos \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-3001:AUTHORIZED
txn-3002:DECLINED
txn-3003:SETTLED
txn-3004:AUTHORIZED
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-3001:AUTHORIZED
txn-3002:DECLINED
txn-3003:SETTLED
txn-3004:AUTHORIZED
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic pagamentos --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

**O que observar**: **as 4 mensagens aparecem nos DOIS terminais.** Você produziu 4 e
foram entregues 8 — sem duplicar nada no log. O antifraude e o analytics leem o mesmo
fluxo sem saber um do outro.

> 💡 **Este é o momento-chave da aula.** Entre o Passo 3 e o Passo 4 não mudou o
> tópico, não mudou o produtor, não mudou o consumidor. Mudou **uma string**: o nome
> depois de `--group`.

Confirme que existem dois grupos independentes (mesmo comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
```

Encerre os consumidores.

---

## Passo 5 — Cena 3: REPLAY *(~5 min)*

**Objetivo**: provar que ler não apaga.

Um grupo que **nunca existiu**, lendo desde o início:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic pagamentos --group auditoria-nova --from-beginning \
  --timeout-ms 10000 --property print.key=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic pagamentos --group auditoria-nova --from-beginning --timeout-ms 10000 --property print.key=true --bootstrap-server localhost:9092
```

**O que observar**: aparecem **todas as 16** mensagens (6 + 6 + 4) — inclusive as que
os grupos anteriores já tinham "consumido". Este consumidor não existia quando elas
foram produzidas e mesmo assim leu o histórico inteiro.

**Numa fila tradicional, ele encontraria o vazio.** É esta a diferença que a aula
inteira estava construindo.

**Validação** — conte quantas há no log, sem consumir (mesmo comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-get-offsets --topic pagamentos --bootstrap-server localhost:9092
```

Some os offsets das 3 partições: dá 16. **O consumo não diminuiu esse número.**

---

## Passo 6 — Decida a semântica *(~5 min)*

Para cada situação, responda **fila** ou **streaming**:

| # | Situação | Fila ou streaming? |
|---|---|---|
| 1 | Enviar o push de "compra aprovada" para o celular do cliente | |
| 2 | Propagar `TransacaoAutorizada` para antifraude, extrato e BI | |
| 3 | Reprocessar 3 dias de transações porque o cálculo de fraude tinha um bug | |
| 4 | Gerar o PDF da fatura mensal de cada conta | |

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

| # | Resposta | Por quê |
|---|---|---|
| 1 | **Fila** | Trabalho a executar **uma vez**. Dois workers = cliente recebe dois pushes |
| 2 | **Streaming** | Fato que interessa a N sistemas. Amanhã entra um quarto consumidor sem tocar em ninguém |
| 3 | **Streaming** | Só existe se o dado foi **retido** — é o Passo 5 aplicado a um incidente real |
| 4 | **Fila** | Cada fatura é gerada por um worker só |

**O padrão**: se a pergunta é *"quem faz esse trabalho?"*, é fila. Se é *"o que
aconteceu?"*, é streaming.

</details>

---

## Se algo não funcionou

**`ERROR ... TimeoutException` ao consumir**
Não é erro: é o `--timeout-ms` encerrando o consumidor. Sem ele, o processo espera indefinidamente.

**O consumidor não mostra nada**
Sem `--from-beginning`, ele só lê o que chega **depois** de subir. Suba o consumidor primeiro, produza depois — ou use `--from-beginning`.

**`UnknownTopicOrPartitionException`**
O tópico não existe. Volte ao Passo 1. (O sandbox tem auto-create **desligado** de propósito: criar tópico é decisão, não acidente.)

**O produtor não sai com Ctrl+C** *(modo interativo)*
Use **Ctrl+D** (fim de entrada). Se travar, `Ctrl+C` duas vezes. Na forma com
heredoc/here-string isso não acontece — o produtor encerra sozinho no fim dos dados.

**`the input device is not a TTY`**
Causa mais comum: você usou `-it` com dados vindo por pipe/heredoc — com entrada por
pipe é **`-i` sem `-t`**. Se foi no modo interativo *(Windows)*: rode no **Windows
Terminal** ou PowerShell nativo, não em consoles embutidos de editor que não emulam
TTY (ou use `winpty` no Git Bash antigo).

**No PowerShell, o comando multi-linha dá erro estranho** *(Windows)*
Você colou a versão macOS: o PowerShell não entende o `\` de continuação de linha e
quebra o comando em pedaços. Use a versão de uma linha marcada **Windows (PowerShell)**.

**Quero recomeçar do zero** (mesmo comando nos dois sistemas)
```bash
docker exec bootcamp-kafka kafka-topics --delete --topic pagamentos --bootstrap-server localhost:9092
```
Depois refaça o Passo 1.

---

## Limpar

Mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-topics --delete --topic pagamentos --bootstrap-server localhost:9092
```

Vai parar por aqui? Derrube o sandbox e apague os volumes:

**macOS / Linux**
```bash
make down
```

**Windows (PowerShell)**
```powershell
docker compose -f deployment/docker/docker-compose.yml down -v
```

---

## Bônus — as mesmas cenas, em Python

Tudo o que você fez na mão está automatizado em [`demos/curso1/`](../../../demos/curso1/), com output colorido e identificação de grupo:

```bash
make aula2-setup
make aula2-cena1a
make aula2-cena1b
make aula2-produce
make aula2-cena3
make aula2-teardown
```

| Alvo | O que faz |
|---|---|
| `aula2-setup` | cria o tópico |
| `aula2-cena1a` / `aula2-cena1b` | consumidor A (terminal 1) / B (terminal 2) |
| `aula2-produce` | produz 10 transações (terminal 3) |
| `aula2-cena3` | replay |

Use depois de ter feito na mão — a versão automatizada é conveniente, mas é a manual que ensina.

> 🪟 Os alvos `make` exigem o utilitário `make`, que o Windows não traz (nem o Git
> Bash). Rode o bônus pelo **WSL**, instale o make (`winget install GnuWin32.Make`) —
> ou pule: o conteúdo da aula é a parte manual, que você já fez.

---

## Glossário

| Termo | Definição |
|---|---|
| **Consumer group** | Conjunto de consumidores que dividem as partições de um tópico entre si. |
| **`--group`** | A flag que decide tudo: mesmo nome = fila; nomes diferentes = streaming. |
| **Offset** | Posição de leitura, mantida por grupo e por partição. |
| **Lag** | `LOG-END-OFFSET − CURRENT-OFFSET`: o quanto o grupo está atrasado. |
| **Retention** | Tempo que a mensagem fica no log, independentemente de ter sido lida. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Por que a divisão não é meio a meio | Aula 3 — partições |
| A chave decidindo a partição | Aula 5 — ordem por chave |
| Rebalance e commit de offset | Aula 4 — consumer groups |
| LAG como alerta | Curso 7 — operação |

**Fontes**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* — [e-book gratuito](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- Apache Kafka — [Introduction](https://kafka.apache.org/intro)
- KIP-932 — [Queues for Kafka (share groups)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 1 · O que é EDA?](../aula1/README.md) | [Curso 1](../README.md) | [Aula 3 · O log do Kafka](../aula3/README.md) |
