# Aula 4 — a versão linha de comando

> **Referência CLI** · O guia principal da aula é em Python → [README.md](README.md)

A mesma coreografia do guia — consumidor sozinho, rebalance, queda, offsets e LAG —
feita inteiramente com as ferramentas de linha de comando do Kafka. É a caixa de
ferramentas de **operação**: o que você usa para inspecionar e socorrer um grupo em
produção, mesmo quando a aplicação é Python.

Pré-requisitos: os mesmos do [README.md](README.md) (sandbox no ar via `make up`),
**sem** precisar do venv — aqui é só Docker.

---

## Passo 1 — Preparar o cenário *(~4 min)*

Mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-topics --create --topic pagamentos-app --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

Produza 6 transações, num comando só:

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic pagamentos-app \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-1:AUTHORIZED 150.00
txn-2:AUTHORIZED 89.90
txn-3:DECLINED 1250.00
txn-4:SETTLED 42.00
txn-5:AUTHORIZED 310.50
txn-6:DECLINED 75.00
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-1:AUTHORIZED 150.00
txn-2:AUTHORIZED 89.90
txn-3:DECLINED 1250.00
txn-4:SETTLED 42.00
txn-5:AUTHORIZED 310.50
txn-6:DECLINED 75.00
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic pagamentos-app --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

O comando termina sozinho — sem Ctrl+D (é o `-i` sem `-t`; detalhes na Aula 2, Passo 2).

---

## Passo 2 — Um consumidor sozinho *(~6 min)*

**Objetivo**: ver que, sozinho, ele atende **todas** as partições.

### Terminal 1 — suba o consumidor A

**macOS / Linux**
```bash
docker run -it --rm --name consumidor-A \
  --network bootcamp-confluent_default \
  confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer --topic pagamentos-app --group app-pagamentos \
  --from-beginning --property print.partition=true \
  --bootstrap-server kafka:29092
```

**Windows (PowerShell)**
```powershell
docker run -it --rm --name consumidor-A --network bootcamp-confluent_default confluentinc/cp-kafka:7.6.1 kafka-console-consumer --topic pagamentos-app --group app-pagamentos --from-beginning --property print.partition=true --bootstrap-server kafka:29092
```

Ele lê as 6 mensagens e fica esperando. **Deixe rodando.**

### Terminal 2 — inspecione o grupo

Mesmo comando nos dois sistemas (você vai repeti-lo várias vezes nesta aula):

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```

**Output esperado**
```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID                                           HOST         CLIENT-ID
app-pagamentos  pagamentos-app  0          5               5               0     console-consumer-8f406ebd-3a42-4b9a-8226-1939fba8e30d /127.0.0.1   console-consumer
app-pagamentos  pagamentos-app  1          0               0               0     console-consumer-8f406ebd-3a42-4b9a-8226-1939fba8e30d /127.0.0.1   console-consumer
app-pagamentos  pagamentos-app  2          1               1               0     console-consumer-8f406ebd-3a42-4b9a-8226-1939fba8e30d /127.0.0.1   console-consumer
```

**O que observar**: o **mesmo `CONSUMER-ID`** aparece nas 3 linhas. Um consumidor
sozinho no grupo fica com **todas** as partições — funciona, mas sem paralelismo
nenhum.

E as três colunas de número:

| Coluna | Significado |
|---|---|
| `CURRENT-OFFSET` | Até onde o grupo **confirmou** que leu |
| `LOG-END-OFFSET` | Onde o log **termina** (o que você viu com `kafka-get-offsets` na Aula 3) |
| `LAG` | A diferença — **o quanto o grupo está atrasado** |

> 💡 `LAG = 0` significa que o consumo alcançou a produção. No Curso 7, esse número
> vira o principal alerta de produção: LAG subindo = consumidor não dá conta.

---

## Passo 3 — Entra o segundo consumidor: REBALANCE *(~8 min)*

**Objetivo**: provocar a redistribuição e ver quem ficou com o quê.

### Terminal 3 — suba o consumidor B, no MESMO grupo

**macOS / Linux**
```bash
docker run -it --rm --name consumidor-B \
  --network bootcamp-confluent_default \
  confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer --topic pagamentos-app --group app-pagamentos \
  --property print.partition=true \
  --bootstrap-server kafka:29092
```

**Windows (PowerShell)**
```powershell
docker run -it --rm --name consumidor-B --network bootcamp-confluent_default confluentinc/cp-kafka:7.6.1 kafka-console-consumer --topic pagamentos-app --group app-pagamentos --property print.partition=true --bootstrap-server kafka:29092
```

Aguarde ~15 segundos e inspecione de novo (terminal 2, mesmo comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```

**Output esperado** (recortando as colunas que importam):
```
PARTITION  CONSUMER-ID
0          console-consumer-0cd0bb5e-9e9...     ← consumidor A
1          console-consumer-0cd0bb5e-9e9...     ← consumidor A
2          console-consumer-c03b67b9-417...     ← consumidor B
```

**O que observar**: agora são **dois CONSUMER-IDs diferentes**. O grupo se
reorganizou sozinho — isso é o **rebalance**. A ficou com 2 partições, B com 1.

> ⚠️ Nunca é meio a meio: **3 partições não dividem por 2**. Esta é a regra de ouro
> da aula — *cada partição é lida por no máximo UM consumidor do grupo*. Se você
> subisse um 4º consumidor, ele ficaria **ocioso**: partições são o teto do
> paralelismo.

### Prove o teto do paralelismo

Existe uma visão do grupo por **membro**, que mostra isso de forma direta (mesmo
comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --members --bootstrap-server localhost:9092
```

Suba um terceiro e um quarto consumidor (mesmo comando, mudando `--name`) e rode de
novo:

**Output esperado com 4 consumidores e 3 partições**
```
GROUP         CONSUMER-ID                                           HOST         CLIENT-ID        #PARTITIONS
teste-ocioso  console-consumer-dbe5257f-28d3-4e74-9ada-71c0d77b9cae /172.21.0.4  console-consumer 1
teste-ocioso  console-consumer-a6a7a001-1d1d-4180-b399-78571f312964 /172.21.0.6  console-consumer 1
teste-ocioso  console-consumer-eed5504f-885c-4b80-a61e-d27e0e227d73 /172.21.0.5  console-consumer 0
teste-ocioso  console-consumer-cbfec4aa-8059-4e91-b023-229a4f33aba1 /172.21.0.7  console-consumer 1
```

**O que observar**: o terceiro membro tem **`#PARTITIONS = 0`**. Ele está no grupo,
consumindo recursos, recebendo heartbeats — e **sem fazer nada**. Adicionar
consumidores além do número de partições não acelera coisa alguma.

---

## Passo 4 — Derrube um consumidor *(~8 min)*

**Objetivo**: provar que a queda de um membro não perde mensagens.

Com A e B rodando, derrube **só o B**:

```bash
docker stop consumidor-B
```

Aguarde ~20 segundos (o `session.timeout.ms` default é 45s, mas o `docker stop`
faz o consumidor sair de forma limpa, acelerando o processo) e inspecione:

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```

**Output esperado**
```
PARTITION  CONSUMER-ID
0          console-consumer-0cd0bb5e-9e9...
1          console-consumer-0cd0bb5e-9e9...
2          console-consumer-0cd0bb5e-9e9...     ← A herdou a partição do B
```

**O que observar**: o `CONSUMER-ID` do A agora aparece nas **três** partições. Ele
herdou o trabalho do consumidor morto, sem intervenção humana. **Nenhuma partição
ficou órfã.**

Produza mais mensagens (num terminal livre) e confirme que o A recebe todas:

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic pagamentos-app \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-7:AUTHORIZED 500.00
txn-8:SETTLED 22.10
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-7:AUTHORIZED 500.00
txn-8:SETTLED 22.10
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic pagamentos-app --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

> 💡 **É isto que dá tolerância a falha ao consumo.** Em produção, um pod que morre
> tem suas partições assumidas pelos outros em segundos. O custo é a pausa do
> rebalance — e consumidores instáveis, que entram e saem, causam rebalance em loop:
> um clássico problema de produção.

---

## Passo 5 — O commit e o LAG *(~7 min)*

**Objetivo**: entender que o offset é um ponteiro — e que dá para movê-lo.

Derrube o consumidor A (`docker stop consumidor-A`, ou Ctrl+C no terminal 1). O
grupo fica sem membros ativos, mas **os offsets continuam guardados**:

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```
```
Consumer group 'app-pagamentos' has no active members.

app-pagamentos  pagamentos-app  0    5    5    0    -   -   -
app-pagamentos  pagamentos-app  1    0    0    0    -   -   -
app-pagamentos  pagamentos-app  2    1    1    0    -   -   -
```

**O que observar**: sem membros, mas com posição preservada. É por isso que um
consumidor que reinicia **retoma de onde parou** — o progresso é do **grupo**, não
do processo.

### Reposicione o ponteiro

Primeiro, simule sem aplicar (`--dry-run`):

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-consumer-groups \
  --reset-offsets --to-earliest \
  --group app-pagamentos --topic pagamentos-app \
  --dry-run --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-consumer-groups --reset-offsets --to-earliest --group app-pagamentos --topic pagamentos-app --dry-run --bootstrap-server localhost:9092
```

**Output esperado**
```
GROUP                          TOPIC                          PARTITION  NEW-OFFSET
app-pagamentos                 pagamentos-app                 0          0
app-pagamentos                 pagamentos-app                 1          0
app-pagamentos                 pagamentos-app                 2          0
```

Agora aplique de verdade — **o mesmo comando, trocando `--dry-run` por `--execute`**.

E veja o LAG **reaparecer**:

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group app-pagamentos --bootstrap-server localhost:9092
```
```
app-pagamentos  pagamentos-app  0    0    5    5     ← LAG voltou a 5
app-pagamentos  pagamentos-app  1    0    0    0
app-pagamentos  pagamentos-app  2    0    1    1     ← LAG voltou a 1
```

**O que observar**: você não recriou mensagem nenhuma — apenas **moveu o ponteiro
para trás**. O log não mudou (Aula 3); mudou onde o grupo acha que parou. Se você
subir o consumidor agora, ele relê tudo.

> ⚠️ `--reset-offsets` só funciona com o grupo **sem membros ativos**. Com um
> consumidor rodando, o comando falha — é uma proteção contra reset acidental em
> produção.

**Outras opções úteis**: `--to-latest` (pular tudo o que está pendente),
`--to-datetime` (voltar a um momento), `--shift-by -100` (recuar 100 mensagens).

---

---

## Se algo não funcionou

**`docker: Error response from daemon: network bootcamp-confluent_default not found`**
O sandbox não está no ar. Rode `make up` (Windows: `docker compose -f deployment/docker/docker-compose.yml up -d`).
Confirme o nome exato com `docker network ls | grep bootcamp` (PowerShell: `| Select-String bootcamp`).

**O consumidor em container próprio não conecta**
Use `--bootstrap-server kafka:29092` (endereço interno), não `localhost:9092`. Dentro de outro container, `localhost` é ele mesmo.

**`Error: Assignments can only be reset if the group 'X' is inactive`**
Exatamente a proteção do Passo 5. Derrube todos os consumidores antes.

**O rebalance não acontece**
Espere mais. Se o consumidor foi morto abruptamente (`kill -9`), o coordinator só o expulsa após o `session.timeout.ms` (45s default).

**`--name consumidor-B` já em uso**
Sobrou de uma execução anterior: `docker rm -f consumidor-B`.

**`GroupNotEmptyException: The group is not empty` ao deletar o grupo**
O broker ainda considera algum membro vivo. Confirme que nenhum container sobrou
(`docker ps --filter name=consumidor`), aguarde ~15s e repita. O grupo só pode ser
removido quando estiver de fato vazio.

---

---

## Limpar

**macOS / Linux**
```bash
docker rm -f consumidor-A consumidor-B consumidor-C 2>/dev/null
docker exec bootcamp-kafka kafka-consumer-groups --delete --group app-pagamentos --bootstrap-server localhost:9092
docker exec bootcamp-kafka kafka-topics --delete --topic pagamentos-app --bootstrap-server localhost:9092
```

**Windows (PowerShell)** — só a primeira linha muda (`2>$null` em vez de `2>/dev/null`):
```powershell
docker rm -f consumidor-A consumidor-B consumidor-C 2>$null
docker exec bootcamp-kafka kafka-consumer-groups --delete --group app-pagamentos --bootstrap-server localhost:9092
docker exec bootcamp-kafka kafka-topics --delete --topic pagamentos-app --bootstrap-server localhost:9092
```

---

---

| ↑ Guia principal (Python) |
|---|
| [README.md](README.md) |
