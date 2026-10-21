# Curso 1 · Aula 3 — O log do Kafka: tópicos, partições e offsets

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~35 min · 💻 Mão na massa · **1 terminal** basta

Nesta aula você abre o capô. Vai ver que a partição **é um arquivo no disco**, ler o conteúdo binário desse arquivo, e provar que o log não muda quando alguém lê. Ao final, "log append-only" deixa de ser slogan e passa a ser algo que você tocou.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 2](../aula2/README.md) | Você já sabe criar tópico, produzir e consumir |
| Docker rodando | Docker Desktop aberto |
| 1 terminal | Na raiz de `repo/` |

**macOS / Linux**
```bash
make up
make status
```

`make up` sobe o Kafka (~30s); `make status` confirma `bootcamp-kafka Up`.

**Windows (PowerShell)** — sem `make`, use o `docker compose` direto:
```powershell
docker compose -f deployment/docker/docker-compose.yml up -d
docker compose -f deployment/docker/docker-compose.yml ps    # confirme: bootcamp-kafka Up
```

> 🪟 **Windows**: como na Aula 2, os comandos multi-linha aparecem em dupla — a versão
> PowerShell em uma linha só. Comandos de uma linha são idênticos nos dois sistemas.

> 💡 Todos os comandos rodam dentro do container. Se quiser encurtar:
> `alias kafka='docker exec bootcamp-kafka'` (macOS/Linux) ou
> `function kafka { docker exec bootcamp-kafka @args }` (PowerShell)

---

## O que você vai saber fazer ao final

- Criar um tópico particionado e ler suas configurações efetivas (retention, cleanup.policy)
- Explicar por que cada partição tem offsets começando em 0
- Encontrar e listar os arquivos de uma partição no disco do broker
- Ler o conteúdo de um segment com `kafka-dump-log` e identificar cada campo do record
- Consultar o tamanho de um tópico sem consumir nenhuma mensagem

---

## Passo 1 — Criar o tópico e ler suas configs *(~6 min)*

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-topics \
  --create --topic transacoes \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec bootcamp-kafka kafka-topics \
  --describe --topic transacoes \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-topics --create --topic transacoes --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker exec bootcamp-kafka kafka-topics --describe --topic transacoes --bootstrap-server localhost:9092
```

**Output esperado**
```
Topic: transacoes	TopicId: 5Z-NSjkWQJCGkUBAW15Igg	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1048576
	Topic: transacoes	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: transacoes	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: transacoes	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

**O que observar**: o tópico **não é uma coisa só** — são 3 partições, cada uma com seu líder. Guarde esse número: é o teto do paralelismo (Aula 4).

### As configs que você não pediu, mas herdou

O `--describe` acima mostra pouca coisa. As configurações **efetivas** (incluindo os defaults do broker) vêm de outro comando:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-configs \
  --describe --topic transacoes --all \
  --bootstrap-server localhost:9092 | grep -E "retention.ms|cleanup.policy|segment.bytes"
```

**Windows (PowerShell)** — sem `grep`; o filtro equivalente é `Select-String`:
```powershell
docker exec bootcamp-kafka kafka-configs --describe --topic transacoes --all --bootstrap-server localhost:9092 | Select-String "retention.ms|cleanup.policy|segment.bytes"
```

**Output esperado**
```
  cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}
  retention.ms=604800000 sensitive=false synonyms={}
  segment.bytes=1048576 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.segment.bytes=1048576, DEFAULT_CONFIG:log.segment.bytes=1073741824}
```

**O que observar**: três números que explicam a aula inteira.

| Config | Valor | Significado |
|---|---|---|
| `retention.ms` | 604800000 | **7 dias** — é isso que torna o replay da Aula 2 possível… e finito |
| `cleanup.policy` | `delete` | Ao expirar, o dado é **apagado**; a alternativa é `compact` (guarda o último valor por chave) |
| `segment.bytes` | 1048576 | **1 MB** — quando o arquivo atinge esse tamanho, o Kafka fecha e abre outro |

> O `segment.bytes` de 1 MB é uma configuração deste sandbox (o default do Kafka é
> 1 GB). Foi reduzido de propósito para você conseguir ver mais de um arquivo no
> Passo 3 sem produzir milhões de mensagens.

---

## Passo 2 — Produzir e ver os offsets por partição *(~6 min)*

Produza 9 transações **com chave** (a chave decide a partição — Aula 2, Passo 2), num
comando só:

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic transacoes \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-1001:AUTHORIZED 150.00
txn-1002:AUTHORIZED 89.90
txn-1003:DECLINED 1250.00
txn-1004:SETTLED 42.00
txn-1005:AUTHORIZED 310.50
txn-1006:DECLINED 75.00
txn-1007:SETTLED 199.99
txn-1008:AUTHORIZED 620.00
txn-1009:SETTLED 33.30
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-1001:AUTHORIZED 150.00
txn-1002:AUTHORIZED 89.90
txn-1003:DECLINED 1250.00
txn-1004:SETTLED 42.00
txn-1005:AUTHORIZED 310.50
txn-1006:DECLINED 75.00
txn-1007:SETTLED 199.99
txn-1008:AUTHORIZED 620.00
txn-1009:SETTLED 33.30
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic transacoes --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

> Repare no **`-i` sem `-t`**: entrada por heredoc/here-string não tem terminal — o
> `-t` causaria `the input device is not a TTY` (detalhes na Aula 2, Passo 2).

Agora leia mostrando partição **e** offset:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes --from-beginning --timeout-ms 9000 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes --from-beginning --timeout-ms 9000 --property print.key=true --property print.partition=true --property print.offset=true --bootstrap-server localhost:9092
```

**Output esperado**
```
Partition:1	Offset:0	txn-1004	SETTLED 42.00
Partition:1	Offset:1	txn-1005	AUTHORIZED 310.50
Partition:1	Offset:2	txn-1006	DECLINED 75.00
Partition:2	Offset:0	txn-1001	AUTHORIZED 150.00
Partition:2	Offset:1	txn-1007	SETTLED 199.99
Partition:0	Offset:0	txn-1002	AUTHORIZED 89.90
Partition:0	Offset:1	txn-1003	DECLINED 1250.00
Partition:0	Offset:2	txn-1008	AUTHORIZED 620.00
Partition:0	Offset:3	txn-1009	SETTLED 33.30
```

**O que observar** — esta é a observação central da aula:

1. **Cada partição tem sua própria sequência de offsets, começando em 0.** Existe um `Offset:0` na partição 0, outro na 1 e outro na 2. O offset **não** identifica a mensagem sozinho — a identidade é o par **(partição, offset)**.
2. A distribuição não é uniforme (4/3/2): a chave é jogada num hash, e hash não distribui perfeitamente com poucos itens.
3. A **ordem de leitura não é a ordem que você digitou** — o consumidor lê partição por partição. Ordem só existe *dentro* de uma partição, nunca no tópico como um todo.

---

## Passo 3 — A partição é um arquivo *(~8 min)*

**Objetivo**: sair do conceito e olhar o disco.

```bash
docker exec bootcamp-kafka ls -lh /var/lib/kafka/data/transacoes-0/
```

**Output esperado**
```
total 12K
-rw-r--r-- 1 appuser appuser 10M Aug 18 16:43 00000000000000000000.index
-rw-r--r-- 1 appuser appuser 183 Aug 18 16:43 00000000000000000000.log
-rw-r--r-- 1 appuser appuser 10M Aug 18 16:43 00000000000000000000.timeindex
-rw-r--r-- 1 appuser appuser   8 Aug 18 16:43 leader-epoch-checkpoint
-rw-r--r-- 1 appuser appuser  43 Aug 18 16:43 partition.metadata
```

**O que observar**:

- O diretório é `transacoes-0` — **`{tópico}-{partição}`**. Existem também `transacoes-1` e `transacoes-2`
- O `.log` é o segment: **onde suas mensagens estão de verdade** (183 bytes para 4 mensagens)
- O nome `00000000000000000000` é o **offset base** do segment — o primeiro offset que ele contém
- O `.index` mapeia offset → posição no arquivo; o `.timeindex` faz o mesmo por timestamp. Ambos são pré-alocados com 10 MB
- `.index` e `.timeindex` **não são os dados** — são atalhos de busca. Achar o offset 5.000 é escolher o segment certo e pular direto, sem varrer o arquivo

### Agora leia o binário

O Kafka tem uma ferramenta para abrir o segment:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-dump-log \
  --files /var/lib/kafka/data/transacoes-0/00000000000000000000.log \
  --print-data-log
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-dump-log --files /var/lib/kafka/data/transacoes-0/00000000000000000000.log --print-data-log
```

**Output esperado**
```
Dumping /var/lib/kafka/data/transacoes-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 3 count: 4 baseSequence: 0 lastSequence: 3 producerId: 2 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1787071431152 size: 183 magic: 2 compresscodec: none crc: 132172820 isvalid: true
| offset: 0 CreateTime: 1787071431151 keySize: 8 valueSize: 16 sequence: 0 headerKeys: [] key: txn-1002 payload: AUTHORIZED 89.90
| offset: 1 CreateTime: 1787071431152 keySize: 8 valueSize: 16 sequence: 1 headerKeys: [] key: txn-1003 payload: DECLINED 1250.00
| offset: 2 CreateTime: 1787071431152 keySize: 8 valueSize: 17 sequence: 2 headerKeys: [] key: txn-1008 payload: AUTHORIZED 620.00
| offset: 3 CreateTime: 1787071431152 keySize: 8 valueSize: 13 sequence: 3 headerKeys: [] key: txn-1009 payload: SETTLED 33.30
```

**O que observar**: cada linha `| offset: ...` é a **anatomia do record** que você viu no slide, agora lida do disco:

| Campo | O que é |
|---|---|
| `offset` | A posição — atribuída pelo log, não pelo produtor |
| `CreateTime` | O timestamp em epoch millis |
| `keySize` / `valueSize` | Tamanhos em bytes — para o broker, key e value **são só bytes** |
| `key` | `txn-1002` — a identidade que decidiu esta partição |
| `payload` | O conteúdo (o `value`) |
| `headerKeys` | Vazio aqui; é onde vive trace-id, origem etc. |

A linha `baseOffset:` acima é o **record batch** — o Kafka agrupa mensagens em lotes para comprimir e escrever de uma vez. `compresscodec: none` porque o console-producer não comprime; `crc` é o checksum que detecta corrupção.

> 💡 **Este é o momento da aula.** "O Kafka é um log append-only" acabou de virar um
> arquivo de 183 bytes que você abriu e leu.

---

## Passo 4 — O log não muda quando você lê *(~5 min)*

**Objetivo**: provar a imutabilidade.

Leia **só a partição 0**, duas vezes seguidas:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes --partition 0 --from-beginning --timeout-ms 7000 \
  --property print.offset=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes --partition 0 --from-beginning --timeout-ms 7000 --property print.offset=true --bootstrap-server localhost:9092
```

Rode o **mesmo comando** de novo.

**Output esperado** — nas duas vezes:
```
Offset:0	AUTHORIZED 89.90
Offset:1	DECLINED 1250.00
Offset:2	AUTHORIZED 620.00
Offset:3	SETTLED 33.30
```

**O que observar**: mesmas mensagens, mesma ordem, **mesmos offsets**. Duas leituras completas e o log ficou intacto. Quem muda de posição é o leitor, nunca o log.

> A flag `--partition 0` faz o consumidor ler **uma partição específica**, ignorando
> consumer group. É assim que se investiga uma partição isolada em produção.

---

## Passo 5 — O tamanho do log, sem ler nada *(~4 min)*

Mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-get-offsets --topic transacoes --bootstrap-server localhost:9092
```

**Output esperado**
```
transacoes:0:4
transacoes:1:3
transacoes:2:2
```

**O que observar**: o formato é `tópico:partição:offset_final`. Some: 4+3+2 = **9**, exatamente o que você produziu. E você obteve esse número **sem consumir uma única mensagem** — o log é uma estrutura inspecionável, não uma caixa preta.

> Este é o mesmo número que aparece como `LOG-END-OFFSET` no
> `kafka-consumer-groups --describe` da Aula 2. A diferença entre ele e o
> `CURRENT-OFFSET` de um grupo é o **lag**.

---

## Passo 6 — Retention: quem apaga o dado *(~5 min)*

**Objetivo**: ver que o consumidor não apaga nada — a política de retenção apaga.

Mude a retenção do tópico para 1 minuto — mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-configs --alter --topic transacoes --add-config retention.ms=60000 --bootstrap-server localhost:9092
```

**Output esperado**
```
Completed updating config for topic transacoes.
```

Confirme que a configuração agora é **dinâmica** (sobrepõe o default) — mesmo comando
nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-configs --describe --topic transacoes --bootstrap-server localhost:9092
```
```
Dynamic configs for topic transacoes are:
  retention.ms=60000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=60000}
```

**O que observar**: você acabou de dizer ao Kafka *"dados com mais de 1 minuto podem
ir"*. A limpeza roda em background (a cada 5 min por default), e ela **apaga segments
inteiros** — nunca edita o arquivo. É por isso que a imutabilidade e a expiração
convivem sem contradição.

Volte ao default antes de seguir — mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-configs --alter --topic transacoes --delete-config retention.ms --bootstrap-server localhost:9092
```

> ⚠️ `retention.ms` curto é útil para demonstrar, **péssimo** por acidente: com 60s,
> um replay de ontem não existe mais. É uma das configs que mais causa perda de dado
> não intencional em produção.

---

## Passo 7 — Fixe os conceitos *(~4 min)*

Responda sem rodar nada:

1. Um tópico tem 3 partições. Existe mais de uma mensagem com `offset: 0`? Por quê?
2. Você tem o offset 42 e precisa achar a mensagem. Que arquivos o broker usa?
3. Um consumidor leu todo o tópico e encerrou. Quantos bytes foram liberados no disco?
4. Você precisa guardar **o estado atual** de cada conta (não o histórico). Qual `cleanup.policy`?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Sim — um por partição.** O offset é sequencial *dentro* da partição. A identidade
única de uma mensagem é o par **(partição, offset)** — foi o que você viu no Passo 2:
três linhas diferentes com `Offset:0`.

**2. O `.index` e o `.log`.** Primeiro o broker escolhe o segment pelo nome do arquivo
(o offset base — o segment `00000000000000000000.log` contém do offset 0 em diante).
Depois consulta o `.index` para achar a **posição em bytes** dentro do arquivo, e faz
um *seek* direto. Sem varrer o arquivo.

**3. Zero.** Consumir não apaga nada — foi o Passo 4. Quem libera disco é a política de
retenção (Passo 6), apagando segments expirados.

**4. `compact`.** A log compaction mantém **o último valor de cada chave** e descarta
os anteriores. É o mecanismo por trás dos tópicos de CDC (Curso 3): a foto atual de
cada linha da tabela. `delete` (o default) expira por tempo/tamanho, mantendo o
histórico completo enquanto ele durar.

</details>

---

## Se algo não funcionou

**`ERROR ... TimeoutException` ao consumir**
Esperado — é o `--timeout-ms` encerrando o consumidor.

**`ls: /var/lib/kafka/data/transacoes-0/: No such file or directory`**
O tópico não existe ou tem outro nome. Confira:
`docker exec bootcamp-kafka ls /var/lib/kafka/data/ | grep transacoes`
(no PowerShell, troque `grep transacoes` por `Select-String transacoes`)

**`kafka-dump-log` não mostra o payload**
Falta a flag `--print-data-log`. Sem ela, só os metadados dos batches aparecem.

**A partição 0 está vazia**
A distribuição depende do hash das chaves. Se você digitou chaves diferentes das do guia, inspecione outra partição — `kafka-get-offsets` mostra quais têm dados.

**Quero recomeçar** (mesmo comando nos dois sistemas)
```bash
docker exec bootcamp-kafka kafka-topics --delete --topic transacoes --bootstrap-server localhost:9092
```

---

## Limpar

Mesmo comando nos dois sistemas:

```bash
docker exec bootcamp-kafka kafka-topics --delete --topic transacoes --bootstrap-server localhost:9092
```

Vai parar por aqui?

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

> 🪟 Os alvos `make` exigem o utilitário `make`, que o Windows não traz — rode pelo
> **WSL** ou pule (a parte manual acima é o conteúdo da aula).

```bash
make aula3-cena1
make aula3-cena2
make aula3-cena3
make aula3-cena4
make aula3-cena5
make aula3-teardown
```

| Alvo | O que faz |
|---|---|
| `aula3-cena1` | cria o tópico + describe |
| `aula3-cena2` | produz sem chave e mostra partição/offset |
| `aula3-cena3` | `ls` nos segments do disco |
| `aula3-cena4` | relê a mesma partição duas vezes |
| `aula3-cena5` | offsets finais |

---

## Glossário

| Termo | Definição |
|---|---|
| **Log append-only** | Sequência em que só se escreve no fim; nada é alterado ou removido no meio. |
| **Partição** | Um log independente dentro do tópico; a unidade de paralelismo e de ordem. |
| **Offset** | Posição sequencial do record **dentro de uma partição**, começando em 0. |
| **Segment** | Arquivo `.log` que armazena um trecho da partição; nomeado pelo offset base. |
| **Índice (`.index`/`.timeindex`)** | Atalhos que mapeiam offset/timestamp → posição em bytes no segment. |
| **Record batch** | Lote de records escrito de uma vez; unidade de compressão e de checksum. |
| **Retention** | Política que define quando o dado expira (`retention.ms` / `retention.bytes`). |
| **Log compaction** | `cleanup.policy=compact`: mantém só o último valor de cada chave. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Partição como unidade de paralelismo | Aula 4 — consumer groups e rebalance |
| A chave decidindo a partição | Aula 5 — ordem por chave |
| `LOG-END-OFFSET` e lag | Aula 4 e Curso 7 — operação |
| `cleanup.policy=compact` | Curso 3 — tópicos CDC e tombstones |
| Retention como custo | Curso 7 — dimensionamento e custos |

**Fontes**

- Jay Kreps — [*The Log: What every software engineer should know*](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.*, cap. 3 e 6 — [e-book gratuito](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- Apache Kafka — [Topic-level configs](https://kafka.apache.org/documentation/#topicconfigs)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 2 · Message Queue vs Data Streaming](../aula2/README.md) | [Curso 1](../README.md) | [Aula 4 · Produtores e consumidores](../aula4/README.md) |
