# Curso 1 · Aula 4 — Produtores e consumidores: grupos, rebalance e commit

> **Guia do aluno** · Bloco 1 — Fundamentos
> ⏱ ~50 min · 🐍 **Python** (`confluent-kafka`) · 2 terminais no Passo 4

Esta é a aula mais "ao vivo" do curso: você vai ver partições **migrando entre
consumidores em tempo real**. E vai ver isso escrito em Python — a mesma API dos seus
serviços de produção, com a config montada campo a campo, sem factory escondendo nada.

> 💻 Prefere a linha de comando? A coreografia inteira também existe em versão CLI
> (`kafka-console-*` + containers) → [cli.md](cli.md). O conceito é o mesmo; aqui o
> foco é **o código que você vai escrever de verdade**.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito a [Aula 3](../aula3/README.md) | Partições e offsets são a base de tudo aqui |
| Docker rodando | Docker Desktop aberto |
| O venv instalado | `make install` (uma vez) — ver [índice](../../README.md) |
| 2 terminais | Só no Passo 4 (o rebalance precisa de dois consumidores) |

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

Cada passo mostra o alvo `make` e, ao lado, o comando Windows equivalente
(`.venv\Scripts\python -m ...`). Pelo **PyCharm**: as configs `Aula 4 API · *` no
dropdown do ▶ rodam cada cena — ver [PYCHARM.md](../../../PYCHARM.md).

Os dois arquivos da aula — feitos para **ler tanto quanto rodar**:

| Arquivo | O que ensina |
|---|---|
| [aula4_api_producer.py](../../../demos/curso1/aula4_api_producer.py) | `produce()` assíncrono, padrões de envio, `acks` |
| [aula4_api_consumer.py](../../../demos/curso1/aula4_api_consumer.py) | as 3 decisões do consumer, rebalance, commit, LAG |

---

## O que você vai saber fazer ao final

- Montar a config de um producer campo a campo — e explicar cada campo
- Escolher entre fire-and-forget, síncrono e assíncrono — com números
- Escolher o `acks` certo — e dizer o que cada nível garante
- Escrever o loop de um consumer: `subscribe` → `poll` → processa → `commit`
- Provocar um rebalance e **ler os callbacks** que o narram
- Medir o LAG de um grupo pela API — a métrica nº 1 do Curso 7
- Escolher a estratégia de commit e a posição inicial de leitura

---

## Passo 1 — A Producer API: anatomia *(~6 min)*

**Objetivo**: ver o ciclo completo de uma escrita — e provar que `produce()` não espera
ninguém.

O coração do arquivo é a config, comentada linha a linha:

```python
{
    "bootstrap.servers": LOCAL_BOOTSTRAP,   # UM endereço basta — o client descobre o resto
    "client.id": "aula4-api-producer",      # como você aparece nas métricas do broker
    "acks": "all",                          # só confirma com TODAS as réplicas in-sync
    "enable.idempotence": True,             # retry do producer não duplica (Aula 5)
    "linger.ms": 0,                         # sem espera de lote — demo pequena
}
```

Rode:

```bash
make aula4-api-producer
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_producer`

**Output esperado**
```
1) produce() — enfileira e RETORNA NA HORA:
  → enfileirada txn-1  (nenhuma confirmação ainda)
  → enfileirada txn-2  (nenhuma confirmação ainda)
  ...
  → enfileirada txn-6  (nenhuma confirmação ainda)

2) flush() — espera tudo que está na fila ser confirmado:
  ✓ entregue  txn-3 → Partition:0 Offset:0
  ✓ entregue  txn-4 → Partition:0 Offset:1
  ✓ entregue  txn-6 → Partition:0 Offset:2
  ✓ entregue  txn-1 → Partition:1 Offset:0
  ✓ entregue  txn-2 → Partition:2 Offset:0
  ✓ entregue  txn-5 → Partition:2 Offset:1
  ✓ fila vazia — todas as entregas confirmadas pelo broker
```

**O que observar**: a ordem do output conta a história inteira. Os seis `→ enfileirada`
saem **antes** de qualquer `✓ entregue` — `produce()` não espera o broker. Quem traz a
confirmação (com partição e offset atribuídos) é o **delivery callback**, depois. E o
`flush()` retorna quantas mensagens **não** foram entregues — o script checa esse
número, porque ignorá-lo é reportar sucesso com mensagem perdida.

Repare também na chave decidindo a partição: mesma chave, sempre a mesma partição — a
Aula 5 constrói em cima disso.

---

## Passo 2 — Os três jeitos de enviar *(~5 min)*

O mesmo `produce()` sustenta três padrões — muda só **o quanto você espera (e sabe)**:

| Padrão | Como | Você fica sabendo se falhou? |
|---|---|---|
| **Fire-and-forget** | `produce()` e pronto | **Não** |
| **Síncrono** | `produce()` + `flush()` a cada mensagem | Sim — antes da próxima |
| **Assíncrono** | `produce(on_delivery=...)` + um `flush()` no fim | Sim — via callback |

```bash
make aula4-api-envios
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_producer --cena envios`

**Output esperado** (os tempos variam; a ordem entre eles, não)
```
1) fire-and-forget — produce() e pronto:
   6 produce() em 0.1 ms — e zero confirmações
   se uma falhasse (topic errado, broker fora), NINGUÉM ficaria sabendo

2) síncrono — produce() + flush() a cada mensagem:
   6 confirmadas, uma a uma, em 10.0 ms
   latência = soma dos round-trips; o throughput morre aqui

3) assíncrono — callback por mensagem, flush no fim:
   6 confirmadas em lote em 4.5 ms

padrão                tempo   confirmação
fire-and-forget       0.1 ms   nenhuma — falha é invisível
síncrono             10.0 ms   cada mensagem, antes da próxima
assíncrono            4.5 ms   todas — via callback
```

**O que observar**: em `localhost` a diferença síncrono × assíncrono já é ~2×; numa
rede real (round-trip de milissegundos, não de microssegundos) ela vira **ordens de
grandeza** — o síncrono paga um round-trip **por mensagem**. O assíncrono é o padrão de
produção: velocidade de fire-and-forget com a garantia do síncrono. Os outros dois têm
nicho: métrica descartável (fire-and-forget) e a rara mensagem que não pode sair da
ordem do fluxo de código (síncrono).

> 💡 Na API Java, o síncrono é `producer.send(record).get()` — o `.get()` bloqueando no
> `Future`. Em `confluent-kafka`, o equivalente é o `flush()` por mensagem que o script
> usa.

---

## Passo 3 — `acks`: o contrato de durabilidade *(~6 min)*

Quantas confirmações você exige antes de chamar de "escrito"?

| `acks` | Espera | Se o líder cair logo depois |
|---|---|---|
| `0` | ninguém | mensagem **pode sumir** — e você nem sabe o offset |
| `1` | o líder escreveu | pode sumir se as réplicas não copiaram |
| `all` | todas as réplicas in-sync | **sobrevive** — a âncora da Aula 5 |

```bash
make aula4-api-acks
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_producer --cena acks`

**Output esperado** (offsets variam conforme o que você já produziu)
```
acks=0    — não espera NINGUÉM — nem o líder
   offsets no delivery report: [None, None, None, None, None, None]  (4.9 ms)
   ↑ repare: SEM offsets reais — o broker nunca respondeu. O report só diz 'saiu do client'.

acks=1    — o LÍDER escreveu (réplicas ainda não)
   offsets no delivery report: [15, 16, 17, 5, 10, 11]  (4.4 ms)

acks=all  — todas as réplicas IN-SYNC escreveram
   offsets no delivery report: [18, 19, 20, 6, 12, 13]  (3.8 ms)

E se eu pedir enable.idempotence com acks=1?
  ✗ o client RECUSA: KafkaError{...str="Failed to create producer: `acks` must be
    set to `all` when `enable.idempotence` is true"}
```

**O que observar** — dois detalhes que valem a cena:

1. **Com `acks=0`, os offsets vêm `None`.** O delivery report existe, mas só diz "saiu
   do client" — o broker nunca respondeu, então ninguém sabe *onde* (nem *se*) a
   mensagem foi parar. É a prova material do que `acks=0` significa.
2. **`enable.idempotence` exige `acks=all` — e o client recusa a combinação inválida**
   na criação do `Producer`, antes de qualquer mensagem. Idempotência sem confirmação
   de todas as réplicas seria promessa furada.

> ⚠️ No sandbox (1 broker, RF=1), `acks=1` e `acks=all` são equivalentes na prática —
> só há uma réplica. A diferença real aparece no Confluent Cloud (RF=3,
> `min.insync.replicas=2`), onde `acks=all` é o que garante que a mensagem sobrevive à
> queda do líder.

---

## Passo 4 — A Consumer API e o rebalance ao vivo *(~10 min)*

**Objetivo**: escrever o loop de um consumer — e assistir ao grupo se reorganizar.

As três decisões que definem um consumer, como código:

```python
{
    "bootstrap.servers": LOCAL_BOOTSTRAP,
    "group.id": GROUP_ID,             # DECISÃO 1 — quem divide o trabalho (Aula 2!)
    "auto.offset.reset": "earliest",  # DECISÃO 2 — grupo novo começa de onde?
    "enable.auto.commit": False,      # DECISÃO 3 — NÓS decidimos quando commitar
}
```

E o rebalance vira **evento observável** — os callbacks imprimem cada movimentação:

```python
consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

while True:
    msg = consumer.poll(timeout=1.0)      # busca mensagens E mantém o heartbeat
    if msg is None:
        continue
    processa(msg)
    consumer.commit(message=msg)          # commit DEPOIS de processar
```

A coreografia, em dois terminais. **Terminal 1**:

```bash
make aula4-api-consumer-a
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_consumer --nome A`

Aguarde ele processar as 6 mensagens, então suba o B no **terminal 2**
(`make aula4-api-consumer-b`), espere o rebalance, e derrube o B com **Ctrl+C**. O log
do A registra a história completa:

**Output esperado (consumidor A)**
```
[consumidor A] ⇄ REBALANCE — recebeu partição(ões): 0, 1, 2
[consumidor A] Partition:2 Offset:0  txn-2  AUTHORIZED 89.90
[consumidor A] Partition:1 Offset:0  txn-1  AUTHORIZED 150.00
[consumidor A] Partition:0 Offset:0  txn-3  DECLINED 1250.00
  ...
[consumidor A] ⇄ REBALANCE — devolveu partição(ões): 0, 1, 2   ← o B entrou
[consumidor A] ⇄ REBALANCE — recebeu partição(ões): 0, 1
[consumidor A] ⇄ REBALANCE — devolveu partição(ões): 0, 1      ← o B morreu
[consumidor A] ⇄ REBALANCE — recebeu partição(ões): 0, 1, 2    ← A herdou tudo
```

**O que observar** — três coisas:

1. **O rebalance tem duas fases**: o coordinator primeiro **revoga** (`devolveu`),
   depois **atribui** (`recebeu`). Entre as duas, ninguém consome — é a pausa do
   rebalance.
2. **`poll()` é o heartbeat**: o loop chama `poll(1.0)` continuamente mesmo sem
   mensagens. Um consumer que trava num processamento longo e para de chamar `poll()`
   é declarado morto — e causa exatamente o rebalance que você viu.
3. **Nenhuma partição ficou órfã.** Quando o B morreu, o A herdou o trabalho dele em
   segundos, sem intervenção — é isto que dá tolerância a falha ao consumo. O custo é
   a pausa; consumidores instáveis, entrando e saindo, causam rebalance em loop: um
   clássico problema de produção.

> ⚠️ Nunca é meio a meio: **3 partições não dividem por 2** (A ficou com 2, B com 1).
> A regra de ouro: *cada partição é lida por no máximo UM consumidor do grupo*. Um 4º
> consumidor ficaria **ocioso** — partições são o teto do paralelismo.

---

## Passo 5 — O grupo por dentro: o LAG *(~5 min)*

**Objetivo**: ler a posição do grupo pela API — sem consumir nada.

```bash
make aula4-api-lag
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_consumer --cena lag`

**Output esperado** (logo após o Passo 4)
```
PARTITION   CURRENT-OFFSET  LOG-END-OFFSET    LAG
0                        3               3      0
1                        1               1      0
2                        2               2      0

LAG total: 0  — o consumo alcançou a produção
```

As três colunas:

| Coluna | O que é | Na API |
|---|---|---|
| `CURRENT-OFFSET` | até onde o grupo **confirmou** | `consumer.committed(...)` |
| `LOG-END-OFFSET` | onde o log **termina** | `get_watermark_offsets(...)` |
| `LAG` | a diferença — o atraso do grupo | a subtração |

**Agora veja o LAG se mover.** Produza mais (sem consumir) e meça de novo:

```bash
make aula4-api-envios
make aula4-api-lag
```

(o `envios` produz mais 18 mensagens sem consumir; o `lag` mede de novo)

```
PARTITION   CURRENT-OFFSET  LOG-END-OFFSET    LAG
0                        3              12      9
1                        1               4      3
2                        2               8      6

LAG total: 18  — há mensagens que o grupo ainda não confirmou
```

Rode o consumidor A de novo (Passo 4) e meça outra vez: **zero**.

**O que observar**: `LAG = 0` estável é saúde; **LAG subindo** é o consumidor não dando
conta da produção — e é o alerta nº 1 de operação (o Curso 7 coroa essa métrica).
Repare que o observador **não entra no grupo**: ele só pergunta ao coordinator —
inspecionar não causa rebalance.

---

## Passo 6 — O commit: quem confirma, e quando *(~7 min)*

A pergunta de design por trás de todo consumer:

> O offset deve ser confirmado **antes** ou **depois** de processar?

| Ordem | Se o processo morrer no meio | Nome |
|---|---|---|
| Confirma → processa | Confirmada mas não processada → **perdida** | at-most-once |
| Processa → confirma | Processada mas não confirmada → **relida** | at-least-once |

E o **como** tem três respostas na API:

| Modo | Como | O risco |
|---|---|---|
| **Auto-commit** | um timer, a cada 5s (o **default**!) | commita o que você ainda não processou |
| **Manual síncrono** | `commit(msg)` bloqueante por mensagem | um round-trip por mensagem |
| **Manual assíncrono** | `commit(msg, asynchronous=True)` + barreira no `close()` | o veredito chega depois |

```bash
make aula4-api-commits
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_consumer --cena commits`

**Output esperado** (tempos variam; a ordem e a contagem, não)
```
auto-commit — um TIMER commita a cada 5s — processado ou não
   6 processadas · 0 chamadas de commit · 2.3 ms

manual síncrono — commit(msg) bloqueante após CADA mensagem
   6 processadas · 6 chamadas de commit · 7.8 ms

manual assíncrono — commit(msg, async) por mensagem + barreira no close()
   6 processadas · 6 chamadas de commit · 5.4 ms
```

**O que observar**: a coluna do meio é a lição. Com auto-commit há **zero** linhas de
commit no seu código — conveniente, e é exatamente aí que mora a janela: o timer pode
confirmar uma mensagem que você ainda não terminou de processar (at-most-once
disfarçado). O síncrono paga um round-trip por mensagem, como o producer síncrono do
Passo 2. O padrão de produção é o meio-termo: **manual** (você decide *quando* —
depois de processar), **assíncrono** no loop, **síncrono só na saída**.

**O ponto conceitual**: o commit é a fronteira entre *reprocessar* e *perder*. Como o
Kafka nunca pode perder, o padrão é **at-least-once** — e o preço é a duplicata, que a
Aula 5 mostra na tela e depois neutraliza.

---

## Passo 7 — De onde começar: `reset` e `assign` *(~7 min)*

Duas cenas curtas sobre posição de leitura.

**`auto.offset.reset`** — onde um grupo **novo** começa:

```bash
make aula4-api-reset
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_consumer --cena reset`

**Output esperado**
```
1) grupo novo com auto.offset.reset=earliest:
   leu 6 mensagens do HISTÓRICO: ['txn-2', 'txn-5', 'txn-1', 'txn-3', 'txn-4', 'txn-6']

2) grupo novo com auto.offset.reset=latest:
   partições atribuídas; o histórico ficou para trás. Produzindo 2 novas...
   leu 2 mensagens — SÓ as novas: ['txn-7', 'txn-8']
```

**O que observar**: mesma config, mesma inscrição — `earliest` leu o histórico
inteiro, `latest` só o que foi produzido **depois** dele entrar. Daqui saem duas
pegadinhas clássicas: *"meu consumidor não lê nada!"* (grupo novo com `latest` — que é
o **default** — num tópico parado) e o oposto (`earliest` num tópico de anos =
reprocessar anos no primeiro deploy). E o detalhe que pega todo mundo: o reset **só
vale para grupo sem offset commitado** — depois do primeiro commit, quem manda é o
offset do grupo.

**`assign()`** — leitura manual, sem grupo:

```bash
make aula4-api-assign
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_consumer --cena assign`

**Output esperado**
```
1) partição 0, do início (OFFSET_BEGINNING):
   Partition:0 Offset:0  txn-3  DECLINED 1250.00
   Partition:0 Offset:1  txn-4  SETTLED 42.00
   Partition:0 Offset:2  txn-6  DECLINED 75.00

2) partição 0, a partir do offset 1 — replay parcial:
   Partition:0 Offset:1  txn-4  SETTLED 42.00
   Partition:0 Offset:2  txn-6  DECLINED 75.00
```

**O que observar**: nenhum `on_assign` disparou, nenhum grupo se reorganizou — com
`assign()` a posição é 100% sua (é o par do `--partition 0` do CLI da Aula 3, e a base
de qualquer ferramenta de inspeção e replay). O preço: paralelismo, failover e
progresso commitado viram problema **seu** — é tudo o que o grupo dava de graça.

> 💡 O reposicionamento de um grupo **existente** (o `--reset-offsets` da operação)
> está na [versão CLI](cli.md) — é ferramenta de socorro, não de aplicação.

---

## Passo 8 — Fixe os conceitos *(~4 min)*

1. Um tópico tem 3 partições e você sobe 5 consumidores no mesmo grupo. Quantos ficam ociosos?
2. Um consumidor morre. As mensagens da partição dele são perdidas?
3. `CURRENT-OFFSET=100`, `LOG-END-OFFSET=350`. Qual o LAG e o que ele indica?
4. Seu consumer usa o default (auto-commit de 5s) e processa cada mensagem em 200 ms. Onde mora o risco?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Dois.** Cada partição é lida por no máximo um consumidor do grupo. Com 3
partições, o paralelismo máximo é 3 — os outros 2 ficam de braços cruzados. Para
escalar além disso, é preciso **mais partições** (decisão de infraestrutura, não de
escala do time).

**2. Não.** As mensagens estão **no log do broker**, não no consumidor. O que se
perde é a *posição de leitura em memória* — e mesmo essa está commitada no grupo. No
rebalance, outro membro assume a partição e continua do último offset confirmado. Foi
o Passo 4.

**3. LAG = 250.** O grupo está 250 mensagens atrás do fim do log. Um LAG estável é
normal; **LAG crescendo** significa que a produção está mais rápida que o consumo — e
é o alerta nº 1 de operação (Curso 7).

**4. Na janela entre o poll e o timer.** Em 5 segundos a 200 ms/mensagem, o consumer
pode ter **até 25 mensagens processadas e não commitadas** (morreu → relidas,
duplicata) — ou, pior, o timer pode commitar mensagens que o poll entregou e você
**ainda não processou** (morreu → perdidas). O Passo 6 existe para isso: commit
manual, depois de processar, devolve o controle a você.

</details>

---

## Se algo não funcionou

**`ModuleNotFoundError: No module named 'demos'` (ou `confluent_kafka`)**
O venv não está instalado ou ativo. Rode `make install` (Windows: os comandos do
[índice](../../README.md)) e execute pelos alvos `make` ou pelo ▶ do PyCharm.

**`Connection refused` / `broker transport failure`**
O sandbox não está no ar. `make up` (Windows: `docker compose -f deployment/docker/docker-compose.yml up -d`).

**`Failed to acquire idempotence PID ... retrying` na primeira execução**
O broker recém-subido ainda carregando o coordinator — o retry resolve sozinho.

**O rebalance do Passo 4 não aparece**
Confira se A e B usam o **mesmo grupo** (é o default — não passe `--nome` como grupo).
Se o B foi morto de forma abrupta (janela fechada, `kill -9`), o coordinator espera o
`session.timeout.ms` (6s nesta demo) antes de redistribuir — o Ctrl+C limpo chama
`close()` e o rebalance é imediato.

**O LAG mostra `-` em CURRENT-OFFSET**
O grupo nunca commitou naquela partição. Rode o consumidor A (Passo 4) e meça de novo.

**As cenas leem 0 mensagens**
O tópico foi limpo (teardown) e o seed automático repõe só na próxima cena — rode de
novo. Persistindo, `make aula4-api-teardown` e recomece do Passo 1.

---

## Limpar

```bash
make aula4-api-teardown
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula4_api_producer --teardown`

Remove o tópico **e os consumer groups** da aula (os offsets guardados fariam as cenas
se comportarem como "2ª execução" numa próxima passada).

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

## Bônus

**A versão linha de comando** → [cli.md](cli.md): a mesma coreografia com
`kafka-console-*`, consumidores em containers (`docker run` + `docker stop`), o
`kafka-consumer-groups --describe` e o `--reset-offsets --dry-run/--execute` — a caixa
de ferramentas de operação.

**A demo cinematográfica** — produção lenta e contínua, feita para gravação:

```bash
make aula4-setup
make aula4-producer
make aula4-consumer-a
make aula4-consumer-b
make aula4-lag
make aula4-teardown
```

| Alvo | Terminal |
|---|---|
| `aula4-producer` | 1 — fluxo lento e visível |
| `aula4-consumer-a` | 2 |
| `aula4-consumer-b` | 3 — dispara o rebalance |
| `aula4-lag` | 4 |

> 🪟 Os alvos `make` exigem o utilitário `make` — no Windows, rode pelo **WSL** ou
> chame os módulos com `.venv\Scripts\python -m ...` como nos passos acima.

---

## Glossário

| Termo | Definição |
|---|---|
| **Consumer group** | Time de consumidores dividindo as partições de um tópico. |
| **Group coordinator** | O broker que gerencia membros e atribuições do grupo. |
| **Rebalance** | Redistribuição das partições quando alguém entra ou sai do grupo. |
| **Delivery callback** | A função que recebe o veredito de cada `produce()` — sucesso ou erro. |
| **`acks`** | Quantas confirmações o producer exige: `0`, `1` ou `all`. |
| **`session.timeout.ms`** | Tempo sem heartbeat até o membro ser considerado morto. |
| **`auto.offset.reset`** | Onde um grupo **sem** offset commitado começa: `earliest`/`latest`. |
| **`assign()`** | Atribuição manual de partição/offset — sem grupo, sem rebalance. |
| **CURRENT-OFFSET** | Até onde o grupo confirmou leitura, por partição. |
| **LOG-END-OFFSET** | Onde o log termina, por partição. |
| **LAG** | `LOG-END-OFFSET − CURRENT-OFFSET`: o atraso do grupo. |
| **Commit** | Registro do progresso; a fronteira entre reprocessar e perder. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| at-least-once e a duplicata | Aula 5 — idempotência |
| A posição do commit decidindo a semântica | Aula 5 — o crash executável |
| LAG como alerta | Curso 7 — operação e Metrics API |
| Rebalance cooperativo (sem stop-the-world) | Curso 7 — tuning |

**Fontes**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.*, cap. 3–4 — [e-book gratuito](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- Confluent — [confluent-kafka-python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- Apache Kafka — [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- Confluent — [Consumer group protocol](https://developer.confluent.io/courses/architecture/consumer-group-protocol/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 3 · O log do Kafka](../aula3/README.md) | [Curso 1](../README.md) | [Aula 5 · Ordem, idempotência e entrega](../aula5/README.md) |
