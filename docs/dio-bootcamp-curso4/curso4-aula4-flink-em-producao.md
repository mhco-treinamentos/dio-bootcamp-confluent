# Curso 4 · Aula 4 — Apache Flink em Produção: Stateful Processing, Checkpoints e Tolerância a Falhas

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aulas 1–3 do Curso 4

---

## 1. Objetivo da Aula

Fechar o curso com a pergunta de produção: **o que acontece com o estado quando o processo cai?** Dissecar onde o estado vive, como os **checkpoints** tiram a foto consistente (estado + posição nos offsets), como a recuperação funciona, e como o Flink entrega **exactly-once de ponta a ponta** — amarrando de vez as garantias do Curso 1. E delimitar, no Confluent Cloud, o que é gerenciado e o que continua sendo seu.

---

## 2. Conceitos Simplificados

### 2.1 O estado: o tesouro acumulado *(~3 min)*

Toda construção da Aula 3 **acumula memória**:

- A janela tumbling aberta guarda **a soma parcial** do minuto corrente
- O interval join guarda **os eventos esperando o par**
- O join de enriquecimento guarda **a tabela de clientes materializada**
- A agregação updating guarda **o valor corrente por chave**

Esse acumulado é o **estado** — e ele é caro: horas (às vezes dias) de processamento condensadas. Ele vive no **state backend** (Aula 1: RocksDB nos TaskManagers — no CC, gerenciado), e perdê-lo numa falha significaria **recontar tudo do zero** (reler o log inteiro) ou, pior, **responder errado** (janelas pela metade). A pergunta da aula: **como falhar sem perder o tesouro?**

### 2.2 Anatomia do checkpoint: a foto consistente *(~5 min)*

A resposta do Flink é o **checkpoint** — periodicamente, uma foto **consistente** do job inteiro:

```
 O QUE A FOTO CONTÉM (e por que os dois juntos):
 ┌────────────────────────────────────────────────┐
 │ 1. o ESTADO de cada operador                   │
 │    (somas parciais, janelas abertas, joins)    │
 │ 2. a POSIÇÃO de leitura nos tópicos            │
 │    (os offsets — o marcador de página!)        │
 └────────────────────────────────────────────────┘
     salvos JUNTOS, do MESMO instante lógico

 COMO, sem parar o mundo: a BARREIRA
 offsets ──►│barreira│──► operadores ──► saída
 uma marca flui PELO stream junto com os dados;
 quando ela passa por um operador, ele fotografa
 seu estado — todos fotografam o MESMO ponto do
 fluxo, sem pausar o processamento
```

- **Estado + offsets juntos é o pulo do gato**: a foto diz "processei até o offset 4.317 e ESTE era o estado" — os dois casam perfeitamente
- Quem coordena a foto é o **JobManager** (Aula 1) — ele injeta a barreira na entrada e coleta as confirmações de cada operador; no CC, tudo isso acontece invisível
- É o **commit de offset do Curso 1, Aula 4, elevado a sistema**: lá, o consumidor marcava a página; aqui, o Flink marca a página **e** fotografa tudo que estava na cabeça ao ler até ela
- A barreira (o algoritmo de snapshot distribuído do Flink) permite tirar a foto **sem parar o fluxo** — nível bootcamp: entender que a consistência vem de "todos fotografam o mesmo ponto lógico"

### 2.3 A recuperação: cair sem perder *(~3 min)*

```
 FALHA no instante T
   │
   ├─ 1. restaurar o ESTADO do último checkpoint (ex.: de T-30s)
   ├─ 2. rebobinar a leitura para os OFFSETS da mesma foto
   └─ 3. reprocessar os últimos ~30s → alcançar o presente → seguir
```

- Perde-se no máximo **o intervalo desde o último checkpoint** de processamento — nunca o tesouro acumulado
- O replay do log (Curso 1, Aula 3!) é o que torna o passo 2 possível: **checkpoint só funciona porque o Kafka retém** — as peças do bootcamp se encaixando
- Mas atenção: reprocessar os últimos 30 s significa **produzir saídas de novo**... olá, duplicata (at-least-once). O que nos leva à última peça:

### 2.4 Exactly-once de ponta a ponta *(~4 min)*

O Curso 1 (Aula 5) apresentou o EOS como "reenvia + deduplica, com custo". Agora, o mecanismo completo no pipeline Flink:

```
 tópico A ──► FLINK (estado + checkpoint) ──► tópico B
              │
              │ a saída para o tópico B é escrita em TRANSAÇÃO Kafka
              │ a transação COMMITA junto com o checkpoint
              ▼
 falhou antes do checkpoint? transação ABORTA → a saída duplicada
 nunca fica visível para consumidores read_committed
```

- **Checkpoint + transação Kafka comitando juntos** = o reprocesso pós-falha não vaza duplicata: as escritas "tentativas" morrem com o abort
- É o casamento das peças do Curso 1, Aula 5: idempotência (produtor do Flink), transações (a saída atômica) e `isolation.level=read_committed` (o consumidor que só vê o confirmado)
- O preço honesto: a saída ganha a latência do intervalo de checkpoint (resultados confirmam em "batidas") — exactly-once não é grátis, é **contabilizado**
- Síntese do bootcamp: *at-least-once + idempotência* resolve a maioria (Cursos 1 e 3); **exactly-once transacional** é para quando duplicata é inaceitável e o custo de latência cabe (financeiro, billing)

### 2.5 No Confluent Cloud: o que é da plataforma, o que é seu *(~3 min)*

| Gerenciado (plataforma) | Seu (sempre) |
|---|---|
| Checkpointing automático (intervalo, storage, tuning) | A **lógica** — SQL correto, janelas certas, watermark declarada |
| Recuperação de falhas e re-execução de statements | **Dimensionar o pool** (teto de CFUs) e observar o autoscaling |
| Upgrades do motor, JobManager/TaskManager invisíveis | **Monitorar statements** — status, CFUs, lag de consumo (Curso 7) |
| Estado: backend, réplicas, snapshots | **Evolução das queries** — parar/substituir statements com critério |
| | **Custo** — statement perpétuo esquecido queima crédito para sempre |

A fronteira de gestão (Curso 2, Aula 1) fechando o ciclo: o self-managed Flink exige operar todo o universo da coluna esquerda — o serverless o faz sumir, e sobra o que sempre foi seu: **pensar certo sobre dados e tempo**.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: uma agregação stateful (a tumbling da Aula 3, janela larga o suficiente pra acumular estado visível) + Datagen. Tempo de tela: ~6 min. **Teardown completo — fecha o curso.**

**O que a demo deve mostrar, nesta ordem:**

1. **Statement stateful acumulando** — a agregação rodando via `INSERT INTO` (perpétuo), com resultado parcial crescendo visivelmente (ex.: contagem/soma numa janela de 10 min, ainda aberta).
   - **Observação-alvo:** apontar na tela **onde está o tesouro** — a janela aberta com minutos de acumulado.

2. **A "falha"** — parar e **resumir** o statement (o mecanismo exato — stop/resume do statement — a cargo do projeto de demos), com o Datagen produzindo o tempo todo.
   - **Observação-alvo:** o momento-síntese do curso — ao voltar, o resultado **continua de onde estava**: nada recontado do zero, os eventos produzidos durante a pausa **incluídos** (o rebobinar dos offsets funcionando). O aluno nunca configurou checkpoint nenhum — a plataforma o fez.

3. **Inspecionar o statement como recurso de produção** — status, CFUs consumidos, o autoscaling do pool, mensagens de exceção (se houver).
   - **Observação-alvo:** a coluna "seu" da tabela 2.5 na prática — o que se monitora num Flink serverless.

4. **Teardown completo do curso** — parar todos os statements e o Datagen, conferir o pool de volta a 0 CFU, billing check.

**Fechamento na tela** *(~1 min)*: "estado que sobrevive, contas que não se perdem nem se duplicam, e uma plataforma que carrega o peso operacional. O Bloco 3 continua: no Curso 5, os resultados do Flink deixam de ser só tópicos e viram **tabelas Iceberg no Lakehouse** — Tableflow. O shift-left começa agora."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Se cair, o Flink reconta tudo do log."** Não — restaura o checkpoint e rebobina só o intervalo desde a última foto. Recontar tudo é o que acontece **sem** checkpoint (e é por isso que stream processing caseiro dói).
- **"Checkpoint é backup."** Backup protege contra perda de dados; checkpoint protege a **continuidade do processamento** — estado + posição, casados, para retomar. Propósitos diferentes.
- **"Exactly-once é mágica / é default universal."** É checkpoint + transação, com custo de latência declarado. O bootcamp fecha o arco: at-most/at-least/exactly-once agora têm mecanismo completo, não só definição (Curso 1 → Curso 3 → aqui).
- **"Serverless = não preciso pensar em nada."** A plataforma opera o motor; a **correção** (janela certa, watermark certa, query certa) e o **custo** (statements vivos) continuam sendo engenharia sua. A tabela 2.5 é o contrato.
- **Disciplina de escopo:** savepoints, state TTL, tuning de intervalo de checkpoint e RocksDB são o universo self-managed/intermediate — citar que existem e que o CC os abstrai/gerencia.

**Ganchos com a trilha:**

- Garantias de entrega consolidadas → **Curso 7, Aula 5** (a visão de produção completa, com DR).
- Monitorar statements e lag → **Curso 7, Aula 2**.
- `pedidos_enriquecidos` e as agregações → insumo direto do **Curso 5** (Tableflow) e **Curso 6** (Real-Time AI).
- No **Desafio Final**, o README deve documentar a semântica de entrega escolhida — esta aula dá o vocabulário para justificá-la.

**Perguntas para checar entendimento:**

1. Um job com 6 horas de estado acumulado cai. Com checkpoints a cada 60 s, o que se perde, o que se reprocessa e por que o resultado final não duplica no tópico de saída?
2. Por que o checkpoint precisa salvar estado E offsets **do mesmo instante** — o que quebraria se fossem fotos de momentos diferentes?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Stateful processing** | Processamento que mantém memória entre eventos (janelas, joins, agregados). |
| **Checkpoint** | Foto periódica e consistente do estado + offsets de leitura, para recuperação. |
| **Barreira (checkpoint barrier)** | Marca que flui pelo stream sincronizando a foto de todos os operadores no mesmo ponto lógico. |
| **Recuperação (recovery)** | Restaurar o último checkpoint e rebobinar os offsets para retomar sem perda. |
| **Exactly-once end-to-end** | Checkpoint + transações Kafka comitando juntos: reprocesso sem duplicata visível. |
| **Transação Kafka (na saída do Flink)** | Escritas atômicas que só ficam visíveis com o commit do checkpoint. |
| **read_committed** | Isolamento do consumidor que ignora escritas de transações abortadas (Curso 1, Aula 5). |
| **Autoscaling (CFU)** | Ajuste automático da capacidade do compute pool dentro do teto configurado. |
| **Savepoint** | Foto manual e portável do estado (universo self-managed/avançado — citado). |

---

## 6. Referências

**Livros:**

- Tyler Akidau et al. — *Streaming Systems* (O'Reilly) — cap. 5: exactly-once e consistência em stream processing
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11 (tolerância a falhas em streams)

**Documentação e artigos oficiais:**

- Apache Flink — *Stateful Stream Processing (checkpoints, barriers)* — https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/
- Apache Flink — *Fault Tolerance via State Snapshots* — https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/
- Confluent Cloud — *Flink: Delivery Guarantees e latência de checkpoint* — https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html
- Confluent Cloud — *Monitor Flink Statements* — https://docs.confluent.io/cloud/current/flink/operate-and-deploy/monitor-statements.html
- Paris Carbone et al. — *Lightweight Asynchronous Snapshots for Distributed Dataflows* (o paper do algoritmo de checkpoint do Flink) — https://arxiv.org/abs/1506.08603
