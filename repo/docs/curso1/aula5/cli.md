# Aula 5 — a versão linha de comando

> **Referência CLI** · O guia principal da aula é em Python → [README.md](README.md)

As cenas de ordem por chave, ordem quebrada e duplicata do at-least-once, feitas com o
`kafka-console-producer/consumer` e o `kafka-consumer-groups --reset-offsets` — sem uma
linha de Python. Útil para inspecionar e reproduzir os fenômenos direto do terminal.

Pré-requisitos: sandbox no ar (`make up`), **sem** precisar do venv — aqui é só Docker.

---

## Passo 1 — Ordem por chave *(~8 min)*

**Objetivo**: provar que eventos da mesma transação chegam na ordem, mesmo produzidos misturados.

Crie o tópico (mesmo comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-topics --create --topic transacoes-ordem --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

Agora o ponto: você vai produzir o ciclo de vida de **3 transações intercaladas** — como aconteceria num sistema real, onde várias transações correm em paralelo. Num comando só:

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic transacoes-ordem \
  --property "parse.key=true" --property "key.separator=:" \
  --bootstrap-server localhost:9092 <<'EOF'
txn-1001:CREATED 150.00
txn-1002:CREATED 89.90
txn-1001:AUTHORIZED 150.00
txn-1003:CREATED 1250.00
txn-1002:AUTHORIZED 89.90
txn-1001:SETTLED 150.00
txn-1003:DECLINED 1250.00
txn-1002:SETTLED 89.90
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-1001:CREATED 150.00
txn-1002:CREATED 89.90
txn-1001:AUTHORIZED 150.00
txn-1003:CREATED 1250.00
txn-1002:AUTHORIZED 89.90
txn-1001:SETTLED 150.00
txn-1003:DECLINED 1250.00
txn-1002:SETTLED 89.90
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic transacoes-ordem --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092
```

Repare: a `txn-1001` aparece nas linhas 1, 3 e 6 — **espalhada no meio das outras**. Agora leia:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes-ordem --from-beginning --timeout-ms 9000 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes-ordem --from-beginning --timeout-ms 9000 --property print.key=true --property print.partition=true --property print.offset=true --bootstrap-server localhost:9092
```

**Output esperado**
```
Partition:2	Offset:0	txn-1001	CREATED 150.00
Partition:2	Offset:1	txn-1001	AUTHORIZED 150.00
Partition:2	Offset:2	txn-1001	SETTLED 150.00
Partition:0	Offset:0	txn-1002	CREATED 89.90
Partition:0	Offset:1	txn-1003	CREATED 1250.00
Partition:0	Offset:2	txn-1002	AUTHORIZED 89.90
Partition:0	Offset:3	txn-1003	DECLINED 1250.00
Partition:0	Offset:4	txn-1002	SETTLED 89.90
```

**O que observar** — duas coisas, e as duas importam:

1. **`txn-1001` viveu inteira na partição 2**, na ordem exata do negócio: `CREATED → AUTHORIZED → SETTLED`. Você digitou intercalado; o Kafka entregou ordenado. **A chave garantiu isso.**
2. **`txn-1002` e `txn-1003` dividem a partição 0** e aparecem intercaladas entre si — mas cada uma preserva **sua própria** sequência. A ordem é garantida por chave, não entre chaves.

> 💡 O mecanismo: `partição = hash(chave) % número_de_partições`. Mesma chave → sempre
> a mesma partição → e dentro da partição a ordem é a de escrita (Aula 3).

---

## Passo 2 — Quebre a ordem de propósito *(~6 min)*

**Objetivo**: ver o que acontece quando ninguém define a chave.

Crie o tópico (mesmo comando nos dois sistemas):

```bash
docker exec bootcamp-kafka kafka-topics --create --topic transacoes-semchave --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

O mesmo ciclo de vida, agora **sem chave** (sem `parse.key`, e forçando distribuição round-robin):

**macOS / Linux**
```bash
docker exec -i bootcamp-kafka kafka-console-producer \
  --topic transacoes-semchave \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
  --bootstrap-server localhost:9092 <<'EOF'
txn-1001 CREATED
txn-1002 CREATED
txn-1001 AUTHORIZED
txn-1003 CREATED
txn-1002 AUTHORIZED
txn-1001 SETTLED
EOF
```

**Windows (PowerShell)**
```powershell
@'
txn-1001 CREATED
txn-1002 CREATED
txn-1001 AUTHORIZED
txn-1003 CREATED
txn-1002 AUTHORIZED
txn-1001 SETTLED
'@ | docker exec -i bootcamp-kafka kafka-console-producer --topic transacoes-semchave --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --bootstrap-server localhost:9092
```

Leia:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes-semchave --from-beginning --timeout-ms 8000 \
  --property print.partition=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes-semchave --from-beginning --timeout-ms 8000 --property print.partition=true --bootstrap-server localhost:9092
```

**Output esperado**
```
Partition:1	txn-1002 CREATED
Partition:1	txn-1003 CREATED
Partition:1	txn-1001 SETTLED
Partition:0	txn-1001 CREATED
Partition:0	txn-1001 AUTHORIZED
Partition:0	txn-1002 AUTHORIZED
```

**O que observar** — e este é o momento em que a aula "dói":

A `txn-1001` foi parar em **duas partições diferentes**. Na leitura, o `SETTLED`
(partição 1) apareceu **antes** do `CREATED` (partição 0).

Um consumidor que processasse nessa ordem veria uma transação sendo **liquidada antes
de ser criada**. Em pagamentos, isso é um bug de produção com consequência financeira.

> ⚠️ **A lição**: a ordem não é um presente do Kafka — é uma consequência da chave que
> você escolheu. Sem chave, não há garantia nenhuma entre partições.

---

## Passo 3 — A duplicata do at-least-once *(~8 min)*

**Objetivo**: ver a mesma mensagem sendo processada duas vezes.

Consuma o tópico ordenado com um grupo:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes-ordem --group liquidacao --from-beginning --timeout-ms 8000 \
  --property print.key=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes-ordem --group liquidacao --from-beginning --timeout-ms 8000 --property print.key=true --bootstrap-server localhost:9092
```

Você vê as 8 mensagens. Confirme que o grupo commitou o progresso (mesmo comando nos
dois sistemas):

```bash
docker exec bootcamp-kafka kafka-consumer-groups --describe --group liquidacao --bootstrap-server localhost:9092
```
```
GROUP       TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
liquidacao  transacoes-ordem  0          5               5               0
liquidacao  transacoes-ordem  1          0               0               0
liquidacao  transacoes-ordem  2          3               3               0
```

### Agora simule o crash

Imagine que o consumidor **processou o lote e morreu antes de commitar**. O efeito é
exatamente este: o offset volta para onde estava.

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-consumer-groups \
  --reset-offsets --to-earliest \
  --group liquidacao --topic transacoes-ordem \
  --execute --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-consumer-groups --reset-offsets --to-earliest --group liquidacao --topic transacoes-ordem --execute --bootstrap-server localhost:9092
```

Suba o consumidor de novo — **sem** `--from-beginning`, porque o grupo já tem posição:

**macOS / Linux**
```bash
docker exec bootcamp-kafka kafka-console-consumer \
  --topic transacoes-ordem --group liquidacao --timeout-ms 8000 \
  --property print.key=true \
  --bootstrap-server localhost:9092
```

**Windows (PowerShell)**
```powershell
docker exec bootcamp-kafka kafka-console-consumer --topic transacoes-ordem --group liquidacao --timeout-ms 8000 --property print.key=true --bootstrap-server localhost:9092
```

**Output esperado**
```
txn-1001	CREATED 150.00
txn-1001	AUTHORIZED 150.00
txn-1001	SETTLED 150.00
txn-1002	CREATED 89.90
txn-1003	CREATED 1250.00
txn-1002	AUTHORIZED 89.90
txn-1003	DECLINED 1250.00
txn-1002	SETTLED 89.90
```

**O que observar**: **as mesmas 8 mensagens, de novo.** Se cada uma tivesse debitado
uma conta, você teria acabado de cobrar o cliente duas vezes.

> **Isto não é um bug.** É o contrato **at-least-once** sendo cumprido: *"não perder
> nunca, ao custo de repetir às vezes"*. O Kafka preferiu entregar duas vezes a
> arriscar não entregar.

A alternativa (commitar **antes** de processar) seria **at-most-once**: sem duplicata,
mas com risco de perder. Em pagamentos, perder é pior.

---

---

## Se algo não funcionou

**A `txn-1001` não ficou toda na mesma partição (Passo 1)**
Faltou `--property parse.key=true --property key.separator=:`. Sem isso, a linha
inteira vira o valor e a chave fica nula.

**No Passo 3 o consumidor não mostra nada na segunda leitura**
O `--reset-offsets` falha se o grupo tiver membros ativos. Encerre o consumidor
(Ctrl+C), espere alguns segundos e repita o reset.

**`Error: Assignments can only be reset if the group is inactive`**
Mesmo caso acima — é a proteção que você viu na Aula 4.

**A partição escolhida no meu terminal é diferente do guia**
Normal se você digitou chaves diferentes. O que importa é o **padrão**: mesma chave,
mesma partição, ordem preservada.

---

---

## Limpar

Os três primeiros comandos são idênticos nos dois sistemas; o último muda:

```bash
docker exec bootcamp-kafka kafka-consumer-groups --delete --group liquidacao --bootstrap-server localhost:9092
docker exec bootcamp-kafka kafka-topics --delete --topic transacoes-ordem --bootstrap-server localhost:9092
docker exec bootcamp-kafka kafka-topics --delete --topic transacoes-semchave --bootstrap-server localhost:9092

make aula5-teardown
```

> 🪟 Windows: `.venv\Scripts\python -m demos.curso1.aula5_ordem_e_idempotencia teardown`

---

---

| ↑ Guia principal (Python) |
|---|
| [README.md](README.md) |
