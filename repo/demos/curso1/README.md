# Curso 1 · Roteiro de gravação das demos

Demos do Bloco 1 (Fundamentos de EDA e Kafka). Todas rodam no **sandbox local** (Kafka KRaft single-broker) — sem Confluent Cloud, sem gastar créditos.

Domínio: **payments** — as mesmas transações que virarão CDC (Curso 3), Flink (Curso 4) e fraude (Curso 6). O aluno acompanha um dado só do começo ao fim.

## Antes de gravar

```bash
make up          # sobe Kafka + Schema Registry (~30s)
make status      # confirma que o broker responde
```

Ao terminar: `make curso1-teardown` (remove os topics) ou `make down` (destrói tudo).

> ⚠️ **Diga em voz alta na gravação**: este sandbox tem **1 broker**, então RF=1 e não existe ISR. As âncoras de confiabilidade do bootcamp (RF=3, `min.insync.replicas=2`) são **conceito** aqui e **realidade** a partir do Curso 2, no Confluent Cloud.

---

## Aula 2 — Message Queue vs Data Streaming (~5 min)

A tese: *mesma plataforma, mesmo topic — muda só o `group.id` e a semântica muda junto.*

| Passo | Terminal | Comando |
|---|---|---|
| Setup | 1 | `make aula2-setup` |
| **Cena 1 — fila** | 1 e 2 | `make aula2-cena1a` / `make aula2-cena1b` (mesmo grupo) |
| ↳ publicar | 3 | `make aula2-produce` |
| **Cena 2 — streaming** | 1 e 2 | `make aula2-cena2a` / `make aula2-cena2b` (grupos diferentes) |
| ↳ publicar | 3 | `make aula2-produce` |
| **Cena 3 — replay** | 1 | `make aula2-cena3` |

**O que apontar na tela:**
- Cena 1: as mensagens se **dividem** — nenhuma aparece nos dois consumidores
- Cena 2: **os dois recebem tudo** — só mudou o grupo
- Cena 3: um consumidor que **chegou depois** lê o histórico inteiro. Numa fila tradicional, encontraria o vazio.

---

## Aula 3 — O log do Kafka (~5–6 min)

| Cena | Comando | O que observar |
|---|---|---|
| 1 | `make aula3-cena1` | 3 partições no describe + retention herdada |
| 2 | `make aula3-cena2` | Sem chave → espalhamento; **cada partição tem offsets a partir do 0** |
| 3 | `make aula3-cena3` | Arquivos `.log` e `.index` no disco — a partição é **arquivo de verdade** |
| 4 | `make aula3-cena4` | Duas leituras, **mesmos offsets** — o log não muda, quem move é o leitor |
| 5 | `make aula3-cena5` | Tamanho de cada partição **sem ler nenhuma mensagem** |

---

## Aula 4 — Producers, Consumers e rebalance (~5–6 min)

A demo mais visual: precisa de **4 terminais**.

```bash
make aula4-setup           # uma vez

# terminal 1 — deixe rodando o tempo todo
make aula4-producer

# terminal 2 — consumidor A sozinho: pega as 3 partições
make aula4-consumer-a

# terminal 3 — entra o B: REBALANCE na tela
make aula4-consumer-b

# terminal 4 — o lag como dado
make aula4-lag
```

**Sequência da gravação:**
1. A sozinho → recebe as 3 partições (`⇄ REBALANCE — recebeu partição(ões): 0, 1, 2`)
2. Entra B → ambos imprimem rebalance; partições se dividem
3. **Ctrl+C no B** → A imprime novo rebalance e herda tudo. Nenhuma mensagem órfã.
4. `make aula4-lag` → current offset, log end offset e **LAG** por partição

> Plantar aqui: *"no Curso 7, esse número de lag vira o principal alerta de produção."*

---

## Aula 5 — Ordem, idempotência e entrega (~5–6 min)

| Cena | Comando | O que observar |
|---|---|---|
| 1 | `make aula5-cena1` | 3 transações **intercaladas** na produção; no consumo, cada uma viveu **inteira numa partição, na ordem de negócio** |
| 2 | `make aula5-cena2` (**rode 2×**) | Na 2ª execução os **mesmos eventos voltam** — a duplicata do at-least-once, na tela |
| 3 | `make aula5-cena3` | Mesma releitura, mas o **efeito não se repete** — duplicata inofensiva |

**A fala da cena 2:** isto não é bug. É a semântica cumprindo o contrato *"não perder nunca, ao custo de repetir às vezes"*. A cena 3 é o antídoto — e é o mesmo desenho que o `insert.mode=upsert` faz num sink connector (Curso 3, Aula 3).

---

## Scripts de API (Aulas 4–5 · guia do aluno)

Além das cenas de gravação acima, três módulos ensinam a **anatomia da API** — config
montada campo a campo, para ler tanto quanto rodar. São referenciados pelos guias do
aluno (`docs/curso1/aula4` Passo 7 e `aula5` Passo 6):

| Módulo | Alvos make |
|---|---|
| `aula4_api_producer.py` | `aula4-api-producer`, `aula4-api-envios` (3 padrões de envio), `aula4-api-acks` (escada 0/1/all), `aula4-api-teardown` |
| `aula4_api_consumer.py` | `aula4-api-consumer-a/b` (rebalance), `aula4-api-commits` (3 jeitos de commitar), `aula4-api-reset` (earliest×latest), `aula4-api-assign` (leitura manual), `aula4-api-lag` (tabela de LAG via API) |
| `aula5_api_entrega.py` | `aula5-api-ordem`, `aula5-api-sem-chave`, `aula5-api-crash-antes/depois`, `aula5-api-dedup`, `aula5-api-teardown` |

O `crash --commit antes|depois` roda **duas vezes o mesmo comando**: a 1ª execução
morre no offset 3, a 2ª mostra o veredito (perdido × duplicado).

## Solução de problemas

### O broker não sobe / `make up` trava
```bash
docker compose -f deployment/docker/docker-compose.yml logs kafka | tail -30
make down && make up          # reset limpo (apaga os volumes)
```

### "Topic already exists" ao rodar um setup
Esperado e inofensivo — os scripts são idempotentes. Para começar do zero: `make curso1-teardown`.

### O consumidor não recebe nada
Cheque se o topic tem dados: `make aula3-cena5` mostra o tamanho de cada partição. Consumidores das cenas de streaming usam `auto.offset.reset=latest` — **produza depois de subir o consumidor**, ou use a cena de replay (`--from-beginning`).

### O rebalance da Aula 4 demora
`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` já está no compose para acelerar. Se ainda demorar, é o `session.timeout.ms` (45s default) esperando o consumidor morto — o que é exatamente o conceito da cena 3.
