# Curso 1 · Aula 4 — Produtores e Consumidores na Prática: Consumer Groups, Rebalance e Commit de Offset

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~20 min (teto) · Conceito + demo (sandbox Docker — implementação no projeto de demos)

---

## 1. Objetivo da Aula

Abrir a anatomia dos dois clientes do Kafka — **Producer API** e **Consumer API** — entendendo o caminho interno de uma escrita e de uma leitura e as configurações que governam cada um; e dominar a mecânica de escala da leitura: **consumer groups**, **rebalance** e **commit de offset**.

---

## 2. Conceitos Simplificados

### 2.1 Anatomia do Producer API *(~4 min)*

Um `send()` não vai direto pro broker — ele percorre uma linha de montagem:

```
send(record)
   │
   ├─► serializer        → converte key e value em bytes
   ├─► partitioner       → decide a partição (hash da chave | sticky sem chave)
   ├─► record accumulator→ acumula em BATCHES por partição (buffer em memória)
   │
   └─► sender thread     → envia os batches ao broker (em background)
                broker ──► ack ──► callback (sucesso | erro/retry)
```

Duas ideias-chave da anatomia:

- **O envio é assíncrono e em lote** — o produtor agrupa mensagens antes de enviar. É daí que vem o throughput do Kafka
- **O produtor é quem decide a partição** — o broker recebe a mensagem já endereçada (a chave da Aula 5 age no *partitioner*)

**Configurações de referência — Producer:**

| Config | Grupo | O que faz | Ponto didático |
|---|---|---|---|
| `bootstrap.servers` | Conexão | Endereços iniciais do cluster | Basta um broker alcançável; o cluster se apresenta inteiro |
| `key.serializer` / `value.serializer` | Formato | Convertem objeto → bytes | Kafka só transporta bytes; o formato é contrato dos clientes (gancho: Schema Registry, Curso 2) |
| `acks` | Confiabilidade | Confirmação exigida: `0`, `1`, `all` | Peça central da Aula 5; default moderno = `all` |
| `retries` | Confiabilidade | Reenvios automáticos em falha transitória | Com idempotência ativa, retry não duplica |
| `enable.idempotence` | Confiabilidade | Broker deduplica reenvios do produtor | **Default `true`** nas versões atuais (Aula 5 aprofunda) |
| `batch.size` | Performance | Tamanho máximo do batch por partição (bytes) | Batch maior = mais throughput, mais memória |
| `linger.ms` | Performance | Quanto o produtor **espera** para encher o batch | O trade-off na veia: esperar 5–10 ms pode multiplicar o throughput |
| `compression.type` | Performance | Compressão do batch (`lz4`, `zstd`, ...) | Menos rede e disco ao custo de CPU; quase sempre vale |

O trade-off que resume o produtor: **latência × throughput** — `linger.ms=0` manda já; `linger.ms=10` espera "um pouquinho pra mandar mais de uma vez".

### 2.2 Anatomia do Consumer API *(~3 min)*

O consumidor vive num ciclo — o **poll loop**:

```
subscribe(["pedidos"])
   │
   └─► loop:
         poll()          → busca um lote de mensagens das SUAS partições
         deserializer    → bytes → objeto (key e value)
         processa        → a lógica de negócio
         commit          → registra a posição (auto ou manual)
         (heartbeat corre em background: "estou vivo")
```

Ideias-chave:

- **O consumidor puxa (pull)** — o broker não empurra; o ritmo é de quem consome
- **`poll()` é também o sinal de vida do processamento** — demorar demais entre polls faz o grupo considerar o membro travado (e disparar rebalance)

**Configurações de referência — Consumer:**

| Config | Grupo | O que faz | Ponto didático |
|---|---|---|---|
| `bootstrap.servers` | Conexão | Endereços iniciais do cluster | Igual ao producer |
| `group.id` | Grupo | Nome do consumer group | O que separou fila de pub/sub na demo da Aula 2 |
| `key.deserializer` / `value.deserializer` | Formato | Bytes → objeto | Espelho do serializer; contrato entre as pontas |
| `auto.offset.reset` | Posição | Onde começar **sem offset commitado**: `earliest` \| `latest` | É o "from beginning" das nossas demos; só vale na primeira vez do grupo |
| `enable.auto.commit` | Commit | Confirma posição automaticamente (padrão: a cada 5 s) | A fronteira reprocessar × perder — seção 2.5 |
| `max.poll.records` | Fluxo | Máximo de mensagens por `poll()` | Lotes menores = polls mais frequentes = grupo mais estável |
| `session.timeout.ms` | Vida | Sem heartbeat por esse tempo → membro considerado morto | O gatilho do rebalance da demo (cena 3) |
| `max.poll.interval.ms` | Vida | Tempo máximo entre polls antes de ser expulso | Protege contra consumidor "vivo mas travado" |

### 2.3 Consumer group: a leitura que escala *(~3 min)*

Um **consumer group** é um time de consumidores dividindo o trabalho de ler um tópico. A regra de ouro:

> **Cada partição é lida por no máximo UM consumidor do grupo.**

Consequências diretas:

- **3 partições + 3 consumidores** → um pra cada: paralelismo máximo
- **3 partições + 2 consumidores** → um deles acumula duas partições
- **3 partições + 5 consumidores** → **2 ficam ociosos**: partições são o teto do paralelismo

**Analogia:** caixas do supermercado (Aula 3) com escala de funcionários. 5 operadores pra 3 caixas? Dois ficam de braço cruzado — abrir mais caixa (partição) é decisão de infraestrutura, não de escala do time.

### 2.4 Rebalance: o grupo se reorganiza *(~2 min)*

Quando um consumidor **entra** (deploy, scale-out) ou **sai** (crash, restart, ou expulso por `session.timeout` / `max.poll.interval`), o **group coordinator** redistribui as partições.

- É o que dá **elasticidade e tolerância a falha** à leitura: caiu um membro, os outros herdam suas partições
- Tem **custo**: pausa de consumo durante a redistribuição — consumidores instáveis (que estouram os timeouts da tabela acima) viram rebalance em loop, o clássico problema de produção

### 2.5 Commit de offset: o progresso do grupo *(~3 min)*

Ler não basta — o grupo precisa **registrar até onde leu** em cada partição, para retomar do lugar certo após restart ou rebalance.

- **Auto-commit** (`enable.auto.commit=true`) — confirma periodicamente, de forma automática. Simples, mas o intervalo entre "processei" e "confirmei" abre janelas de **reprocessamento** (ou, na ordem errada, de perda)
- **Commit manual** — o código confirma **depois** de processar. Se o consumidor morrer entre processar e confirmar, a mensagem será **relida** — o *at-least-once* que a Aula 5 formaliza
- O ponto conceitual: **o commit é a fronteira entre "reprocessar" e "perder"**

**Analogia:** o marcador de página da Aula 3, com disciplina: marca **antes** ou **depois** de ler? Antes: se cair no sono, pula páginas. Depois: pode reler meia página, mas nunca pula.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Sandbox: o mesmo Kafka single-broker KRaft, tópico de 3 partições. Tempo de tela: ~5–6 min.

**O que a demo deve mostrar, nesta ordem:**

1. **1 consumidor sozinho no grupo, produção contínua** — um producer publicando devagar (fluxo visível, ex.: 1 msg/s), um consumidor no grupo `pedidos-app` recebendo tudo, com partição de origem visível.
   - **Observação-alvo:** um consumidor só atende as 3 partições — funciona, sem paralelismo.

2. **Entra o 2º consumidor no MESMO grupo, ao vivo** — com a produção rodando.
   - **Observação-alvo:** o **rebalance acontece na tela** — breve pausa e as partições se redistribuem (ex.: um fica com 2, outro com 1). O fluxo se divide.

3. **Mata-se um consumidor (Ctrl+C / kill)** — ainda com produção rodando.
   - **Observação-alvo:** passado o `session.timeout` (ligar explicitamente à config da tabela), novo rebalance: o sobrevivente **herda** as partições do morto e nenhuma mensagem fica órfã.

4. **Inspecionar o grupo** (ferramenta de descrição de consumer groups): exibir por partição o **current offset**, o **log end offset** e o **lag**.
   - **Observação-alvo:** o progresso do grupo é um dado consultável — e o lag é a distância entre o fim do log e a posição confirmada. (Plantar: "no Curso 7, esse número vira o principal alerta de produção.")

**Fechamento na tela** *(~1 min)*: "duas APIs anatomizadas, grupo que escala, se recupera e registra progresso. Mas repare: quando o consumidor morreu, quem herdou pode ter relido alguma mensagem. Isso tem nome — at-least-once — e é o assunto da próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"O `send()` já enviou."** Não — enfileirou no accumulator; quem envia é a sender thread, em batch. Erros podem chegar depois, no callback. Essa assincronia explica o throughput e justifica `acks`/`retries`.
- **"Mais consumidores = sempre mais throughput."** O teto é o número de partições; excedentes ficam ociosos (Aula 3: partição = unidade de escala).
- **"Rebalance é de graça."** Há pausa de consumo — e os vilões usuais são os timeouts da tabela (processamento lento estourando `max.poll.interval.ms`). Tuning fino (assignment strategies, static membership) é conteúdo intermediate; não abrir essa porta.
- **"O commit marca a mensagem como lida para todo mundo."** O commit é **do grupo**; outros grupos têm offsets próprios, intocados (modelo de leitores independentes da Aula 2).
- **Disciplina de relógio:** as tabelas de configuração são **material de consulta** — em vídeo, narrar 4–5 por API (as da coluna "Ponto didático" mais fortes) e apontar que a documentação de apoio traz a referência completa.

**Ganchos com a trilha:**

- `acks`, `retries` e `enable.idempotence` → **Aula 5** (semânticas de entrega).
- Serializers/deserializers como contrato → **Curso 2, Aula 3** (Schema Registry).
- Consumer lag como métrica de saúde → **Curso 7, Aula 2** (observabilidade).
- Share groups (Aula 2) flexibilizam a regra "1 partição : 1 consumidor".

**Perguntas para checar entendimento:**

1. Seu produtor precisa de máximo throughput e tolera ~10 ms de latência extra. Quais duas configs você mexe primeiro?
2. Um consumidor processa cada lote em 8 minutos e o `max.poll.interval.ms` está em 5 minutos. O que acontece com o grupo — e que duas saídas você tem?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Producer API** | Cliente de escrita: serializa, particiona, acumula em batches e envia de forma assíncrona. |
| **Consumer API** | Cliente de leitura: opera em poll loop, puxando lotes das partições atribuídas ao seu grupo. |
| **Serializer / Deserializer** | Conversores objeto ↔ bytes nas duas pontas; o formato é contrato entre produtor e consumidor. |
| **Partitioner** | Componente do produtor que decide a partição (hash da chave ou distribuição sticky). |
| **Record accumulator** | Buffer do produtor que agrupa mensagens em batches por partição. |
| **Poll loop** | Ciclo do consumidor: `poll()` → processa → commit; também sinaliza que o membro está saudável. |
| **Consumer group** | Conjunto de consumidores que dividem as partições; cada partição vai a no máximo um membro. |
| **Rebalance** | Redistribuição de partições quando um membro entra, sai ou é considerado morto. |
| **Group coordinator** | Broker que gerencia a filiação e o rebalance do grupo. |
| **Heartbeat** | Sinal periódico de vida do consumidor ao coordinator (`session.timeout.ms` é o prazo). |
| **Offset commit** | Registro da posição de leitura confirmada pelo grupo, por partição. |
| **Consumer lag** | Distância entre o fim do log (log end offset) e a posição confirmada do grupo. |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 3 ("Kafka Producers") e cap. 4 ("Kafka Consumers") são exatamente as duas anatomias desta aula. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/

**Documentação e artigos oficiais:**

- Apache Kafka — *Producer Configs* (referência completa das configurações) — https://kafka.apache.org/documentation/#producerconfigs
- Apache Kafka — *Consumer Configs* — https://kafka.apache.org/documentation/#consumerconfigs
- Confluent — *Kafka Producer Design* (accumulator, batching, sender) — https://docs.confluent.io/platform/current/clients/producer.html
- Confluent — *Kafka Consumer Design* (poll loop, groups, rebalance) — https://docs.confluent.io/platform/current/clients/consumer.html
- Apache Kafka — *Consumer Rebalance Protocol* (KIP-848, o protocolo de nova geração) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
