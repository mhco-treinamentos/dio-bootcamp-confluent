# Curso 1 · Aula 3 — O Log do Kafka: Tópicos, Partições e Offsets

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~20 min (teto) · Conceito + demo (sandbox Docker — implementação no projeto de demos)

---

## 1. Objetivo da Aula

Abrir o capô do Kafka e dissecar sua estrutura fundamental: o **log append-only**, a anatomia de um **record**, como o **tópico** se divide em **partições**, como a partição vive no **disco** (segments) e o papel do **offset**. É a anatomia que explica tudo o que vem depois — paralelismo, ordem, replay e escala.

---

## 2. Conceitos Simplificados

### 2.1 O log: a estrutura mais simples que existe *(~3 min)*

O coração do Kafka é um **log append-only**: uma sequência em que só se faz uma coisa — **escrever no fim**. Nada de update, nada de delete no meio.

```
                                      escrita SEMPRE aqui
                                                │
┌──────┬──────┬──────┬──────┬──────┬──────┐    ▼
│ msg 0│ msg 1│ msg 2│ msg 3│ msg 4│ msg 5│  (próxima)
└──────┴──────┴──────┴──────┴──────┴──────┘
   ▲ imutável ─── a história não muda ───▲
```

- **Append-only** — toda mensagem nova entra no final da sequência
- **Imutável** — o que foi escrito não muda (o extrato bancário da Aula 1, materializado)
- **Sequencial** — a ordem de chegada é a ordem física no disco; por isso o Kafka é rápido: escrita e leitura sequenciais, não acesso aleatório

**Analogia:** o livro-caixa do contador. Lançamentos entram sempre na próxima linha, ninguém rasura linha antiga, e a numeração das linhas conta a história na ordem.

### 2.2 Anatomia de um record: o que vive em cada posição do log *(~3 min)*

Cada item do log é um **record** — e ele é mais do que o payload:

```
┌──────────────────────── RECORD ────────────────────────┐
│ offset      → posição na partição (atribuído pelo log) │
│ timestamp   → quando foi criado ou gravado             │
│ headers     → metadados livres (trace-id, origem...)   │
│ key (bytes) → identidade → decide a PARTIÇÃO           │
│ value(bytes)→ o payload: o evento/comando da Aula 1    │
└─────────────────────────────────────────────────────────┘
```

- É a **mensagem/envelope da Aula 1 ganhando endereço**: o log adiciona `offset` e carimba o `timestamp`
- **Key e value são só bytes para o broker** — ele não abre a carta; quem dá significado são serializer/deserializer nas pontas (anatomia completa na Aula 4)
- A **key** tem dupla função: identidade de negócio e roteamento de partição (o gatilho da ordem, Aula 5)

### 2.3 Tópico e partições: o log com nome, fatiado *(~4 min)*

Um **tópico** é o log nomeado por assunto (`pedidos`, `pagamentos`). Na prática, ele é dividido em **N partições — e cada partição é um log independente**:

```
topic: pedidos  (partitions=3)
├─ partition 0 │ off 0 │ off 1 │ off 2 │ off 3 │ →
├─ partition 1 │ off 0 │ off 1 │ →
└─ partition 2 │ off 0 │ off 1 │ off 2 │ →
      ▲ cada partição: sequência PRÓPRIA de offsets
```

Por que fatiar? Duas respostas:

1. **Paralelismo** — partições vivem em brokers diferentes e são lidas por consumidores diferentes ao mesmo tempo. Partição é a **unidade de escala** do Kafka
2. **Ordem tem escopo** — a ordem é garantida **dentro de cada partição**, não no tópico como um todo (como escolher a partição certa é a Aula 5 — chave)

**Analogia:** os caixas do supermercado. Um caixa só = fila ordenada, mas lenta. Dez caixas = dez filas em paralelo; a ordem existe **dentro de cada fila**, não entre elas.

### 2.4 A partição no disco: segments *(~3 min)*

Dissecando um nível abaixo: no broker, a partição **não é um arquivo gigante** — é uma cadeia de **segments**:

```
partition 0 (no disco do broker)
├─ 00000000000000000000.log   ← segment antigo (fechado)
├─ 00000000000000004096.log   ← segment fechado
└─ 00000000000000008192.log   ← ACTIVE segment (recebe as escritas)
   + arquivos .index           ← atalho offset → posição no arquivo
```

Por que isso importa (mesmo em beginner):

- **Retention age por segment** — expirar dados = **apagar arquivos inteiros** do início da cadeia, nunca editar o log. Barato e coerente com a imutabilidade
- **O nome do arquivo é o offset base** — achar o offset 5.000 é escolher o segment certo e usar o índice: leitura eficiente sem varrer tudo
- É a prova física de que "consumir não apaga": **quem remove dado é a política de retenção, nunca o consumidor**

### 2.5 Offset: o ponteiro de leitura *(~3 min)*

Cada record dentro de uma partição recebe um número sequencial: o **offset** (0, 1, 2, ...).

- **Identidade posicional** — a mensagem é "a de offset 42 da partição 1"; offsets se repetem entre partições, a identidade é o par (partição, offset)
- **O consumidor guarda a própria posição** — o Kafka não rastreia "quem já leu"; cada consumer group guarda até onde leu **por partição**
- **Replay é aritmética** — voltar ao offset 0 (ou qualquer ponto) é reposicionar o ponteiro; foi a Cena 3 da Aula 2

**Analogia:** o marcador de página. O livro (log) é o mesmo para todos; cada leitor tem **seu** marcador. Perder o marcador não destrói o livro — você só decide de onde recomeçar.

**Configurações de referência — Tópico:**

| Config | Grupo | O que faz | Ponto didático |
|---|---|---|---|
| `partitions` | Estrutura | Número de partições do tópico | Teto do paralelismo (Aula 4); aumentar depois redistribui chaves — decidir cedo |
| `replication.factor` | Durabilidade | Cópias de cada partição em brokers distintos | Só citar: o "3" do trio de confiabilidade da Aula 5 |
| `retention.ms` | Retenção | Tempo de vida dos dados no log | Default 7 dias; é o que torna o replay possível — e finito |
| `retention.bytes` | Retenção | Limite de tamanho por partição | Alternativa/complemento ao tempo |
| `cleanup.policy` | Retenção | `delete` (expira) ou `compact` (guarda o último valor por chave) | Compact = "foto atual por chave"; aprofunda no Curso 7 (custos) |
| `segment.bytes` / `segment.ms` | Disco | Quando fechar o segment ativo e abrir outro | Granularidade da expiração: retention apaga segments inteiros |
| `min.insync.replicas` | Durabilidade | Mínimo de réplicas em dia para aceitar escrita | Apresentado de verdade na Aula 5 — aqui só o nome |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Sandbox: o mesmo Kafka single-broker KRaft da Aula 2. Tempo de tela: ~5–6 min.

**O que a demo deve mostrar, nesta ordem:**

1. **Criar um tópico com 3 partições** — criação explícita (ex.: `pedidos-v2`, `--partitions 3`) e em seguida **descrever o tópico**, exibindo as 3 partições e configs herdadas (retention visível).
   - **Observação-alvo:** o tópico não é uma coisa só — são 3 logs independentes, com configuração inspecionável.

2. **Produzir ~9 mensagens SEM chave** e consumir exibindo **partição + offset + timestamp** de cada uma.
   - **Observação-alvo:** as mensagens se **espalham** entre as partições e **cada partição tem sua própria sequência de offsets a partir do 0** — a identidade é (partição, offset). Ligar ao diagrama da seção 2.3.

3. **Espiar o disco do broker** — listar o diretório de dados da partição 0 (ex.: `ls /var/lib/kafka/data/pedidos-v2-0/`), mostrando os arquivos `.log` e `.index`.
   - **Observação-alvo:** a partição é **arquivo de verdade no disco** — o segment ativo existe, tem nome numérico (offset base) e cresce com a produção. A teoria da seção 2.4, tocável.

4. **Reler a mesma partição duas vezes** — consumir a partição 0 desde o início, encerrar, consumir de novo desde o início.
   - **Observação-alvo:** mesmas mensagens, mesma ordem, mesmos offsets. O log não mudou — quem muda de posição é o leitor.

5. **Consultar os offsets finais do tópico** (ferramenta de listagem de offsets), o "tamanho" de cada partição.
   - **Observação-alvo:** o log é inspecionável como estrutura — dá pra saber quanto cada partição tem sem ler nenhuma mensagem.

**Fechamento na tela** *(~1 min)*: "3 logs, records com anatomia, segments no disco, ordem por partição e leitor dono da posição. Na próxima aula, dissecamos quem escreve e quem lê: as anatomias do Producer e do Consumer API."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"O tópico tem ordem global."** Não — ordem é **por partição**. O equívoco mais caro do Kafka em produção; a Cena 2 da demo o desmonta visualmente. A solução (chave) é da Aula 5 — plante a curiosidade, não a resposta inteira.
- **"O offset é do tópico."** O offset é **por partição**, e a posição de leitura é por consumer group, por partição. "Consumidor X está no offset 42" só faz sentido dizendo de qual partição.
- **"O Kafka sabe quem já leu."** O broker não marca mensagem como lida — o consumidor guarda a posição. É isso que torna N leitores independentes baratos.
- **"Consumir remove; retention é detalhe."** Inverter: **só** a retenção remove — e remove segments inteiros. A cena 3 da demo (disco) torna isso concreto.
- **"Mais partições é sempre melhor."** Unidade de escala, mas com custo (arquivos, líderes, rebalance). No Curso 7 vira decisão dimensionada; aqui basta "partições definem o teto de paralelismo".
- **Disciplina de relógio:** a tabela de configs é material de consulta — em vídeo, narrar `partitions`, `retention.ms` e `cleanup.policy`; o resto fica na documentação de apoio.

**Ganchos com a trilha:**

- Teto de paralelismo por partições → **Aula 4** (consumer groups).
- Key do record → partição → ordem → **Aula 5**.
- Serializer/deserializer dando significado aos bytes → **Aula 4** e **Curso 2, Aula 3** (Schema Registry).
- Retention, compaction e Tiered Storage como alavanca de custo → **Curso 7, Aula 4**.

**Perguntas para checar entendimento:**

1. Um tópico com 3 partições recebeu 30 mensagens. É possível dizer qual foi a 15ª mensagem "do tópico"? Por quê?
2. O consumidor leu tudo até o fim. Os dados sumiram do disco? Quem decide quando eles somem — e em que granularidade?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Log (append-only log)** | Estrutura sequencial e imutável onde records são sempre adicionados ao final. |
| **Record** | Item do log: offset + timestamp + headers + key + value. |
| **Topic (tópico)** | Log nomeado que agrupa records de um mesmo assunto. |
| **Partition (partição)** | Fatia independente de um tópico; unidade de paralelismo e escopo da ordem. |
| **Offset** | Número sequencial de um record dentro de uma partição. |
| **Segment** | Arquivo físico que compõe a partição no disco; o ativo recebe escritas, os antigos expiram inteiros. |
| **Retention (retenção)** | Política de tempo/tamanho que define por quanto tempo o log guarda os dados. |
| **Compaction (`cleanup.policy=compact`)** | Política que preserva o último record de cada chave em vez de expirar por tempo. |
| **Round-robin / sticky** | Estratégias de distribuição de records sem chave entre as partições. |
| **Broker** | Servidor Kafka que armazena partições e atende produtores e consumidores. |
| **Committed position** | Posição de leitura que um consumer group guarda por partição (detalhada na Aula 4). |

---

## 6. Referências

**Livros:**

- Jay Kreps — *I ♥ Logs: Event Data, Stream Processing, and Data Integration* (O'Reilly) — o livro-manifesto do log como abstração central
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 6 ("Kafka Internals") cobre armazenamento físico, segments e índices. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11 ("Stream Processing"): logs particionados como base de sistemas de streaming

**Documentação e artigos oficiais:**

- Jay Kreps — *The Log: What every software engineer should know about real-time data's unifying abstraction* (LinkedIn Engineering) — https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- Apache Kafka — *Design: Persistence* e *Log Compaction* — https://kafka.apache.org/documentation/#persistence e https://kafka.apache.org/documentation/#compaction
- Apache Kafka — *Topic-Level Configs* (retention, cleanup.policy, segment.*) — https://kafka.apache.org/documentation/#topicconfigs
- Confluent — *Kafka Storage Internals* (Topics/Partitions/Segments) — https://docs.confluent.io/kafka/design/log_compaction.html
