# Curso 4 · Aula 1 — Introdução ao Apache Flink no Confluent Cloud e ao Processamento de Streams

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Cursos 1–3 (tópicos populados pelo pipeline do Curso 3)

---

## 1. Objetivo da Aula

Abrir o Bloco 3 apresentando o **processamento de streams com estado** — o que a SMT do Curso 3 não faz — e o **Apache Flink no Confluent Cloud**: a anatomia serverless (**compute pool → statement → CFU**), o posicionamento honesto frente a **Kafka Streams e ksqlDB**, e o primeiro statement da vida do aluno: a query que nunca termina.

---

## 2. Conceitos Simplificados

### 2.1 O degrau que faltava: processamento com estado *(~3 min)*

A régua do Curso 3 dizia: *Connect move, Flink transforma*. Agora definimos o que "transformar de verdade" exige — **estado**:

```
 STATELESS (SMT, Curso 3)          STATEFUL (Flink, este curso)
 evento entra → evento sai          evento entra → LEMBRA do que veio antes → resultado
 mascarar, achatar, rotear          "total por cliente na última hora"
 1-para-1, sem memória              "pedido SEM pagamento em 10 min" (fraude!)
                                    "junte pedido com o cadastro do cliente"
```

- Toda pergunta interessante sobre um fluxo envolve **memória**: agregar, juntar, detectar padrão, comparar com o passado
- Manter memória sobre um fluxo infinito, com escala e tolerância a falha, é **o** problema difícil — e é exatamente o que um stream processor resolve
- O **Apache Flink** é o motor open source que venceu essa categoria: processamento com estado, event time, exactly-once — o padrão de mercado que o Confluent Cloud oferece serverless

### 2.2 Anatomia do Apache Flink: o motor por dentro *(~4 min)*

Antes do serverless, o motor — os componentes que existem em **todo** cluster Flink do mundo:

```
 SEU SQL/CÓDIGO
      │ vira um JOB GRAPH (o plano: operadores encadeados —
      │  ler → filtrar → agregar → escrever)
      ▼
 ┌─ JOBMANAGER ────────────────────────────────────────┐
 │  o cérebro: distribui o trabalho, coordena os        │
 │  CHECKPOINTS (Aula 4), reage a falhas                │
 └──────────────┬───────────────────────────────────────┘
                ▼
 ┌─ TASKMANAGERS (os músculos — N processos) ──────────┐
 │  ├─ TASK SLOTS: as vagas de paralelismo              │
 │  │    (cada fatia do job roda num slot)              │
 │  └─ STATE BACKEND (RocksDB): onde o ESTADO vive      │
 │       localmente durante o processamento             │
 └──────────────┬───────────────────────────────────────┘
                ▼
   CHECKPOINT STORAGE durável (ex.: object storage)
   as fotos periódicas do estado (Aula 4)
```

- **Job graph** — a query vira um plano de operadores paralelos; o paralelismo de cada operador ocupa slots (o eco do "partições = teto de paralelismo" do Curso 1 — aqui, slots são as vagas)
- **JobManager** — coordena: agenda tasks, dispara checkpoints, orquestra a recuperação. Cai o JobManager? Alta disponibilidade dele também é problema de quem opera
- **TaskManagers + state backend** — onde o trabalho e o **estado** de fato vivem (RocksDB local para estado maior que memória) — o tesouro da Aula 4
- A moral: um cluster Flink self-managed é um **sistema distribuído completo para operar** — dimensionar TaskManagers, HA do JobManager, storage de checkpoint, upgrades. Poderoso, e caro de carregar

### 2.3 O que o Confluent Cloud faz com isso *(~3 min)*

O Flink serverless do CC pega cada peça da seção 2.2 e move a fronteira:

| Componente do motor | No self-managed | No Confluent Cloud |
|---|---|---|
| JobManager (+ HA) | Você provisiona e opera | **Invisível** — plataforma |
| TaskManagers / slots | Você dimensiona e escala | **CFUs elásticos** no compute pool |
| State backend (RocksDB) | Você configura e monitora | **Gerenciado** |
| Checkpoint storage | Você provê (S3 etc.) e ajusta | **Automático** (Aula 4) |
| Deploy do job | Empacotar, submeter, versionar | **Statement SQL** — submeteu, rodou |
| Upgrades do motor | Janela de manutenção sua | Plataforma |

O que sobra para você — a interface serverless:

```
 ENVIRONMENT
   └── COMPUTE POOL  (o "orçamento": elástico, em CFUs, de 0 ao teto)
          ├── STATEMENT 1  (um SQL contínuo — a unidade de trabalho)
          └── STATEMENT 2 ...
                │ lê/escreve DIRETO nos topics · usa o MESMO Schema Registry
                ▼
              KAFKA (os tópicos dos Cursos 1–3)
```

- **Compute pool** — o envelope de capacidade em **CFUs**, escalando sozinho de 0 ao teto. **Pool ocioso em 0 CFU não custa; statement rodando, custa** — o vilão nº 2 do teardown (Aula 0)
- **Statement** — o job graph inteiro condensado num comando SQL: `SELECT` exploratório morre com a sessão; `INSERT INTO ... SELECT` roda **para sempre** até ser parado
- Por que a anatomia do motor ainda importa, então? Porque **os conceitos operam igual** (paralelismo, estado, checkpoint — a Aula 4 é sobre eles) e porque o mercado tem muito Flink self-managed: o aluno que entende as duas colunas conversa com os dois mundos

### 2.4 Antes do Flink: Kafka Streams e ksqlDB *(~3 min)* 📊 *slide 1 da dupla*

O ecossistema Kafka criou duas gerações de resposta ao stream processing antes do Flink dominar:

**Kafka Streams** — uma **biblioteca Java/Scala**, não um servidor:

- O processamento roda **dentro da sua aplicação** — o microsserviço é o processador; escala subindo instâncias (é um consumer group turbinado)
- Estado local (RocksDB) com changelog no próprio Kafka; entrega exactly-once
- A força: processamento **embutido no serviço** — quando a transformação é parte da lógica do microsserviço, não um pipeline à parte
- O custo: é código Java para escrever, testar e operar em cada aplicação

**ksqlDB** — o SQL sobre o Kafka Streams:

- Um **servidor** que executa SQL contínuo — a tentativa de democratizar o Streams sem Java
- Historicamente importante (popularizou "SQL em streams" no ecossistema), mas com limites conhecidos: joins complexos e estado alto degradam, e o motor evoluiu menos que o Flink
- **Leitura honesta do mercado:** no Confluent Cloud, o **Flink assumiu o posto** de motor SQL recomendado — o ksqlDB permanece em uso em bases existentes, mas projeto novo nasce em Flink

### 2.5 A tabela de decisão: Streams × ksqlDB × Flink *(~3 min)* 📊 *slide 2 da dupla*

| Dimensão | **Kafka Streams** | **ksqlDB** | **Flink (Confluent Cloud)** |
|---|---|---|---|
| O que é | Biblioteca Java na sua app | Servidor SQL sobre Streams | Motor de processamento serverless |
| Linguagem | Java/Scala | SQL (dialeto próprio) | **Flink SQL** (ANSI-like) + Table API |
| Onde roda | Nas suas instâncias | Cluster ksqlDB (self ou managed) | Compute pool — **zero infra sua** |
| Estado | Local (RocksDB) + changelog | Idem (herda do Streams) | Gerenciado pela plataforma |
| Joins/janelas complexos | Bom, com código | Limites conhecidos em estado alto | **O ponto forte** |
| Event time / watermarks | Sim, via código | Parcial | **Nativo e central** (Aula 3) |
| Escala | Instâncias da app (teto: partições) | Nós do cluster | CFUs elásticos |
| Quando escolher | Transformação **embutida** num microsserviço Java | Base legada já em ksqlDB | **O caminho padrão** para pipelines de transformação no CC |

A régua de bolso para narrar: **a lógica pertence a um microsserviço específico?** Kafka Streams. **É um pipeline de dados (transformar, enriquecer, agregar para vários consumidores)?** Flink. **ksqlDB?** Mantém o que existe; não começa o que não existe.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — UI do Flink no Confluent Cloud (workspace SQL) + CLI. Fonte: tópicos do pipeline do Curso 3 (dataset placeholder). Tempo de tela: ~6 min. **Statement parado ao final.**

**O que a demo deve mostrar, nesta ordem:**

1. **Criar o compute pool** — no environment do bootcamp, pool pequeno (teto baixo de CFUs), região igual à do cluster.
   - **Observação-alvo:** o pool nasce em 0 CFU — capacidade elástica, não servidor ligado. O medidor de CFU na tela.

2. **Abrir o workspace SQL e rodar o primeiro `SELECT`** — `SELECT * FROM pedidos` (a tabela que "já existe" — spoiler da Aula 2), com o pipeline do Curso 3 alimentando.
   - **Observação-alvo:** **a query não termina** — resultados continuam chegando enquanto eventos chegam. O momento-síntese da aula: SQL sobre um fluxo infinito. (Dramatizar: "num banco, isso acabaria; aqui, isso é um processo vivo.")

3. **Um filtro contínuo** — `SELECT ... WHERE total > 200`, novos eventos caros pingando na tela ao vivo.
   - **Observação-alvo:** a semântica é a de sempre (SQL), o comportamento é novo (contínuo) — a curva de aprendizado é menor do que parece.

4. **Ver o statement como recurso** — a lista de statements: status, CFUs consumidos, o SELECT exploratório morrendo quando a sessão fecha.
   - **Observação-alvo:** statement é recurso vivo com custo — a diferença entre o SELECT de sessão e o `INSERT INTO` perpétuo (que vem na Aula 2) já anunciada.

5. **Teardown** — encerrar statements; pool fica (0 CFU ocioso ≈ ok), regra na tela: **statement rodando = crédito queimando**.

**Fechamento na tela** *(~1 min)*: "SQL que não termina, rodando sem nenhum cluster nosso. Na próxima aula, a mágica por trás do `FROM pedidos`: por que todo tópico é uma tabela — e toda tabela, um stream."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Flink é um banco de dados / o SELECT termina."** É um **processador contínuo** — a query é um processo, não uma consulta. A cena 2 da demo existe para chocar de propósito.
- **"Streams, ksqlDB e Flink são intercambiáveis."** Modelo, operação e estado diferem — a tabela da seção 2.5 é a resposta. Nomear o critério (lógica de serviço × pipeline de dados), não a moda.
- **"ksqlDB morreu."** Enquadramento profissional: continua suportado e presente em bases reais — a recomendação para projeto novo mudou, e saber operar o legado também é mercado.
- **"Compute pool é um cluster que estou pagando."** Pool é envelope elástico — em 0 CFU não custa; quem custa é statement rodando. A distinção evita tanto o susto quanto o desperdício.
- **Disciplina de escopo:** Table API/DataStream API (Flink com código) existem — citar que o bootcamp foca **Flink SQL** (o caminho do CC e o de maior alavancagem para dados) e seguir.

**Ganchos com a trilha:**

- A dualidade tópico ↔ tabela → **Aula 2** (na sequência).
- Janelas, joins e watermarks → **Aula 3**.
- Estado, checkpoints e produção → **Aula 4**.
- O Flink é a **camada de transformação do Desafio Final** — e o "shift-left" do **Curso 5** o coloca antes do Lakehouse.

**Perguntas para checar entendimento:**

1. Sua equipe precisa (a) validar e enriquecer o fluxo de pedidos para 5 áreas consumidoras e (b) manter um contador de tentativas de login dentro do microsserviço de auth (Java). Qual tecnologia para cada caso, e por quê?
2. Por que "a query nunca termina" não é um defeito — e o que isso implica sobre o custo de deixá-la rodando?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Stream processing** | Processamento contínuo de fluxos de eventos, com resultados atualizados à medida que dados chegam. |
| **Estado (state)** | A memória que o processador mantém entre eventos (agregados parciais, janelas, joins). |
| **Apache Flink** | Motor open source de stream processing com estado, event time e exactly-once. |
| **Job graph** | O plano de execução: a query decomposta em operadores paralelos encadeados. |
| **JobManager** | O coordenador do cluster Flink: agenda tasks, dispara checkpoints, orquestra recuperação. |
| **TaskManager** | O processo trabalhador: executa as tasks nos seus task slots. |
| **Task slot** | A vaga de paralelismo dentro de um TaskManager. |
| **State backend (RocksDB)** | Onde o estado vive localmente durante o processamento (self-managed: você configura; CC: gerenciado). |
| **Compute pool** | Envelope elástico de capacidade Flink no CC, medido em CFUs (escala de 0 ao teto). |
| **CFU (Confluent Flink Unit)** | Unidade de capacidade/cobrança do Flink no Confluent Cloud. |
| **Statement** | Um comando SQL contínuo em execução — a unidade de trabalho (e de custo) do Flink no CC. |
| **Kafka Streams** | Biblioteca Java de stream processing que roda dentro da aplicação. |
| **ksqlDB** | Servidor de SQL contínuo construído sobre o Kafka Streams. |
| **Flink SQL** | O dialeto SQL do Flink — a interface do bootcamp para processamento de streams. |

---

## 6. Referências

**Livros:**

- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — processamento como cidadão do log. E-book gratuito: https://www.confluent.io/designing-event-driven-systems/
- Tyler Akidau, Slava Chernyak & Reuven Lax — *Streaming Systems* (O'Reilly) — a bíblia conceitual de stream processing (estado, tempo, janelas)

**Documentação e artigos oficiais:**

- Confluent Cloud — *Apache Flink Overview (compute pools, statements, CFUs)* — https://docs.confluent.io/cloud/current/flink/index.html
- Apache Flink — *What is Apache Flink?* — https://flink.apache.org/what-is-flink/flink-architecture/
- Apache Flink — *Flink Architecture (JobManager, TaskManagers, slots)* — https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/
- Apache Kafka — *Kafka Streams* — https://kafka.apache.org/documentation/streams/
- ksqlDB — *Documentation* — https://docs.ksqldb.io/
- Confluent — *Comparing Stream Processing: Flink, Kafka Streams e ksqlDB (developer)* — https://developer.confluent.io/learn/kafka-streams-vs-flink/
