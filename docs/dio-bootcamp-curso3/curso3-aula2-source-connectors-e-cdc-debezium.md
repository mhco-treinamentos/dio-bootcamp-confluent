# Curso 3 · Aula 2 — Source Connectors e CDC: capturando dados de bancos legados com Debezium

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo (Neon Postgres → Confluent Cloud)
> **Pré-requisitos:** Aula 1 do Curso 3 + Setup Neon concluído (banco com replicação lógica e dataset carregado)

---

## 1. Objetivo da Aula

Dissecar o **CDC (Change Data Capture)** — a técnica que transforma o banco legado em fonte de eventos **sem tocar na aplicação** — entendendo a diferença entre captura **log-based e query-based**, a anatomia do **envelope Debezium** (`before`/`after`/`op`), o ciclo **snapshot → streaming**, e capturar mudanças reais do Postgres do bootcamp em tempo real.

---

## 2. Conceitos Simplificados

### 2.1 O problema: o dado nasce no banco, o streaming precisa dele *(~3 min)*

O Curso 1 (Aula 6) prometeu a migração incremental: o legado continua operando **e** os eventos fluem. Mas o sistema legado não publica eventos — ele faz `INSERT`, `UPDATE`, `DELETE` num banco. As opções ruins:

- **Mudar a aplicação legada** para publicar eventos — caro, arriscado, às vezes impossível (sistema de terceiro)
- **Dual write** (a aplicação grava no banco E publica no Kafka) — o anti-padrão clássico: sem transação entre os dois, uma hora eles **divergem**
- **Polling** (consultar o banco de tempos em tempos) — perde deletes, perde estados intermediários, pesa no banco

A saída elegante: **o banco já registra toda mudança num log de transações** (WAL no Postgres, binlog no MySQL) — é como ele garante durabilidade. O CDC log-based **lê esse log** e converte cada mudança em evento. A aplicação legada nem fica sabendo.

### 2.2 Anatomia do CDC log-based: três camadas trabalhando juntas *(~4 min)*

Antes do diagrama, a separação de papéis que evita toda a confusão de nomenclatura:

| Camada | Quem é | O que faz |
|---|---|---|
| **Framework** | Kafka Connect (Aula 1) | Executa o plugin: tasks, entrega, offsets, converters, escala |
| **Plugin / motor** | **Debezium** | O `connector.class`: sabe **ler e interpretar** o log de cada banco e montar o evento |
| **Tecnologia do banco** | Replicação lógica / WAL (Postgres), binlog (MySQL), redo log (Oracle) | O mecanismo **nativo** que o banco já usa para durabilidade e replicação |

Ou seja: **Debezium não é uma ferramenta que você opera** — é o plugin que roda dentro do Connect, conversando com o recurso de CDC que o próprio banco oferece. No Confluent Cloud, o produto é o conector fully managed **"PostgreSQL CDC Source V2 (Debezium)"**: você preenche a configuração; o framework, o motor e a conexão com o WAL são problema da plataforma.

```
 APLICAÇÃO LEGADA (intocada)
      │ INSERT / UPDATE / DELETE
      ▼
 POSTGRES ──► WAL (Write-Ahead Log: toda mudança, em ordem, durável)
                │
                │  replicação lógica ◄── a TECNOLOGIA DO BANCO
                │                        (o toggle do Setup Neon)
                ▼
        ┌─ KAFKA CONNECT (framework) ─────────────┐
        │   DEBEZIUM (plugin/motor)                │
        │   lê o log, interpreta, monta o evento,  │
        │   marca a posição                        │
        └───────────────────────────────────────────┘
                ▼
        TOPIC por tabela  (key = primary key da linha!)
```

- **Log-based × query-based** — ler o WAL captura **tudo** (inclusive DELETEs e cada estado intermediário), em ordem, com custo mínimo no banco; polling por query é a alternativa fraca (perde eventos, pesa, atrasa)
- **A epifania da aula:** o WAL **é um log append-only de fatos imutáveis** — o banco relacional sempre teve um "Kafka interno". O CDC só o publica. (Curso 1, Aula 3, fechando o ciclo)
- **key = primary key** → mesma linha, mesma partição, **ordem por linha garantida** — o Curso 1, Aula 5, decidindo a arquitetura de novo
- Debezium marca a **posição no log** (offset de replicação): conector cai e volta **de onde parou** — o marcador de página outra vez

### 2.3 Anatomia do envelope Debezium *(~4 min)*

Cada mudança vira um evento com estrutura rica — o **envelope**:

```json
{
  "before": { "id": 42, "status": "PENDENTE",  "total": 289.90 },
  "after":  { "id": 42, "status": "PAGO",      "total": 289.90 },
  "op": "u",
  "ts_ms": 1767100327000,
  "source": { "table": "pedidos", "lsn": 39017528, "db": "bootcamp", ... }
}
```

| Campo | O que carrega | Leitura |
|---|---|---|
| `op` | O tipo da mudança | `c`=create, `u`=update, `d`=delete, `r`=read (snapshot) |
| `before` | O estado ANTES | `null` em creates; precioso em updates/deletes |
| `after` | O estado DEPOIS | `null` em deletes |
| `source` | A proveniência | tabela, banco, posição no log (LSN) |
| `ts_ms` | Quando aconteceu | a linha do tempo do dado |

- **`before` + `after` = mais que o estado atual: a transição** — dá para saber *o que mudou*, não só *como ficou* (auditoria e lógica de negócio agradecem)
- **DELETE vira evento** (`op: "d"`) — a informação que o polling perde para sempre
- O envelope é verboso por design; "achatar" para consumir só o `after` é trabalho de SMT (**Aula 4**) ou do Flink (**Curso 4**)

### 2.4 Snapshot → streaming: os dois tempos do CDC *(~3 min)*

O conector novo encontra um banco que **já tem anos de dados**. O ciclo:

1. **Snapshot inicial** — o Debezium lê as tabelas inteiras e emite cada linha existente como evento `op: "r"` — o passado entra no tópico
2. **Streaming contínuo** — terminado o snapshot, o conector gruda no WAL e segue em tempo real — o presente flui

Consequência para quem consome: o tópico CDC contém **a história completa** (estado inicial + toda mudança desde então) — um consumidor novo pode reconstruir a tabela inteira fazendo replay (retention/compaction do Curso 1 decidindo quanto de história fica).

**Configurações de referência — Postgres CDC Source (Debezium):**

| Config | O que faz | Ponto didático |
|---|---|---|
| `database.hostname/.port/.dbname` | Onde está o banco | Os dados do Setup Neon |
| `table.include.list` | Quais tabelas capturar | Começar pequeno; cada tabela → um tópico |
| `snapshot.mode` | Se/como fazer o snapshot inicial | `initial` (padrão didático): passado + presente |
| `output.data.format` | Formato no tópico (AVRO...) | Contrato desde a ingestão (Curso 2, Aula 3) |
| `tombstones.on.delete` | Emitir tombstone após deletes | Par com compaction — citar, aprofundar é intermediate |
| `tasks.max` | Paralelismo | CDC de um banco = tipicamente 1 task (o log é um só) |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Fonte: Neon Postgres do Setup (dataset do bootcamp — *placeholder até a modelagem fechar*). Tempo de tela: ~7 min. **Conector deletado ao final** (teardown).

**O que a demo deve mostrar, nesta ordem:**

1. **Criar o Postgres CDC Source (Debezium) fully managed** — apontando pro Neon (host/credenciais do Setup), capturando a tabela `pedidos` (ilustrativa), formato Avro, `snapshot.mode=initial`.
   - **Observação-alvo:** o formulário materializa a tabela de configs da seção 2.4 — e a role dedicada do Setup autenticando (escopo mínimo, sempre).

2. **Assistir ao snapshot** — o tópico da tabela recebendo os dados **que já existiam** no banco; abrir um evento e ver `op: "r"`.
   - **Observação-alvo:** o passado entrou no streaming sem ninguém produzir nada — e o schema do envelope registrado no Registry automaticamente.

3. **INSERT ao vivo** — no SQL Editor do Neon, inserir um pedido; segundos depois, o evento `op: "c"` no tópico (com `before: null`).
   - **Observação-alvo:** a latência banco → evento é de segundos — CDC é tempo real, não batch disfarçado.

4. **UPDATE ao vivo** — atualizar o status do pedido; ver o evento `op: "u"` com **`before` e `after` preenchidos**, lado a lado.
   - **Observação-alvo:** a transição capturada — o momento-síntese do envelope (seção 2.3).

5. **DELETE ao vivo** — apagar uma linha; ver `op: "d"` (com `after: null`).
   - **Observação-alvo:** o evento que o polling jamais capturaria — o argumento definitivo do log-based.

6. **Conferir a chave** — exibir que a key dos eventos é a primary key da linha, e que todos os eventos do pedido 42 estão na mesma partição, em ordem.
   - **Observação-alvo:** Curso 1, Aula 5, funcionando dentro do CDC sem ninguém pedir.

7. **Teardown** — deletar o conector (tópico e schema ficam para a Aula 3).

**Fechamento na tela** *(~1 min)*: "o banco legado virou fonte de eventos sem uma linha de código na aplicação. Metade do caminho — na próxima aula, fazemos a volta: do Kafka para os sistemas de destino, com os Sink Connectors."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Vou precisar instalar/operar o Debezium."** Não — Debezium é o **plugin** que o Kafka Connect executa; no fully managed, você configura o conector e nunca toca no motor. (No self-managed é diferente: lá o plugin se instala no seu cluster de workers — a fronteira de gestão da Aula 1.)
- **"Existe um conector Postgres 'nativo' melhor que o Debezium no Confluent Cloud."** O CDC Source V2 (Debezium) **é** o conector nativo fully managed para Postgres log-based; a alternativa de catálogo é o Postgres Source (JDBC) — que é query-based (polling por timestamp/coluna incremental): não vê DELETEs, perde estados intermediários. É exatamente a versão fraca da seção 2.1.
- **"CDC é um SELECT agendado."** Polling é a versão fraca: perde deletes e estados intermediários, atrasa e pesa. Log-based lê o que o banco **já escreve de graça**. A dupla cena 4/5 da demo é o argumento.
- **"Dual write resolve e é mais simples."** Sem transação distribuída entre banco e Kafka, os dois divergem — e a divergência é silenciosa. CDC elimina a classe do problema: **uma escrita só** (no banco), o evento deriva dela.
- **"O envelope Debezium é o formato final para consumo."** É o formato de **captura** — rico de propósito. Consumidores de negócio geralmente querem o `after` achatado: SMT (Aula 4) ou Flink (Curso 4).
- **"Snapshot é opcional/detalhe."** É a decisão sobre **o passado**: sem ele, o tópico só conta a história a partir de agora. `snapshot.mode` é escolha de arquitetura.
- **Cuidado narrativo:** replication slot parado acumula WAL no Postgres (custo/risco no banco de origem) — citar como "por que não deixar conector CDC morto sem deletar o slot"; aprofundar é intermediate.

**Ganchos com a trilha:**

- Achatar o envelope e rotear → **Aula 4** (SMT).
- Entregar o que o CDC capturou em outros sistemas → **Aula 3** (na sequência).
- Enriquecer/transformar o fluxo CDC → **Curso 4** (Flink) — e o shift-left do **Curso 5**.
- Este conector é a **camada de ingestão do Desafio Final** — o aluno replica este desenho no domínio dele.
- A migração do monólito (Curso 1, Aula 6) + Cluster Linking (Curso 2, Aula 5) + CDC = o kit completo de convivência com o legado.

**Perguntas para checar entendimento:**

1. Por que o dual write é considerado anti-padrão, e como o CDC elimina o problema em vez de mitigá-lo?
2. Um analista pergunta: "consigo saber qual era o status do pedido ANTES da última atualização?" — que campo do envelope responde, e o que se perderia com captura por polling?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **CDC (Change Data Capture)** | Técnica de capturar mudanças de um banco como fluxo de eventos. |
| **Log-based CDC** | Captura lendo o log de transações do banco — completa, ordenada, barata. |
| **Query-based CDC (polling)** | Captura por consultas periódicas — perde deletes e estados intermediários. |
| **WAL (Write-Ahead Log)** | Log de transações do Postgres; toda mudança passa por ele antes do dado. |
| **Replicação lógica / replication slot** | Mecanismo do Postgres que expõe o WAL a consumidores externos e marca a posição de leitura. |
| **Debezium** | Plugin/motor de CDC open source que roda sobre o Kafka Connect, interpretando o log nativo de cada banco — embarcado nos conectores CDC fully managed do Confluent Cloud. |
| **PostgreSQL CDC Source V2 (Debezium)** | O conector fully managed log-based para Postgres no Confluent Cloud (a V1/legacy atingiu end of life em 2026). |
| **PostgreSQL Source (JDBC)** | A alternativa query-based do catálogo: polling por timestamp/coluna incremental — não captura DELETEs. |
| **Envelope Debezium** | Estrutura do evento CDC: `before`, `after`, `op`, `source`, `ts_ms`. |
| **op (c/u/d/r)** | Tipo da mudança: create, update, delete, read (snapshot). |
| **Snapshot inicial** | Leitura das tabelas existentes na primeira execução — o passado entrando no tópico. |
| **Tombstone** | Mensagem de valor nulo que sinaliza remoção (par da compaction — Curso 1). |
| **Dual write** | Anti-padrão: aplicação grava no banco e publica no Kafka sem atomicidade entre os dois. |

---

## 6. Referências

**Livros:**

- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11: CDC e a dualidade banco ↔ log (a fundamentação teórica da "epifania" da aula)
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 9 (pipelines e CDC). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/

**Documentação oficial:**

- Debezium — *Documentation (arquitetura e o envelope de eventos)* — https://debezium.io/documentation/reference/stable/
- Debezium — *PostgreSQL Connector* — https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Confluent Cloud — *Postgres CDC Source V2 (Debezium) Connector* — https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/
- Neon — *Logical Replication com Kafka/Confluent* — https://neon.tech/docs/guides/logical-replication-kafka-confluent
- Martin Kleppmann — *Turning the database inside-out* (talk clássica que fundamenta o CDC como filosofia) — https://martin.kleppmann.com/2015/11/05/database-inside-out-at-oredev.html
