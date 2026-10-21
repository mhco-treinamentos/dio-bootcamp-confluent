# Curso 5 · Aula 2 — Materializando Kafka Topics como Tabelas Apache Iceberg com Tableflow

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 1 do Curso 5 + `pedidos_enriquecidos` populado (Curso 4)

---

## 1. Objetivo da Aula

Apertar o botão e dissecar o que acontece: a anatomia da **materialização** (tópico → Parquet → metadados Iceberg → catálogo), a decisão de **storage** — **Confluent Managed Storage (CMS)** vs **Bring Your Own Storage (BYOS)** — e a manutenção automática que o Tableflow assume (compaction, snapshots, retenção). Ao final, o aluno tem sua primeira tabela Iceberg viva, alimentada pelo streaming.

---

## 2. Conceitos Simplificados

### 2.1 Anatomia da materialização *(~5 min)*

O que o "Enable Tableflow" dispara por baixo:

```
 TOPIC (pedidos_enriquecidos)
    │  ① lê os eventos continuamente (é um consumidor — offsets, lag: Curso 1 valendo)
    ▼
 ② SCHEMATIZAÇÃO — consulta o SCHEMA REGISTRY (a fonte da verdade — Aula 3)
    │   Avro/Protobuf/JSON Schema → o schema da TABELA
    ▼
 ③ CONVERSÃO — eventos → arquivos PARQUET (colunar, comprimido)
    ▼
 ④ METADADOS ICEBERG — snapshots + manifests apontando os Parquet
    ▼
 ⑤ CATÁLOGO — a tabela publicada no Iceberg REST Catalog do Tableflow
    │   (é aqui que os engines a descobrem — Aula 4)
    ▼
 TABELA ICEBERG — read-only, sempre fresca, sem pipeline escrito
```

- **É uma representação contínua, não uma cópia batch** — o Tableflow acompanha o tópico; a tabela reflete o fluxo com atraso de materialização pequeno
- **Read-only por design** — a verdade é o tópico (e o Schema Registry); a tabela é derivada. Ninguém "edita" a tabela — muda-se o dado na origem (a imutabilidade do Curso 1 mantendo a linha)
- **Sem schema, sem tabela** — o Tableflow **não suporta tópicos schemaless**: o contrato do Curso 2 deixou de ser boa prática e virou **pré-requisito** (o argumento definitivo para o AvroConverter desde a ingestão)

E a natureza do tópico define a natureza da tabela — o eco direto do append × updating do Curso 4:

| Natureza do tópico | A tabela materializada | Exemplo do bootcamp |
|---|---|---|
| **Append (fatos)** | Tabela de fatos, crescendo | `pedidos_enriquecidos` — cada evento, uma linha |
| **CDC / upsert (changelog)** | Tabela refletindo o **estado por chave** (a materialização CDC é automática) | O tópico Debezium do Curso 3 virando "a tabela `pedidos`, atual" |
| **Retract (Flink)** | **Não suportado** | Desenhar a saída do Flink em append/upsert quando o destino é o Tableflow |

- A materialização CDC fecha um ciclo bonito: o WAL virou eventos (Curso 3), e os eventos viram tabela de novo — **a dualidade do Curso 4 atravessando o Lakehouse**

### 2.2 A decisão de storage: CMS × BYOS *(~5 min)*

Onde os Parquet e metadados vivem — a escolha estrutural da habilitação:

| | **Confluent Managed Storage (CMS)** | **BYOS (seu bucket)** |
|---|---|---|
| Setup | **Zero** — "batteries included" | Provider integration + bucket (mesma região do cluster) |
| Quem opera o storage | Plataforma | Você (bucket, permissões, KMS) |
| Acesso às tabelas | **Só via o Iceberg REST Catalog do Tableflow**, com credenciais vendidas | Seu bucket — e via REST Catalog |
| Catálogos externos (Glue/Snowflake/Unity) | **Não sincroniza** | **Suportado** — o requisito da Aula 4 |
| Quando escolher | Estudo, protótipo, times sem requisito de posse do dado | Produção com Lakehouse próprio, integração com catálogos da empresa |

- **No bootcamp: CMS** — zero configuração e o aluno consome via REST Catalog (Aula 4). A limitação honesta, dita de frente: CMS **não** alimenta catálogos externos — quem precisa de Glue/Snowflake/Unity vai de BYOS
- **A anatomia do BYOS**: uma **provider integration** no nível do environment (a ponte de permissões IAM entre o Confluent Cloud e o seu bucket — mesma região do cluster, bucket **vazio** no início) — e vários tópicos Tableflow podem compartilhar a mesma integração
- **A regra de ouro dos arquivos:** o storage do Tableflow é **dele** — alterar ou apagar arquivos manualmente corrompe a tabela ou suspende a materialização. No BYOS, o bucket começa **vazio** e ninguém mais escreve nele
- **Decisão de uma via:** habilitou com um storage, não troca depois (CMS↔BYOS ou de bucket) — escolher **antes** é arquitetura, trocar depois é recriar

### 2.3 A outra decisão da habilitação: Iceberg × Delta Lake *(~2 min)*

Além do storage, o enable pergunta o **formato da tabela** — e a escolha define o ecossistema de consumo:

| | **Apache Iceberg** | **Delta Lake** |
|---|---|---|
| Status no Tableflow | O formato principal (GA primeiro) | Suportado (chegou depois — conferir disponibilidade atual por região/cloud) |
| Consumo | **Iceberg REST Catalog** (IRC) + engines abertos — o caminho das Aulas 2–4 | **Via Unity Catalog** — as tabelas Delta são publicadas diretamente na integração Databricks |
| Ecossistema-alvo | Neutro: Spark, Trino, DuckDB, Snowflake, AWS... | Databricks / Microsoft Fabric (OneLake) |
| No bootcamp | **O nosso formato** | Conceitual — citado aqui e na Aula 4 |

- A régua de decisão: **a casa analítica da empresa decide o formato** — mundo Databricks/Fabric consolidado → Delta faz sentido; qualquer outro cenário (ou multi-engine) → Iceberg, pelo alcance neutro
- Nuance importante (da doc): as tabelas **Delta do Tableflow são consumidas pela integração Unity Catalog** — não pelo IRC; quem escolhe Delta está escolhendo também o caminho de catálogo. Formato e consumo andam juntos
- E vale a mesma disciplina do storage: formato é decisão **da habilitação** — escolher olhando pra onde o dado vai ser consumido, não pro que parece moderno

### 2.4 A manutenção que você não escreve *(~4 min)*

O trabalho invisível que diferencia "arquivos no S3" de "tabela de verdade":

- **Compaction** — streaming gera arquivos pequenos continuamente; ler mil arquivinhos mata a performance analítica. O Tableflow **consolida** em arquivos maiores, sozinho — o job de manutenção que todo Lakehouse caseiro esquece de escrever (e paga caro)
- **Gestão de snapshots** — cada avanço da materialização gera snapshots; o Tableflow mantém automaticamente entre um mínimo e um máximo (na doc atual, 10 a 100 — não configurável), expirando os velhos: time travel recente sem metadado infinito
- **Retenção da tabela** — configurável na habilitação (`retention-ms`): por quanto tempo os snapshots/versões vivem — a prima analítica da retention do Curso 1
- **Status como recurso monitorável** — a materialização expõe estado (`Syncing`, `Paused`, `Failed`) e **lag** por tópico; e o sync de catálogo externo (Aula 4) tem status **independente** — a tabela pode estar `Syncing` com o catálogo em `Failed`, sem perda de materialização. Alarmar sobre esses estados é operação básica (Curso 7)
- A síntese: **o Tableflow é o "time de plataforma de Lakehouse" embutido** — a fronteira de gestão (Curso 2) chegando ao mundo analítico

**Configurações de referência — habilitação (CLI):**

| Config | O que faz | Ponto didático |
|---|---|---|
| `confluent tableflow topic enable <topic>` | Liga a materialização | Um comando, uma tabela |
| `--storage-type` | `MANAGED` (CMS, default) × `BYOS` | A decisão da seção 2.2 |
| `--table-formats` | `ICEBERG` (default) × `DELTA` | A decisão da seção 2.3: formato = ecossistema de consumo |
| `--retention-ms` | Vida dos snapshots (default: 7 dias) | A retention analítica |
| `--provider-integration` / `--bucket-name` | Só no BYOS | O universo da Aula 4/produção |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — UI + CLI. Alvo: `pedidos_enriquecidos` (Curso 4; dataset placeholder), storage **CMS**. Tempo de tela: ~7 min. **Tableflow tem custo por uso — desabilitar no teardown do curso (Aula 4).**

**O que a demo deve mostrar, nesta ordem:**

1. **Habilitar o Tableflow no tópico** — via UI (o botão "Enable Tableflow" na lista de tópicos), escolhendo CMS e Iceberg; repetir o equivalente via CLI na tela (a tabela de configs da seção 2.4, viva).
   - **Observação-alvo:** a habilitação é **uma decisão, não um projeto** — contraste explícito com o slide do "pipeline de antes" (Aula 1).

2. **Acompanhar o status da materialização** — o estado `Syncing` do tópico; o lag de materialização.
   - **Observação-alvo:** o Tableflow é um consumidor contínuo — o vocabulário do Curso 1 (offsets, lag) descrevendo um recurso analítico.

3. **Ver a tabela existir** — o Iceberg REST Catalog do Tableflow com a tabela registrada (endpoint + namespace na tela); o schema da tabela espelhando o subject do Registry.
   - **Observação-alvo:** o momento-síntese — **ninguém escreveu DDL, mapping nem job**: o schema Avro do Curso 2/4 virou o schema da tabela. (O consumo de verdade por um engine é a Aula 4 — plantar a expectativa.)

4. **A prova da continuidade** — injetar eventos novos no fluxo (Datagen pontual ou insert manual) e ver a tabela avançar (novo snapshot / contagem crescendo na consulta de status).
   - **Observação-alvo:** não é foto — é representação **viva**: o streaming e o analítico olhando o mesmo dado, com segundos de distância.

5. **Espiar a mecânica dos snapshots** — a lista/contagem de snapshots da tabela crescendo e sendo expirada.
   - **Observação-alvo:** a manutenção automática (seção 2.3) acontecendo sem nenhum job escrito.

**Fechamento na tela** *(~1 min)*: "um botão, uma tabela viva. Mas o herói silencioso dessa mágica é o schema — na próxima aula, dissecamos como o Registry vira DDL, como os tipos se convertem, e o que acontece quando o schema evolui."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Tableflow copia o tópico para uma tabela."** É **representação contínua** com metadados Iceberg — a distinção cópia × representação é o coração da aula (cena 4 da demo).
- **"Depois eu decido o storage."** É decisão de uma via — e CMS **não** sincroniza catálogos externos. Quem sabe que vai de Glue/Snowflake/Unity, começa em BYOS. Arquitetura antes do botão.
- **"Posso organizar os arquivos do bucket."** Os arquivos são do Tableflow — mexer corrompe ou suspende. O storage é área de máquina, não de gente.
- **"Meu tópico JSON sem schema serve."** Não serve — schemaless não materializa. A cobrança prometida no Curso 3 ("JSON sem schema cobra o preço lá na frente") vence aqui.
- **Custo:** Tableflow é recurso pago por uso — entra na lista de teardown do bootcamp (desabilitar ao final do curso, na Aula 4), junto de conectores e statements.

**Ganchos com a trilha:**

- Schematização, conversão de tipos e evolução → **Aula 3** (na sequência).
- Consumir a tabela com engines + catálogos externos → **Aula 4**.
- O tópico silver (`pedidos_enriquecidos`) materializado é a **camada Lakehouse do Desafio Final**.

**Perguntas para checar entendimento:**

1. Sua empresa quer as tabelas do Tableflow visíveis no AWS Glue para o time de Athena. Qual decisão de habilitação isso trava desde o dia 1, e por quê?
2. Em que sentido a tabela Iceberg do Tableflow é "read-only" — e onde se muda o dado quando ele precisa mudar?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Materialização (Tableflow)** | A representação contínua de um tópico como tabela Iceberg — não uma cópia batch. |
| **CMS (Confluent Managed Storage)** | Storage gerenciado pela plataforma — zero setup; acesso só via REST Catalog; sem sync de catálogo externo. |
| **BYOS (Bring Your Own Storage)** | Seu bucket (S3/Azure, mesma região) — requisito para catálogos externos. |
| **Provider integration** | A ponte de permissões entre o Confluent Cloud e o seu bucket (BYOS). |
| **Iceberg REST Catalog (do Tableflow)** | O catálogo embutido onde as tabelas materializadas são publicadas e descobertas. |
| **Credenciais vendidas (vended credentials)** | Credenciais temporárias emitidas pelo REST Catalog para acesso aos dados em CMS. |
| **Compaction (Tableflow)** | Consolidação automática de arquivos pequenos em maiores — performance analítica sem job seu. |
| **Lag de materialização** | O atraso entre o evento no tópico e sua presença na tabela. |
| **Status da materialização** | O estado do pipeline por tópico: `Syncing`, `Paused`, `Failed` — monitorável e alarmável. |
| **Materialização CDC/upsert** | Tópicos changelog materializados como o estado atual por chave (automático). |
| **Delta Lake (no Tableflow)** | O formato alternativo de materialização — consumido via Unity Catalog; alvo Databricks/Fabric. |
| **`retention-ms` (Tableflow)** | Tempo de vida dos snapshots da tabela materializada. |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Tableflow Overview (capacidades e limitações)* — https://docs.confluent.io/cloud/current/topics/tableflow/overview.html
- Confluent Cloud — *Storage with Tableflow (CMS × BYOS)* — https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-storage.html
- Confluent Cloud — *Tableflow Quick Start (Managed Storage)* — https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-managed-storage.html
- Confluent CLI — *`confluent tableflow topic enable`* — https://docs.confluent.io/confluent-cli/current/command-reference/tableflow/topic/confluent_tableflow_topic_enable.html
- Apache Iceberg — *Table Spec (snapshots, manifests)* — https://iceberg.apache.org/spec/
