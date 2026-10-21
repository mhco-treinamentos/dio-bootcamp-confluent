# Curso 5 · Aula 1 — Introdução ao Lakehouse e ao Tableflow: unindo os mundos operacional e analítico

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Aula 100% conceitual (o hands-on começa na Aula 2)
> **Pré-requisitos:** Cursos 1–4 (em especial `pedidos_enriquecidos`, do Curso 4)

---

## 1. Objetivo da Aula

Abrir o caminho analítico do bootcamp: entender a divisão histórica entre os mundos **operacional** (o streaming que construímos) e **analítico** (lakes e warehouses), o que o **Lakehouse** e o **Apache Iceberg** resolvem, e como o **Tableflow** elimina o ETL do meio — materializando tópicos como tabelas abertas, com o **shift-left** (o Flink do Curso 4 enriquecendo **antes** de materializar).

---

## 2. Conceitos Simplificados

### 2.1 Os dois mundos e o abismo entre eles *(~4 min)*

```
 MUNDO OPERACIONAL                    MUNDO ANALÍTICO
 (o bootcamp até aqui)               (BI, ciência de dados, relatórios)
 Kafka · eventos · segundos          lake/warehouse · tabelas · consultas pesadas
 "o que está acontecendo?"           "o que aconteceu, no agregado?"
        │                                    ▲
        └───────── O ABISMO: ETL ────────────┘
          jobs batch noturnos · cópias · scripts frágeis
          o dado chega VELHO e QUEBRADO do outro lado
```

- Os dois mundos têm físicas legítimas e diferentes (o panorama de destinos do Curso 3 já mostrava) — o problema é a **ponte**: pipelines de ETL sob medida, rodando em batch, duplicando dado, quebrando a cada mudança de schema
- O sintoma clássico: o dashboard de "vendas de hoje" mostra **ontem** — e o time de dados passa o dia consertando pipeline em vez de analisar
- A tese do curso: se o dado **já nasce como evento estruturado com contrato** (tudo que construímos nos Cursos 1–4), a ponte não precisa ser um pipeline — pode ser uma **representação**

### 2.2 O Lakehouse: o lake com garantias de warehouse *(~3 min)*

A evolução em três atos:

| | **Data Warehouse** | **Data Lake** | **Lakehouse** |
|---|---|---|---|
| Storage | Proprietário, caro | Object storage barato (S3...) | Object storage barato |
| Estrutura | Forte (schema, ACID) | Fraca ("pântano de arquivos") | **Forte — via formato de tabela aberto** |
| Acesso | O engine do fornecedor | Qualquer ferramenta, sem garantias | **Qualquer engine, com garantias** |
| O problema | Custo e lock-in | Virou pântano sem governança | — |

- O **Lakehouse** = arquivos abertos (Parquet) no storage barato **+ uma camada de metadados** que devolve as garantias de warehouse: schema, transações ACID, evolução, viagem no tempo
- A peça que torna isso possível é o **formato de tabela aberto** — e o vencedor da categoria é o **Apache Iceberg**

### 2.3 Anatomia do Apache Iceberg *(~4 min)*

Iceberg não é um banco nem um engine — é uma **especificação de metadados** sobre arquivos, em quatro níveis:

```
 TABELA ICEBERG — a árvore de metadados
 ① metadata file (JSON)      → schema, particionamento, e a lista de snapshots;
 │                              o "ponteiro-raiz": a versão atual da tabela
 ② └─ manifest list (1/snapshot) → "o snapshot N é composto por ESTES manifests"
 ③      └─ manifests (Avro)      → listas de arquivos de dados + ESTATÍSTICAS
 │                                  (min/max por coluna, contagens — o segredo do pruning)
 ④           └─ data files (Parquet) → os dados de fato, imutáveis, no object storage
```

- **Snapshot = a tabela em um instante** — cada escrita cria um snapshot novo (um novo apontamento na árvore); ler é escolher um snapshot consistente (ACID sem banco no meio) e **time travel** é ler um snapshot antigo: `SELECT ... FOR TIMESTAMP AS OF`
- **As estatísticas dos manifests são o motor da performance:** o engine descarta arquivos inteiros sem abri-los ("esse Parquet só tem pedidos de janeiro; a query pede março") — **file pruning**. É por isso que Lakehouse consulta rápido sem índice tradicional
- **Hidden partitioning** — o particionamento é declarado como transformação (`day(ts)`, `bucket(id)`) e vive nos metadados: o analista escreve `WHERE ts = ...` normal e o Iceberg poda partições sozinho — sem as colunas-fantasma (`year=/month=/day=`) do mundo Hive
- Soa familiar? **É a imutabilidade + versões do Curso 1 aplicada a tabelas** — arquivos não mudam; o que muda é o apontamento. O aluno já tem o modelo mental
- Embaixo de tudo, o **Parquet**: formato **colunar** — a query que pede 3 colunas lê só 3 colunas do disco (o oposto do row-based do mundo operacional). Colunar + estatísticas + pruning = a física da analítica rápida
- O que falta ao Iceberg sozinho: alguém que **escreva** as tabelas a partir do streaming — historicamente, mais um pipeline. Entra o Tableflow

### 2.4 O mercado dos formatos: Iceberg × Delta Lake × Hudi *(~2 min — material de consulta)*

A "guerra dos formatos de tabela" que o aluno vai encontrar em toda discussão de Lakehouse:

| | **Apache Iceberg** | **Delta Lake** | **Apache Hudi** |
|---|---|---|---|
| Origem | Netflix → Apache | Databricks | Uber → Apache |
| Governança | Fundação neutra | Aberto, com gravidade Databricks | Fundação neutra |
| Força | Especificação limpa, hidden partitioning, adoção ampla (Snowflake, AWS, Google...) | Integração profunda com Databricks/Spark | Ingestão upsert-heavy, CDC-first |
| Momento | **O padrão de facto neutro** do mercado | Forte no ecossistema Databricks | Nicho de ingestão intensiva |
| No Tableflow | **GA — o formato do bootcamp** | Suportado (verificar disponibilidade atual) | — |

Leitura honesta: os três resolvem o mesmo problema com trade-offs; o **Iceberg venceu a neutralidade** (todo grande vendor o suporta) e por isso é o eixo deste curso — mas "qual formato" importa menos que "ter formato": qualquer um deles > pântano de arquivos.

### 2.5 Tableflow e o shift-left *(~5 min)*

O **Tableflow** materializa tópicos Kafka como tabelas Iceberg **por habilitação, não por pipeline**: schematização, conversão de tipos, evolução de schema e manutenção dos arquivos, tudo automático — o tópico ganha uma representação de tabela, sem duplicação de lógica.

```
 ANTES (o abismo):
 topic ─► sink ─► arquivos ─► job Spark ─► tabela ─► (quebrou o schema, chama o plantão)

 COM TABLEFLOW:
 topic ══════════════════════════► tabela Iceberg  (uma habilitação; o SR dita o schema)

 E O SHIFT-LEFT (a tese do curso):
 CDC (C3) ─► topic bruto ─► FLINK enriquece/limpa (C4) ─► topic curado ─► TABLEFLOW ─► Iceberg
                            └── o trabalho migra para a ESQUERDA (perto da origem),
                                e o dado chega ao analítico JÁ PRONTO e FRESCO
```

- **Shift-left** — em vez de materializar dado cru e limpar depois (o eterno retrabalho do time de dados), o Flink transforma **antes**: `pedidos_enriquecidos` (Curso 4) é exatamente o candidato — curado na origem, consumido por todos
- A leitura em camadas do mercado (medallion): o tópico bruto materializado é o **bronze**; o curado pelo Flink, o **silver** — o gold continua sendo trabalho do time analítico, mas começando quilômetros à frente
- A dualidade da Aula 2 do Curso 4 fechando o ciclo: lá, **tópico virou tabela no Flink**; aqui, **tópico vira tabela no Lakehouse** — a mesma ideia, agora atravessando a fronteira analítica

**Fechamento** *(~1 min)*: "dois mundos, uma representação — sem ETL no meio. Na próxima aula, apertamos o botão: habilitar o Tableflow num tópico de verdade e dissecar o que acontece por baixo."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Lakehouse é marketing para data lake."** A diferença é objetiva: o formato de tabela (Iceberg) devolve schema, ACID e evolução — o que o lake puro nunca teve. Sem essa camada, é pântano com nome novo.
- **"Iceberg é um banco/engine."** É especificação de metadados sobre arquivos abertos — o poder está exatamente em **não** ser um engine: todos leem a mesma tabela.
- **"Tableflow é um sink de S3 com outro nome."** O sink entrega **arquivos** (e o resto — tabela, schema, manutenção — é seu problema, como a Aula 3 do Curso 3 avisou); o Tableflow entrega **a tabela pronta e mantida**. A comparação honesta é com o pipeline inteiro que ele substitui.
- **"Shift-left significa que o time analítico não trabalha mais."** Significa que ele **começa do silver** — modelagem gold, métricas e análise continuam sendo o trabalho; o que morre é o retrabalho de limpeza.
- **Números e disponibilidade evoluem** (formatos suportados, regiões, Delta Lake) — narrar a estrutura; confirmar detalhes na doc atual.

**Ganchos com a trilha:**

- Habilitar e dissecar a materialização → **Aula 2**.
- O Schema Registry como fonte da verdade da tabela → **Aula 3**.
- Catálogos e o consumo pelos engines → **Aula 4**.
- `pedidos_enriquecidos` como o silver do **Desafio Final** — a camada Lakehouse do capstone nasce neste curso.

**Perguntas para checar entendimento:**

1. Por que o dashboard "de hoje" mostrar ontem é um sintoma **arquitetural**, e não um problema de ferramenta de BI?
2. Explique a analogia entre snapshots do Iceberg e o log imutável do Curso 1 — o que "muda" quando uma tabela Iceberg é atualizada?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **Mundo operacional / analítico** | Sistemas que operam o negócio em tempo real × sistemas que o analisam em agregado. |
| **ETL (Extract, Transform, Load)** | O pipeline tradicional de cópia e transformação entre os mundos — o "abismo". |
| **Data Lakehouse** | Object storage barato + formato de tabela aberto = garantias de warehouse sobre o lake. |
| **Apache Iceberg** | Formato de tabela aberto: metadados (snapshots, manifests) sobre arquivos Parquet. |
| **Snapshot (Iceberg)** | A tabela em um instante — a unidade de consistência e do time travel. |
| **Manifest / manifest list** | Os níveis intermediários da árvore: listas de arquivos com estatísticas por coluna. |
| **File pruning** | Descartar arquivos sem abri-los usando as estatísticas dos manifests — a física da consulta rápida. |
| **Hidden partitioning** | Particionamento declarado como transformação nos metadados — sem colunas-fantasma na query. |
| **Time travel** | Consultar um snapshot antigo da tabela (`FOR TIMESTAMP AS OF`). |
| **Delta Lake / Apache Hudi** | Os outros formatos de tabela abertos do mercado (Databricks / Uber-Apache). |
| **Parquet** | Formato de arquivo colunar aberto — o dado físico do Lakehouse. |
| **Tableflow** | Recurso do Confluent Cloud que materializa tópicos como tabelas Iceberg/Delta, automaticamente. |
| **Shift-left** | Mover transformação e qualidade para perto da origem (Flink antes da materialização). |
| **Arquitetura medallion (bronze/silver/gold)** | Camadas de refinamento: cru → curado → modelado para consumo. |

---

## 5. Referências

**Livros:**

- Tomer Shiran, Jason Hughes & Alex Merced — *Apache Iceberg: The Definitive Guide* (O'Reilly) — a anatomia do formato na fonte
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 10–11 (batch × stream, derivação de dados)

**Documentação e artigos oficiais:**

- Confluent Cloud — *Tableflow Overview* — https://docs.confluent.io/cloud/current/topics/tableflow/overview.html
- Apache Iceberg — *Spec e documentação* — https://iceberg.apache.org/docs/latest/
- Confluent — *Tableflow (página de produto e visão shift-left)* — https://www.confluent.io/product/tableflow/
- Confluent — *Shift Left: Bad Data in Event Streams* (a tese do shift-left) — https://www.confluent.io/blog/shift-left-bad-data-in-event-streams-part-1/
