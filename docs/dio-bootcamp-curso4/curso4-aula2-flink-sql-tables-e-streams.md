# Curso 4 · Aula 2 — Flink SQL: Tables, Streams e modelando dados em movimento

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 1 do Curso 4 (compute pool criado)

---

## 1. Objetivo da Aula

Entender a ideia mais elegante do curso — a **dualidade stream ↔ table** — e como o Flink no Confluent Cloud a materializa: **todo tópico é uma tabela, todo resultado é um tópico**, com o mapeamento **catálogo → database → table** e o Schema Registry fornecendo o DDL de graça. Ao final, o aluno cria sua primeira tabela derivada — um statement perpétuo alimentando um tópico novo.

---

## 2. Conceitos Simplificados

### 2.1 A dualidade: tabela é stream parado; stream é tabela em movimento *(~5 min)*

A epifania que unifica os mundos (e ecoa o CDC do Curso 3):

```
 STREAM (o filme)                          TABLE (o fotograma atual)
 a sequência COMPLETA de mudanças          o estado AGORA, por chave
 ┌──────────────────────────────┐
 │ ord-1: criado                │          ┌──────┬─────────┐
 │ ord-2: criado                │  ──────► │ ord-1│ enviado │
 │ ord-1: pago                  │  agrega  │ ord-2│ pago    │
 │ ord-2: pago                  │  p/ chave└──────┴─────────┘
 │ ord-1: enviado               │
 └──────────────────────────────┘  ◄──────  cada UPDATE na tabela
                                   changelog emite um evento novo
```

- **Stream → table:** aplicar as mudanças em ordem, por chave, "comprime" o filme no fotograma — é uma **materialização** (foi o que o sink com upsert fez no Curso 3!)
- **Table → stream:** registrar cada mudança da tabela gera o filme de volta — é um **changelog** (foi o que o CDC fez com o WAL!)
- O Curso 3 inteiro já era essa dualidade disfarçada: **WAL → eventos** (table→stream) e **eventos → upsert** (stream→table). O Flink apenas a torna **explícita e operável em SQL**
- Consequência prática: dá para fazer SQL "de banco" sobre fluxos — porque, no fundo, banco e fluxo são duas vistas do mesmo dado

### 2.2 Anatomia do mapeamento no Confluent Cloud *(~4 min)*

O Flink SQL enxerga o mundo do bootcamp assim:

```
 FLINK SQL                       CONFLUENT CLOUD
 catalog      ◄────────────────  environment   (bootcamp-dio)
   └─ database ◄───────────────    cluster     (o nosso Basic)
        └─ table ◄─────────────      topic     (pedidos, clientes, ...)
              └─ colunas ◄─────        schema  (Schema Registry!)
```

- **Nenhum `CREATE TABLE` para começar** — os tópicos com schema Avro (Cursos 2–3) **já aparecem como tabelas**, com colunas tipadas vindas do Registry. O contrato de dados pagando dividendos
- **Metadados do Kafka viram colunas de sistema** — partição, offset, timestamp acessíveis na query (`$rowtime` é o event time — protagonista da Aula 3)
- **Tabela nova = tópico novo** — `CREATE TABLE` cria um tópico (com schema registrado); não existe "tabela do Flink" separada do Kafka: **o Kafka é o storage**

### 2.3 Os dois modos de tabela: append × updating *(~4 min)*

Nem todo fluxo tem a mesma natureza — e o Flink distingue:

| | **Append (insert-only)** | **Updating (changelog/upsert)** |
|---|---|---|
| Natureza | Fatos que só se acumulam | Estado por chave que evolui |
| Exemplo do bootcamp | `pedidos` (cada evento é um fato novo) | "status ATUAL por pedido"; resultado de agregação |
| Cada linha é... | Um evento imutável (Curso 1!) | O valor mais recente de uma chave |
| No Kafka vira... | Tópico normal | Tópico com semântica de upsert (par natural da compaction — Curso 1, Aula 3) |

- Um `SELECT` simples sobre `pedidos` produz **append** — eventos passando
- Um `GROUP BY` ou "último estado por chave" produz **updating** — resultados que se **corrigem** conforme chegam dados (a tela mostra valores sendo atualizados, não acrescentados)
- Entender qual modo a query produz evita a surpresa clássica ("por que o resultado mudou?") e prepara as agregações da Aula 3

**Sintaxe de referência — o vocabulário mínimo da aula:**

| Comando | O que faz | Ponto didático |
|---|---|---|
| `SHOW CATALOGS / DATABASES / TABLES` | Navega o mapeamento | Environment → cluster → tópicos |
| `DESCRIBE pedidos` | Colunas e tipos | O schema Avro renderizado como DDL |
| `SELECT ... FROM ... WHERE ...` | Query contínua (append) | O SQL de sempre, comportamento novo |
| `CREATE TABLE ... AS SELECT ...` (CTAS) | Cria tópico novo + statement perpétuo populando | A "view materializada" do streaming |
| `INSERT INTO ... SELECT ...` | Statement perpétuo para tabela existente | O irmão do CTAS |
| `$rowtime` | Coluna de sistema com o event time | A ponte para a Aula 3 |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — workspace SQL do Flink. Fonte: tópicos do Curso 3 (dataset placeholder). Tempo de tela: ~7 min. **Statements perpétuos parados ao final.**

**O que a demo deve mostrar, nesta ordem:**

1. **Navegar o mapeamento** — `SHOW CATALOGS`, `SHOW DATABASES`, `SHOW TABLES`: o environment, o cluster e os tópicos do bootcamp aparecendo como catálogo/database/tabelas, **sem nenhum DDL escrito**.
   - **Observação-alvo:** a seção 2.2 na tela — e a pergunta retórica: "quem escreveu esses CREATE TABLE? O Schema Registry."

2. **`DESCRIBE pedidos`** — colunas tipadas, e as colunas de sistema (`$rowtime` em destaque: "guardem essa — Aula 3").
   - **Observação-alvo:** o contrato Avro do Curso 2 virando estrutura SQL de graça.

3. **Query append** — projeção + filtro contínuo (`SELECT order_id, total FROM pedidos WHERE total > 200`).
   - **Observação-alvo:** linhas **se acumulando** — modo append, eventos passando.

4. **Query updating** — um `GROUP BY` simples (ex.: contagem de pedidos por status, sem janela ainda).
   - **Observação-alvo:** os valores **mudando no lugar** em vez de acumular — modo updating, o contraste visual com a cena 3. ("Por que muda? Porque o resultado é uma tabela viva — e semana que vem a gente doma isso com janelas.")

5. **CTAS: a primeira tabela derivada** — `CREATE TABLE pedidos_grandes AS SELECT ... WHERE total > 200`; em seguida, **sair do Flink** e mostrar no cluster: o **tópico novo existe**, com schema no Registry, recebendo eventos do statement perpétuo.
   - **Observação-alvo:** o momento-síntese — o resultado do Flink É um tópico Kafka de primeira classe: consumível por qualquer consumer, sink, ou outro statement. Não há "dentro do Flink".

6. **Teardown** — parar o statement do CTAS (o tópico derivado fica); regra reafirmada: statement perpétuo esquecido = crédito queimando para sempre.

**Fechamento na tela** *(~1 min)*: "tópicos como tabelas, resultados como tópicos, e a dualidade operando em SQL. Mas repare que a cena 4 contava *tudo desde sempre* — falta domar o tempo: agregações por janela, joins e o relógio do stream. Próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"O Flink guarda as tabelas em algum lugar próprio."** Não — **o Kafka é o storage**; tabela é tópico (+ schema). Derrubar essa imagem cedo evita metade das confusões do curso.
- **"Tabela é tabela, stream é stream."** São **vistas** do mesmo dado — o filme e o fotograma. Amarrar explicitamente ao Curso 3 (WAL→eventos, eventos→upsert): o aluno já viveu a dualidade sem nome.
- **"O resultado do GROUP BY está errado, ficou mudando."** É o modo updating funcionando — resultado de streaming é **vivo por definição**. A cena 4 existe para provocar exatamente essa reação e respondê-la.
- **"CTAS é um comando que termina."** Cria um tópico E um processo perpétuo — dois recursos, dois custos. O teardown da cena 6 é pedagógico.
- **Disciplina de escopo:** changelog kinds em detalhe (retract × upsert), connectors de tabela externos e DDL avançado ficam fora — beginner precisa da dualidade e do mapeamento, não da taxonomia completa.

**Ganchos com a trilha:**

- `$rowtime`, janelas e watermarks → **Aula 3** (na sequência).
- Tabelas derivadas com estado e sua sobrevivência a falhas → **Aula 4**.
- Tópico como tabela é literalmente o **Tableflow ao contrário** — o Curso 5 leva a mesma dualidade para o Lakehouse (Iceberg).
- No **Desafio Final**, a camada de transformação nasce como CTAS/INSERT INTO sobre a ingestão do Curso 3.

**Perguntas para checar entendimento:**

1. Você fez `CREATE TABLE clientes_vip AS SELECT ...`. Liste TUDO que foi criado (recursos) e o que continua existindo se você parar o statement.
2. Por que a contagem por status "muda sozinha" na tela, e que natureza de tabela isso revela?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Dualidade stream-table** | Princípio: tabela é a materialização de um stream; stream é o changelog de uma tabela. |
| **Materialização** | Aplicar as mudanças de um stream, por chave, para obter o estado atual. |
| **Changelog** | O fluxo de mudanças que reconstrói uma tabela. |
| **Catalog / database / table (Flink no CC)** | O mapeamento: environment / cluster / tópico. |
| **Append table (insert-only)** | Tabela cujas linhas só se acumulam — fatos imutáveis. |
| **Updating table** | Tabela cujo estado por chave evolui — resultados vivos (agregações, último estado). |
| **CTAS (`CREATE TABLE AS SELECT`)** | Cria um tópico novo + statement perpétuo que o alimenta. |
| **`$rowtime`** | Coluna de sistema com o event time do registro (base das janelas — Aula 3). |
| **Colunas de sistema** | Metadados do Kafka (partição, offset, timestamp) expostos na query. |

---

## 6. Referências

**Livros:**

- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11: a dualidade log ↔ estado na fonte
- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — tables and streams como fundação dos serviços orientados a eventos. E-book gratuito: https://www.confluent.io/designing-event-driven-systems/
- Tyler Akidau et al. — *Streaming Systems* (O'Reilly) — caps. 6 e 8 (streams & tables)

**Documentação e artigos oficiais:**

- Confluent Cloud — *Flink SQL: Stream/Table Duality e conceitos* — https://docs.confluent.io/cloud/current/flink/concepts/overview.html
- Confluent Cloud — *Flink SQL Statements (CTAS, INSERT INTO)* — https://docs.confluent.io/cloud/current/flink/reference/statements/overview.html
- Apache Flink — *Dynamic Tables (a dualidade na doc do motor)* — https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/
- Michael Noll (Confluent) — *Streams and Tables in Apache Kafka: A Primer* — https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/
