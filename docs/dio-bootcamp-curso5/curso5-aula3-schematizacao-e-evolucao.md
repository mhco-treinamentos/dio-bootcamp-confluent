# Curso 5 · Aula 3 — Schematização, Type Conversion e Evolução de Schema com o Schema Registry

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 2 do Curso 5 (tabela materializada) + Curso 2, Aula 3 (Schema Registry)

---

## 1. Objetivo da Aula

Dissecar o herói silencioso da materialização: como o **Schema Registry vira a fonte da verdade da tabela** (schema Avro → DDL Iceberg), o mapa de **conversão de tipos**, e o momento decisivo — **a evolução do schema**: o que o Tableflow aplica sozinho, o que ele **suspende**, e por que a disciplina BACKWARD do Curso 2 paga o maior dividendo do bootcamp exatamente aqui.

---

## 2. Conceitos Simplificados

### 2.1 O Registry como fonte da verdade da tabela *(~4 min)*

A regra estrutural do Tableflow:

> **O schema do subject define o schema da tabela.** Tópico + schema são a fonte da verdade; a tabela Iceberg é a representação derivada — read-only, sempre em sincronia.

```
 SCHEMA REGISTRY                         TABELA ICEBERG
 subject: pedidos_enriquecidos-value     CREATE TABLE (gerado, nunca escrito)
 {                                       ├─ key          binary
   "name": "PedidoEnriquecido",   ────►  ├─ order_id     string
   "fields": [                           ├─ total        double
     {"name":"order_id","type":"string"} ├─ canal        string (nullable)
     {"name":"total","type":"double"}    └─ ...
     {"name":"canal","type":[...]}
 }
```

- Repare na primeira coluna do exemplo: a **key do Kafka entra na tabela como `key binary`** — a identidade que roteou partições (Curso 1) e dirigiu upserts (Curso 3) chega ao analítico como coluna de pleno direito
- O ciclo completo do contrato: nasceu no **Curso 2** (o portão), viajou pela ingestão no **Curso 3** (converters/CDC), virou DDL no Flink no **Curso 4**, e agora vira **tabela analítica** — um schema, quatro dividendos
- Consequência de governança: **quem controla o subject controla a tabela** — evolução de schema deixa de ser assunto "do time de streaming" e passa a ser contrato com o time analítico também
- Detalhe do mundo real que o Tableflow resolve: tópicos com **múltiplos tipos de evento** (TopicNameStrategy) são normalizados numa tabela única, com cada tipo virando coluna estruturada — citar; aprofundar é intermediate

### 2.2 Type conversion: o mapa entre dois mundos de tipos *(~4 min)*

Avro e Iceberg têm sistemas de tipos primos, mas não idênticos — o Tableflow traduz:

| Avro (o evento) | Iceberg (a tabela) | Nota didática |
|---|---|---|
| `string` | `string` | Direto |
| `int` / `long` | `int` / `long` | Direto |
| `float` / `double` | `float` / `double` | Direto |
| `boolean` | `boolean` | Direto |
| `bytes` / `fixed` | `binary` | A key do Kafka aparece assim |
| `logicalType: decimal` | `decimal(p,s)` | **Dinheiro é decimal, não double** — a lição de modelagem |
| `logicalType: timestamp-millis` | `timestamp` | O tempo do evento como tipo de verdade |
| `record` (aninhado) | `struct` | Estruturas sobrevivem |
| `array` / `map` | `list` / `map` | Coleções sobrevivem |
| `union [null, T]` | `T` nullable | O campo opcional do Curso 2 virando NULL SQL |

- A tabela é **material de consulta** — em vídeo, narrar três linhas: o `decimal` (a pegadinha do dinheiro), o `timestamp` (event time chegando ao analítico) e a `union` com null (o opcional que viaja bem)
- A moral de modelagem: **o tipo escolhido no schema Avro no dia 1 é o tipo que o analista consulta no dia 1000** — capricho no contrato é capricho no Lakehouse

### 2.3 Evolução: o que passa, o que suspende *(~5 min)*

O schema evolui (Curso 2 garantiu isso) — e a tabela precisa acompanhar. O Tableflow valida cada mudança contra o Registry e a aplica **automaticamente quando compatível**:

```
 EVOLUÇÕES QUE O TABLEFLOW APLICA SOZINHO:
 ✓ campo novo opcional (nullable, default null) → coluna nova aparece
 ✓ remoção de campo (onde a compatibilidade permite)
 ✓ ALARGAMENTO de tipo: int → long · float → double · decimal ganhando precisão

 O QUE ELE NÃO ACEITA:
 ✗ mudança incompatível com o modo do subject
   → a materialização do tópico é SUSPENSA (não corrompe — para)
   → intervenção manual para resolver
```

O caminho completo de uma evolução, com as duas linhas de defesa:

```
 dev registra o schema v2 no subject
    ▼
 ① O PORTÃO (Registry · Curso 2): compatível com o modo do subject?
    ├─ NÃO → rejeitado na origem — nem chega a existir dado v2       ✋
    └─ SIM → producers passam a publicar v2
              ▼
 ② O TABLEFLOW valida a mudança contra o Registry na materialização
    ├─ compatível  → APLICA na tabela (coluna nova / widening)       ✓
    └─ incompatível (ex.: modo NONE deixou passar, edge cases)
                   → SUSPENDE a materialização do tópico             🛑
                     (a tabela congela consistente; intervenção manual)
```

- **A escolha de design é reveladora:** diante do incompatível, o Tableflow prefere **parar a mentir** — suspender preserva a tabela consistente; "dar um jeito" produziria colunas fantasma e dado corrompido. É o portão do Curso 2 com consequência física
- Note o papel do **modo do subject**: com BACKWARD bem configurado, a linha ② quase nunca dispara — o portão segura antes. Subject em `NONE` é dirigir sem cinto **e** sem freio
- O fluxo maduro, portanto, não muda: **campo novo, opcional, com default** (a receita do Curso 2) → passa no BACKWARD → o Tableflow aplica → a coluna aparece na tabela → nenhum consumer nem analista quebra
- E o fluxo imaturo dói em dobro agora: uma breaking change forçada não quebra só consumers — **congela o Lakehouse**

### 2.4 As fronteiras honestas *(~3 min)*

O que o Tableflow **não** materializa (e por quê) — a lista de "conheça antes de prometer":

- **Tópico sem schema** — sem contrato, sem tabela (a Aula 2 já cravou)
- **Tabelas em modo retract do Flink** — o changelog retract (Curso 4) não tem representação suportada; desenhar a saída do Flink em append/upsert quando o destino é o Tableflow
- **A tabela é read-only** — engines consultam; quem "escreve" é o fluxo. Pipeline analítico que precisa escrever de volta é outro desenho (e outro curso de arquitetura)
- Detalhes finos (CSFLE, formatos, limites) evoluem — a doc é a fonte; o markdown de apoio traz os links

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: a tabela materializada da Aula 2 (dataset placeholder). Tempo de tela: ~6 min.

**O que a demo deve mostrar, nesta ordem:**

1. **O espelho subject ↔ tabela** — lado a lado: o schema Avro no Registry e o schema da tabela no REST Catalog; apontar as conversões de tipo (o `decimal`, o `timestamp`, o nullable).
   - **Observação-alvo:** a seção 2.2 viva — cada linha do subject com seu reflexo tipado na tabela.

2. **Evolução compatível, fim a fim** — registrar uma nova versão do schema com um campo opcional novo (a receita do Curso 2), produzir eventos com o campo, e ver **a coluna nova aparecer na tabela** com os valores chegando (e NULL nos registros antigos).
   - **Observação-alvo:** o momento-síntese do curso — **uma evolução de contrato atravessou ingestão, streaming e Lakehouse sem quebrar nada nem exigir um job**. O dividendo do BACKWARD, pago na tela.

3. **A incompatível (narrada com segurança)** — mostrar a tentativa de mudança incompatível sendo **rejeitada pelo Registry** (como no Curso 2, cena 5) e narrar sobre a doc/diagrama o comportamento de suspensão do Tableflow caso dado incompatível chegasse à materialização.
   - **Observação-alvo:** as duas linhas de defesa — o portão (Registry) e o freio (suspensão) — e por que parar > corromper. *(Não suspender a tabela de verdade na demo: recuperar toma tempo de aula; a rejeição do portão + narração cobrem a lição.)*

**Fechamento na tela** *(~1 min)*: "o contrato do Curso 2 agora governa até o Lakehouse. Falta a última milha: quem consome essas tabelas — engines, catálogos e o mundo Glue/Snowflake/Databricks. Última aula do curso."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"A tabela tem schema próprio, gerencio nela."** A tabela é derivada e read-only — o schema se gerencia **no subject**. Tentar governar na ponta errada é o anti-padrão da aula.
- **"Evolução de schema é assunto do time de streaming."** Depois do Tableflow, é contrato com o analítico também — a coluna que o dashboard usa nasce de um campo Avro. Governança de dados unificada de fato.
- **"Double serve para dinheiro."** A tabela de tipos existe para essa linha: `decimal` com precisão — o erro de arredondamento do streaming vira erro de balanço no Lakehouse.
- **"Suspender a materialização é um bug."** É a escolha certa: consistência > disponibilidade da representação. O bug seria a tabela mentir.
- **Disciplina de relógio:** a tabela de conversão é consulta (narrar 3 linhas); múltiplos event types e CSFLE são menção com link.

**Ganchos com a trilha:**

- O portão de compatibilidade e a receita do campo opcional → **Curso 2, Aula 3** (o par desta aula).
- Upsert/changelog do Flink e o modo da tabela → **Curso 4, Aula 2**.
- Consumo e catálogos → **Aula 4** (na sequência).
- No **Desafio Final**, o README deve mostrar uma evolução de schema sobrevivendo fim a fim — esta aula é o roteiro.

**Perguntas para checar entendimento:**

1. O time quer adicionar `desconto` aos pedidos e o dashboard não pode quebrar. Escreva o plano: como declarar o campo, o que o Registry valida, o que o Tableflow faz, e o que o analista vê.
2. Por que o Tableflow suspende diante do incompatível em vez de "criar a coluna do jeito que der"? O que essa escolha protege?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Schematização (Tableflow)** | Derivar o schema da tabela a partir do subject no Schema Registry — a fonte da verdade. |
| **Type conversion** | A tradução automática de tipos Avro/Protobuf/JSON Schema para tipos Iceberg. |
| **Type widening (alargamento)** | Evolução aceita que amplia o tipo: int→long, float→double, decimal com mais precisão. |
| **`decimal(p,s)`** | O tipo correto para valores monetários — precisão e escala explícitas. |
| **Suspensão da materialização** | O freio do Tableflow diante de mudança incompatível: parar preservando a consistência. |
| **Multiple event types (normalização)** | Tópico com vários tipos de evento materializado como tabela única com colunas por tipo. |
| **Read-only (tabela derivada)** | A tabela não recebe escrita: muda-se o dado na origem (tópico/schema). |
| **`key binary`** | A key do Kafka materializada como coluna da tabela — a identidade atravessando os mundos. |

---

## 6. Referências

**Livros:**

- Tomer Shiran et al. — *Apache Iceberg: The Definitive Guide* (O'Reilly) — schema evolution no formato
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 4 (encoding e evolução — o fundamento, de novo)

**Documentação oficial:**

- Confluent Cloud — *Tableflow: Schemas e Supported Types (conversão e evolução)* — https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-schemas.html
- Confluent — *Schema Evolution and Compatibility (o portão do Curso 2)* — https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Apache Iceberg — *Schema Evolution* — https://iceberg.apache.org/docs/latest/evolution/
- Apache Avro — *Specification (tipos e logical types)* — https://avro.apache.org/docs/current/specification/
