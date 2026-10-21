# Curso 5 · Aula 4 — Catálogos e Consumo Analítico: AWS Glue, Snowflake Open Catalog e Databricks Unity Catalog

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo (consumo hands-on via REST Catalog; catálogos externos conceituais)
> **Pré-requisitos:** Aulas 1–3 do Curso 5

---

## 1. Objetivo da Aula

Fechar a última milha: como os **engines analíticos descobrem e consultam** as tabelas do Tableflow. Dissecar o papel do **catálogo Iceberg**, consumir a tabela do bootcamp **de verdade** via o **Iceberg REST Catalog** embutido, e mapear a integração com os catálogos do mercado — **AWS Glue, Snowflake Open Catalog e Databricks Unity Catalog** — com seus requisitos honestos.

---

## 2. Conceitos Simplificados

### 2.1 O catálogo: a lista telefônica do Lakehouse *(~4 min)*

Um engine que quer consultar `pedidos_enriquecidos` precisa responder: **onde estão os metadados dessa tabela?** Quem responde é o **catálogo Iceberg**:

```
 ENGINE (Spark, Trino, DuckDB, Snowflake...)
    │ "quero a tabela X"
    ▼
 CATÁLOGO ICEBERG ── "os metadados dela estão AQUI; o snapshot atual é ESTE"
    │                (e, no caso do Tableflow/CMS: "tome credenciais temporárias")
    ▼
 metadados → manifests → arquivos Parquet (a anatomia da Aula 1)
```

- O catálogo é a camada de **descoberta e consistência**: nomes → metadados, e o apontamento atômico do snapshot atual (é ele que garante que dois engines leem a mesma versão)
- O **Tableflow traz um catálogo embutido**: o **Iceberg REST Catalog (IRC)** — um endpoint padrão REST que qualquer engine compatível fala. Toda tabela materializada nasce publicada nele
- **Autenticação com o de sempre:** API keys do Confluent Cloud (o modelo do Curso 2 chegando ao analítico); em CMS, o catálogo emite **credenciais vendidas** temporárias para os dados — o engine nunca vê o storage diretamente

### 2.2 Consumo direto: qualquer engine que fale Iceberg REST *(~4 min)*

A promessa "aberto de verdade" da Aula 1, cobrada:

A anatomia de um `SELECT` que chega de fora:

```
 ① engine AUTENTICA no IRC (API key do Confluent Cloud)
 ② pede a tabela → recebe os METADADOS (o snapshot atual — leitura consistente)
 ③ (em CMS) recebe VENDED CREDENTIALS temporárias para o storage
 ④ lê manifests → PODA arquivos pelas estatísticas (o pruning da Aula 1)
 ⑤ escaneia SÓ os Parquet necessários → resultado
    (o engine nunca "entra" no Confluent Cloud — fala Iceberg REST e object storage)
```

- **O caminho prático:** endpoint do IRC + API key → configurar o catálogo no engine → `SELECT` normal
**Engines de referência e seus caminhos:**

| Engine | Caminho | Nota |
|---|---|---|
| Spark / PySpark | IRC nativo | O do quickstart oficial — e da nossa demo |
| Trino / Starburst | IRC | O SQL federado do mercado |
| DuckDB | Extensão Iceberg + IRC | O caminho leve (laptop do analista) |
| Athena (PySpark) | IRC | Mundo AWS **sem** precisar do Glue |
| Snowflake / Databricks | Via catálogos externos | Requer BYOS — seção 2.3 |
| Flink do CC | — | **Não consulta** tabelas Iceberg do Tableflow (limitação atual honesta: o Flink processa o tópico, não a tabela) |
- O ponto arquitetural: **o dado não foi "para" nenhum engine** — a tabela é uma; os engines são visitantes. Trocar de ferramenta analítica deixa de ser projeto de migração
- É a demo desta aula: a tabela do bootcamp consultada por um engine **fora** do Confluent Cloud

### 2.3 Catálogos externos: entrando no mapa da empresa *(~5 min)*

Empresas já têm um catálogo-casa — e o Tableflow **sincroniza metadados** com eles (a tabela continua uma; o registro dela aparece onde os times já olham):

| Catálogo | O ecossistema que atende | Como a integração funciona |
|---|---|---|
| **AWS Glue Data Catalog** | Athena, EMR, Redshift Spectrum, SageMaker — o mundo AWS | Sync de metadados Iceberg; os engines AWS enxergam a tabela nativamente |
| **Snowflake Open Catalog** (Apache Polaris) | Snowflake e engines conectados ao Polaris | Sync Iceberg; o dado real-time aparece no catálogo governado do Snowflake |
| **Databricks Unity Catalog** | O mundo Databricks | Iceberg via sync; **tabelas Delta são publicadas diretamente via Unity** |

**O caminho Delta é diferente por natureza:** enquanto as tabelas **Iceberg** têm o IRC como catálogo-fonte e os externos como **sync**, as tabelas **Delta** do Tableflow são publicadas **diretamente via Unity Catalog** — é o único caminho de consumo delas. Ou seja: Iceberg = catálogo próprio + sincronizações opcionais; Delta = casado com o Unity desde o nascimento (a decisão de formato da Aula 2, seção 2.3, cobrando aqui).

Os três requisitos honestos (verificados na doc — e o tipo de detalhe que evolui):

1. **BYOS obrigatório** — só tabelas em **custom storage** sincronizam com catálogos externos; CMS não (a decisão de uma via da Aula 2 cobrando aqui)
2. **Read-only sempre** — o catálogo externo enxerga; quem escreve é o fluxo (a regra da Aula 3, agora na governança de fora)
3. **Sync de metadados é independente da materialização** — o catálogo externo pode falhar sem parar a tabela: os arquivos continuam sendo escritos e a tabela segue consultável pelo IRC

### 2.4 O desenho completo — e o gancho para a IA *(~2 min)*

```
 CDC (C3) ─► topics ─► FLINK (C4) ─► topics curados ─► TABLEFLOW (C5)
                                                          │
                              ┌────────────┬──────────────┼──────────────┐
                              ▼            ▼              ▼              ▼
                          IRC direto     Glue         Snowflake      Unity
                        (Spark/DuckDB)  (Athena...)   (Open Catalog) (Databricks)
```

E o medallion da Aula 1, agora com endereço no bootcamp:

| Camada | O que é | No bootcamp | Quem produz |
|---|---|---|---|
| **Bronze** | O dado cru, como chegou | Tópicos CDC materializados direto | Tableflow sobre a ingestão (C3) |
| **Silver** | Curado, enriquecido, confiável | `pedidos_enriquecidos` materializado | **Flink (shift-left, C4) → Tableflow** |
| **Gold** | Modelado para consumo (métricas, marts) | Agregações e modelos analíticos | O time analítico, nos engines desta aula |

O streaming virou cidadão do Lakehouse **sem um job de ETL escrito** — e com o dado fresco. A pergunta que o Curso 6 responde: *se o analítico pode ter dado de segundos atrás... por que a IA ainda trabalha com dado de ontem?*

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Hands-on: consumo via **IRC** com engine gratuito (o quickstart oficial usa **PySpark em Docker** — candidato natural; DuckDB é alternativa leve — decisão fina no projeto de demos). Catálogos externos: **conceitual/telas da doc** (exigem BYOS + contas externas — transparência com a turma). Tempo de tela: ~7 min. **Teardown fecha o curso.**

**O que a demo deve mostrar, nesta ordem:**

1. **As credenciais do consumo** — criar a API key adequada para o Tableflow/IRC (o modelo de escopo do Curso 2, de novo); o endpoint do REST Catalog na tela.
   - **Observação-alvo:** o analítico entra pela mesma porta de identidade do resto da plataforma — nada de credencial mágica.

2. **Configurar o engine externo** — o PySpark (container do quickstart) apontando o catálogo REST para o endpoint + key.
   - **Observação-alvo:** três propriedades de configuração — não um projeto de integração.

3. **O `SELECT` que fecha o bootcamp analítico** — `SHOW TABLES` no catálogo e um `SELECT` com agregação em `pedidos_enriquecidos` **de fora do Confluent Cloud**.
   - **Observação-alvo:** o momento-síntese do curso — o dado que nasceu num INSERT no Neon (C3), foi enriquecido pelo Flink (C4) e materializado pelo Tableflow (C5) sendo consultado por um engine independente. A promessa "aberto" cumprida na tela.

4. **A prova da frescura** — injetar eventos no fluxo e reconsultar: o resultado mudou.
   - **Observação-alvo:** analítica sobre dado de **segundos** atrás — o abismo da Aula 1, fechado.

5. **Catálogos externos (conceitual)** — as telas da doc: a configuração de catalog integration, o requisito BYOS explícito, e a tabela aparecendo no Glue/Snowflake/Unity.
   - **Observação-alvo:** o caminho corporativo existe e é sync de metadados — e a razão de não ser hands-on é orçamento (BYOS + contas externas), não complexidade. Honestidade didática (o padrão da Aula 5 do Curso 2).

6. **Teardown do curso** — desabilitar o Tableflow no tópico, revogar a key de demo, conferência de billing.

**Fechamento na tela** *(~1 min)*: "o pipeline agora atravessa do INSERT no banco legado à consulta analítica em segundos, com um contrato só de ponta a ponta. Próxima parada: colocar esse dado fresco onde ele vale mais — dentro da IA. Real-Time AI, Curso 6."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Catálogo é firula — eu sei onde estão meus arquivos."** O catálogo é o que dá **consistência** (o snapshot atômico) e **descoberta** para N engines — sem ele, volta o pântano da Aula 1.
- **"Integrar com Glue/Snowflake/Unity move o dado."** É **sync de metadados** — a tabela é uma, o registro aparece nos catálogos. Dado parado, governança distribuída.
- **"Escolhi CMS, depois ligo o Glue."** Não liga — catálogo externo exige BYOS, e storage é decisão de uma via (Aula 2). A pergunta do catálogo se faz **antes do enable**.
- **"O engine X é o dono da tabela agora."** Nenhum engine é dono — read-only para todos; o dono é o fluxo (tópico + subject). É o que mantém a arquitetura sã.
- **Detalhes evoluem rápido nesta área** (engines suportados, formatos, integrações) — narrar a estrutura, confirmar especificidades na doc no momento do uso.

**Ganchos com a trilha:**

- A decisão CMS × BYOS que governa tudo aqui → **Aula 2** (reforço deliberado).
- O dado fresco no Lakehouse alimentando RAG e agentes → **Curso 6** (na sequência).
- Governança, lineage e compliance do desenho completo → **Live 3**.
- No **Desafio Final**, a consulta analítica externa é a evidência da camada Lakehouse — esta demo é o gabarito.

**Perguntas para checar entendimento:**

1. O time de dados usa Athena e o de BI usa Snowflake. Desenhe o caminho: que decisão na habilitação do Tableflow, quais integrações, e quantas cópias do dado existem no final?
2. A integração com o catálogo externo falhou às 3h da manhã. O que aconteceu com a materialização, e por onde a tabela continua consultável?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Catálogo Iceberg** | A camada de descoberta e consistência: nomes → metadados → snapshot atual. |
| **Iceberg REST Catalog (IRC)** | O catálogo embutido do Tableflow — endpoint REST padrão que os engines consomem. |
| **Vended credentials** | Credenciais temporárias emitidas pelo IRC para acesso aos dados (obrigatórias em CMS). |
| **Catalog integration** | A sincronização de metadados do Tableflow com um catálogo externo (requer BYOS). |
| **AWS Glue Data Catalog** | O catálogo do ecossistema AWS (Athena, EMR, Redshift Spectrum). |
| **Snowflake Open Catalog (Polaris)** | O catálogo gerenciado do Snowflake baseado no Apache Polaris. |
| **Databricks Unity Catalog** | O catálogo do ecossistema Databricks (com publicação direta de tabelas Delta). |
| **Sync de metadados** | Publicar o registro da tabela em outro catálogo — sem mover dado. |
| **Delta via Unity** | O caminho de consumo das tabelas Delta do Tableflow: publicação direta no Unity Catalog (sem IRC). |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Integrate Catalogs with Tableflow (IRC, Glue, Snowflake, Unity; requisito BYOS)* — https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/catalog-integration/overview.html
- Confluent Cloud — *Tableflow Quick Start (consulta via PySpark/Athena no REST Catalog)* — https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-managed-storage.html
- Apache Iceberg — *REST Catalog Spec* — https://iceberg.apache.org/rest-catalog-spec/
- AWS — *Glue Data Catalog* — https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html
- Snowflake — *Open Catalog (Apache Polaris)* — https://other-docs.snowflake.com/en/opencatalog/overview
- Databricks — *Unity Catalog* — https://docs.databricks.com/en/data-governance/unity-catalog/index.html
