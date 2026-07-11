# Curso 2 · Aula 3 — Schema Registry e Data Contracts: contratos de dados com Avro, Protobuf e JSON Schema

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisito:** Aula 2 (cluster, service account e API key criados)

---

## 1. Objetivo da Aula

Resolver o problema que o Curso 1 deixou plantado — "para o broker, key e value são só bytes; o formato é contrato dos clientes" — dissecando o **Schema Registry**: a anatomia de um **subject**, de um **schema Avro** e do **wire format** que viaja em cada mensagem, e a disciplina que permite **evoluir eventos sem quebrar consumers**: a compatibilidade **BACKWARD**. E dar o passo além da estrutura: o **Data Contract completo** — schema + metadados + **Data Rules** validando os valores.

---

## 2. Conceitos Simplificados

### 2.1 O problema: o contrato invisível *(~3 min)*

Producer serializa, consumer deserializa — e o broker não abre a carta (Aula 1 do Curso 1). Então **quem garante que as duas pontas falam a mesma língua?**

Sem governança, o contrato é implícito: vive no código, na cabeça do time, num README desatualizado. Aí o producer adiciona um campo, renomeia outro... e o consumer **quebra em produção, silenciosamente, dias depois**.

O **Schema Registry** torna o contrato **explícito, versionado e verificado**:

```
producer ──(1) registra/consulta schema──►  SCHEMA REGISTRY
   │                                          │ subjects, versões,
   │ (2) serializa validando                  │ regras de compatibilidade
   ▼                                          ▲
 topic ──(3) bytes + referência ao schema──► consumer (4) consulta e deserializa
```

Três formatos suportados: **Avro** (o mais usado no ecossistema Kafka — o nosso padrão no bootcamp), **Protobuf** e **JSON Schema**.

### 2.2 Anatomia de um schema Avro *(~3 min)*

```json
{
  "type": "record",
  "name": "PedidoCriado",
  "namespace": "com.bootcamp.pedidos",
  "fields": [
    { "name": "order_id",    "type": "string" },
    { "name": "customer_id", "type": "string" },
    { "name": "total",       "type": "double" },
    { "name": "canal",       "type": ["null", "string"], "default": null }
  ]
}
```

- **record + fields tipados** — o evento da Aula 1 do Curso 1, agora com contrato formal
- **union com null + default** — a anatomia de um **campo opcional**: é o que torna a evolução segura (guarde o `canal` — ele volta na demo)
- Avro é binário e compacto — o JSON acima é a **definição**; o dado trafega serializado

### 2.3 Anatomia do subject: como schemas se organizam *(~3 min)*

O Registry organiza schemas em **subjects** — a linha do tempo de versões de um contrato:

```
subject: "pedidos-value"
   ├─ version 1  →  schema id 100  (PedidoCriado sem canal)
   └─ version 2  →  schema id 105  (PedidoCriado com canal opcional)
```

- **Estratégia de nomeação default (TopicNameStrategy):** `{topic}-value` e `{topic}-key` — um contrato por tópico, o caso comum
- Existem estratégias por **tipo de record** (RecordNameStrategy) para tópicos com múltiplos tipos de evento — citar que existe; aprofundar é intermediate
- **Version** é a posição na linha do tempo do subject; **schema ID** é o identificador global — é ele que viaja na mensagem

### 2.4 Anatomia do wire format: o que viaja de verdade *(~3 min)*

Quando o producer serializa com o Schema Registry, cada mensagem carrega uma **referência** ao schema:

```
byte 0        bytes 1–4         bytes 5+
┌──────────┬────────────────┬─────────────────────────┐
│ magic 0  │  schema ID     │  payload Avro (binário) │
└──────────┴────────────────┴─────────────────────────┘
   "isto      "o contrato       o dado, compacto
    usa SR"    é o nº 105"
```

- O consumer lê o **schema ID**, busca o schema no Registry (com cache) e deserializa com segurança
- **A mensagem não carrega o schema inteiro** — só 5 bytes de overhead: contrato garantido quase de graça
- É também por isso que consumir mensagem Avro "na unha" (sem o deserializer do SR) mostra lixo binário — os 5 bytes iniciais não são Avro puro (armadilha clássica que a demo expõe)

### 2.5 Evolução e compatibilidade: mudar sem quebrar *(~4 min)*

O Registry não é só um catálogo — é um **portão**: toda nova versão passa pelo teste de compatibilidade do subject.

| Modo | Garante que... | Mudanças permitidas (típico) |
|---|---|---|
| **BACKWARD** *(default)* | Consumer **novo** lê dados **antigos** | adicionar campo opcional (com default); remover campo |
| **FORWARD** | Consumer **antigo** lê dados **novos** | adicionar campo; remover campo opcional |
| **FULL** | Os dois sentidos | só mudanças opcionais com default |
| **NONE** | Nada — qualquer mudança passa | (o portão aberto — evitar) |
| Variantes **_TRANSITIVE** | O mesmo, contra **todas** as versões, não só a última | mais rígido e mais seguro |

Por que **BACKWARD** é o default e o nosso padrão:

- No mundo Kafka, **o log guarda dados antigos** (retention, replay — Curso 1): o consumidor atualizado **vai** encontrar mensagens velhas. BACKWARD garante que ele as lê
- Fluxo de deploy resultante: **atualiza consumers primeiro, depois producers**
- Receita prática do dia a dia: **campo novo sempre opcional com default** — passa em BACKWARD e não quebra ninguém

### 2.6 Data Contracts completos: schema + metadados + regras *(~3 min)*

O schema garante a **estrutura** — mas repare: `total` do tipo `double` aceita `-50.0` sem reclamar. Estruturalmente válido, semanticamente absurdo. O **Data Contract completo**, dentro do Stream Quality (a terceira frente do Stream Governance — Aula 1), adiciona camadas sobre o schema:

```
┌──────────────── DATA CONTRACT ────────────────┐
│ schema         → a ESTRUTURA (seções 2.2–2.5) │
│ metadata/tags  → o SIGNIFICADO: dono, domínio,│
│                  marcação PII (Stream Catalog)│
│ ruleSet        → o COMPORTAMENTO:             │
│   ├─ Data Quality Rules → validam VALORES     │
│   ├─ Migration Rules    → transformam entre   │
│   │                       versões incompatíveis│
│   └─ ações de violação  → rejeitar ou desviar │
│                           para DLQ            │
└────────────────────────────────────────────────┘
```

- **Data Quality Rules** — expressões **CEL** (Common Expression Language) avaliadas na serialização/deserialização: `message.total > 0`, formato de e-mail, valor dentro de um enum. O portão de compatibilidade valida a **forma**; as rules validam o **conteúdo**
- **Ações de violação** — o que fazer com o evento que viola: rejeitar na origem (erro no producer) ou **desviar para um Dead Letter Topic** (DLQ action) — o padrão de resiliência do Curso 3 aparecendo já na camada de contrato
- **Migration Rules** — quando a mudança é inevitavelmente incompatível (major version), regras de transformação (JSONata) convertem os dados entre versões em tempo de leitura/escrita — consumers antigos e novos coexistem mesmo com breaking change declarado
- Síntese da aula em uma frase: **schema diz "é um pedido"; o data contract diz "é um pedido válido, de quem, e o que fazer quando não for"**

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — CLI + UI do Confluent Cloud. Pré-requisitos: cluster e credenciais da Aula 2; API key de Schema Registry criada no início da demo (reforço da anatomia de escopo). Tempo de tela: ~7 min.

**O que a demo deve mostrar, nesta ordem:**

1. **Registrar o schema v1** — tópico `pedidos` recriado; schema Avro `PedidoCriado` (SEM o campo `canal`) registrado no subject `pedidos-value`; exibir subject, versão 1 e schema ID na UI.
   - **Observação-alvo:** o contrato existe como recurso de plataforma — visível, versionado, fora do código.

2. **Produzir COM o serializer do Registry e consumir** — 2–3 eventos validados pelo schema.
   - **Observação-alvo:** produção validada passa; em seguida, **tentar produzir um evento violando o schema** (campo faltando/tipo errado) e ver a rejeição **na origem** — o erro morre no producer, não no consumer em produção.

3. **Expor o wire format** — consumir a mensagem SEM o deserializer do SR (console consumer cru).
   - **Observação-alvo:** o "lixo binário" com os bytes iniciais — o magic byte + schema ID da seção 2.4, visível. Reconsumir com o deserializer correto: dado limpo. A armadilha clássica, desarmada em aula.

4. **Evoluir com sucesso (v2)** — registrar a v2 adicionando `canal` como opcional com default (o schema da seção 2.2).
   - **Observação-alvo:** o portão BACKWARD **aceita**; o subject agora tem 2 versões; consumer lendo dados antigos e novos sem quebrar.

5. **Evoluir quebrando (rejeição)** — tentar registrar uma v3 que remove `order_id` obrigatório ou muda tipo de campo incompativelmente.
   - **Observação-alvo:** o Registry **rejeita na cara do aluno**, com a mensagem de incompatibilidade — o portão funcionando. É o momento-síntese da aula.

6. **(Cena bônus — se o relógio permitir) Uma Data Quality Rule em ação** — anexar ao subject uma regra CEL simples (`message.total > 0`) e produzir um evento estruturalmente válido com `total = -10`.
   - **Observação-alvo:** o schema aceitaria (a estrutura está correta), mas o **valor** viola o contrato — a rejeição agora é semântica. Dois portões: forma (compatibilidade) e conteúdo (rules).

**Fechamento na tela** *(~1 min)*: "contrato explícito, evolução com portão, 5 bytes de overhead. Na próxima aula, juntamos tudo — cluster, identidade, credencial e contrato — no fluxo ponta a ponta da primeira mensagem 'de gente grande'."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Schema trava a evolução."** É o contrário: compatibilidade é o que **permite** evoluir com regras — sem ela, cada mudança é uma roleta russa em produção. A dupla cena 4/5 da demo (passa/rejeita) é o argumento.
- **"O schema viaja em cada mensagem."** Só o **ID** (5 bytes). Confundir isso leva a superestimar o overhead do Avro.
- **"BACKWARD é uma escolha entre várias equivalentes."** Não no mundo Kafka: retention + replay tornam 'consumer novo lê dado antigo' o cenário inevitável. Amarrar explicitamente ao Curso 1.
- **"JSON sem schema é mais simples."** Até o primeiro campo renomeado em produção. JSON Schema existe no Registry justamente para quem precisa de JSON **com** contrato.
- **"Schema válido = dado bom."** Estrutura correta não impede valor absurdo (`total = -50.0`). É a fresta que as Data Quality Rules fecham — forma e conteúdo são portões diferentes.
- **Disciplina de relógio nas rules:** em vídeo, as Data Rules entram como conceito + UM exemplo (a cena bônus, se couber) — CEL/JSONata a fundo é referência nesta documentação de apoio, o hands-on de DLQ é o **Curso 3, Aula 4**, e governança em escala (catálogo, lineage, compliance) é a **Live 3**.

**Ganchos com a trilha:**

- A primeira mensagem ponta a ponta com schema → **Aula 4** (na sequência imediata).
- Schemas em conectores e CDC (o contrato vindo do banco) → **Curso 3**.
- Schematização e evolução no caminho do Lakehouse → **Curso 5, Aula 3** (Tableflow depende disso).
- Governança, catálogo e lineage em escala → **Live 3**.

**Perguntas para checar entendimento:**

1. Seu time quer adicionar o campo `cupom` ao evento `PedidoCriado` sem coordenar deploy com os 4 times consumidores. Como o campo deve ser declarado, e por quê isso passa em BACKWARD?
2. Um consumer sem o deserializer do SR lê o tópico e vê bytes estranhos no início de cada mensagem. O que são, e o que eles permitem?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Schema Registry** | Serviço que armazena, versiona e valida schemas — o guardião do contrato de dados. |
| **Schema** | Definição formal da estrutura de um evento (campos, tipos, defaults). |
| **Avro** | Formato de serialização binário com schema — o padrão do ecossistema Kafka e do bootcamp. |
| **Protobuf / JSON Schema** | Formatos alternativos suportados pelo Registry. |
| **Subject** | Linha do tempo de versões de um contrato (default: `{topic}-value`). |
| **TopicNameStrategy** | Estratégia default de nomeação de subjects, um contrato por tópico. |
| **Schema ID** | Identificador global do schema; é o que viaja na mensagem. |
| **Wire format** | Layout dos bytes: magic byte + schema ID + payload serializado. |
| **Compatibilidade BACKWARD** | Garante que consumer novo lê dados antigos — o default e o padrão do bootcamp. |
| **Data Contract** | Contrato completo: schema (estrutura) + metadados/tags (significado) + ruleSet (comportamento). |
| **ruleSet** | Conjunto de regras anexado ao schema no Registry (quality, migration e ações). |
| **Data Quality Rule** | Regra que valida os valores dos dados na serialização/deserialização (expressões CEL). |
| **CEL (Common Expression Language)** | Linguagem de expressões usada nas Data Quality Rules (ex.: `message.total > 0`). |
| **Migration Rule** | Regra de transformação (JSONata) que converte dados entre versões major incompatíveis. |
| **DLQ action** | Ação de violação que desvia o evento inválido para um Dead Letter Topic em vez de quebrar o fluxo. |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 3 (serialização com Avro e Schema Registry). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 4 ("Encoding and Evolution"): o fundamento teórico de compatibilidade de schemas

**Documentação e artigos oficiais:**

- Confluent — *Schema Registry Documentation* — https://docs.confluent.io/platform/current/schema-registry/index.html
- Confluent — *Schema Evolution and Compatibility* — https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Confluent — *Wire Format* — https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
- Apache Avro — *Specification* — https://avro.apache.org/docs/current/specification/
- Confluent Cloud — *Manage Schemas* — https://docs.confluent.io/cloud/current/sr/schemas-manage.html
- Confluent Cloud — *Data Contracts (rules, metadata, ruleSet)* — https://docs.confluent.io/cloud/current/sr/fundamentals/data-contracts.html
- CEL — *Common Expression Language (especificação)* — https://github.com/google/cel-spec
- Confluent — *Stream Governance: Stream Quality* — https://docs.confluent.io/cloud/current/stream-governance/index.html
