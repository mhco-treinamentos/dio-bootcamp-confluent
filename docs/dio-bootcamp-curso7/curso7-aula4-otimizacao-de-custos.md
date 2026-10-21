# Curso 7 · Aula 4 — Otimização de Custos: Retenção, Compactação, Tiered Storage, Freight e Sizing

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 1 do Curso 7 (e as configs do Curso 1, que voltam como dinheiro)

---

## 1. Objetivo da Aula

O terceiro pilar: **saber quanto custa e onde apertar**. Dissecar o **modelo de cobrança** do Confluent Cloud (o que é medido em cada recurso), as **alavancas de otimização** — retenção, compactação, Tiered Storage, o cluster type certo (incluindo o **Freight**) — e o básico de **sizing**: a conta que o arquiteto faz antes do contrato, não depois da fatura.

---

## 2. Conceitos Simplificados

### 2.1 O modelo de cobrança: o que o taxímetro mede *(~5 min)*

Cada recurso do bootcamp tem sua régua — e conhecer a régua É a otimização:

| Recurso | O que é cobrado | O reflexo no bootcamp |
|---|---|---|
| **Cluster elástico** (Basic/Standard/Enterprise) | **eCKU por uso**: throughput in/out, storage, partições, conexões | O Basic ocioso ≈ zero (por isso ele podia ficar) |
| **Cluster Dedicated** | **CKU provisionado por hora** — usando ou não | A capacidade reservada: paga-se o teto |
| **Storage** | GB-mês retido | Retention é config **e** fatura |
| **Conectores** (C3) | Por task/hora + throughput | O vilão nº 1 do teardown, explicado |
| **Flink** (C4) | **CFU-minuto** dos statements | O vilão nº 2 — statement perpétuo = custo perpétuo |
| **Tableflow** (C5) | Por GB processado/armazenado | A materialização tem taxímetro próprio |
| **RTCE / IA** (C6) | Serving + **as chamadas do SEU provedor de LLM** | A conta de token mora fora do CC — e é sua |
| **Rede** | Egress/transfer (e private networking, se houver) | O custo invisível dos consumidores externos |

- A leitura estrutural: **elástico cobra o que flui; provisionado cobra o que reserva** — a escolha do cluster type (Curso 2) é a decisão de custo nº 1
- E os "vilões do teardown" do bootcamp inteiro, agora com explicação econômica: conector, statement e Tableflow cobram **por existir/rodar**, não por fazer algo útil

### 2.2 As alavancas do storage: retenção e compactação *(~4 min)*

As configs do Curso 1, Aula 3, reapresentadas como dinheiro:

- **`retention.ms` é a alavanca mais barata do catálogo** — cada dia a mais de retenção é GB-mês em todos os tópicos derivados também (o bruto, o enriquecido, o classificado...). A pergunta certa por tópico: *"quanto de replay eu realmente preciso?"* — 7 dias é default, não destino
- **`cleanup.policy=compact` é a compressão semântica** — para tópicos de **estado** (o "último valor por chave": cadastros, saldos, o mundo upsert dos Cursos 4–6), compaction guarda a foto atual em vez do filme inteiro: o custo para de crescer com o tempo e passa a crescer com a **cardinalidade**
- A régua: **fatos → retention pensada; estado → compact** — e o híbrido `compact,delete` para estado com janela de história
- O erro clássico de custo: retention "infinita por segurança" em tópico de alto volume — segurança de verdade tem número e justificativa (auditoria exige 90 dias? 90 dias. "Vai que" não é requisito)

### 2.3 Tiered Storage e o Freight: a física do barato *(~4 min)*

**Tiered Storage** — a arquitetura que mudou a economia do Kafka:

```
 partição = camada QUENTE (disco local dos brokers — recente, rápido)
          + camada FRIA (object storage — o histórico, barato)
 o consumidor não vê a emenda: replay antigo simplesmente... funciona
```

- É o que torna **retenção longa economicamente viável** — no Confluent Cloud ela é parte do motor (o Kora, Curso 2, separando computação de armazenamento); no self-managed/Platform, é feature para configurar
- A consequência arquitetural: **"guardar mais" deixou de ser proibitivo** — a decisão de retenção volta a ser de negócio (replay, auditoria), não de pânico de disco

**Freight** — o cluster type do volume extremo:

- Para cargas **altíssimas e tolerantes a latência** (telemetria, logs, ingestão massiva): troca latência de ponta por **custo de rede/replicação drasticamente menor**
- A leitura de portfólio: Basic/Standard para o comum, Enterprise para requisitos de rede, Dedicated para o previsível-gigante, **Freight para o firehose barato** — o cluster type é ferramenta, não status

### 2.4 Sizing: a conta antes do contrato *(~4 min)*

O esqueleto da conta que dimensiona um cluster (a versão beginner — o ofício completo é profissão):

```
 ① VOLUME     — msgs/s × tamanho médio = MB/s in;  × fan-out de consumo = MB/s out
 ② PARTIÇÕES  — paralelismo alvo (C1!) × nº de tópicos = total de partições
 ③ CONEXÕES   — apps × instâncias × clients
 ④ RETENÇÃO   — MB/s in × segundos retidos = storage em regime
      → confrontar com os limites/preço do cluster type → a escolha
```

- **Medir antes de estimar**: o pipeline de bootcamp/POC rodando + a Metrics API (Aula 2) dão os números reais de ①–④ — sizing bom nasce de telemetria, não de chute
- **Picos dimensionam, médias enganam** — o cluster que atende a média morre na Black Friday; elástico absorve picos (pagando-os), provisionado exige tê-los no papel
- E a humildade da aula: sizing fino de Dedicated (CKUs, multi-região, contratos) é trabalho de arquiteto com planilha e doc — o bootcamp entrega **o vocabulário e o esqueleto da conta**

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: o billing e os recursos do próprio bootcamp — a fatura como material didático. Tempo de tela: ~6 min.

**O que a demo deve mostrar, nesta ordem:**

1. **A fatura aberta** — a tela de billing/costs do environment do bootcamp: o custo **por recurso** (cluster, conectores que existiram, Flink, Tableflow) ao longo das semanas do curso.
   - **Observação-alvo:** o taxímetro da seção 2.1 com os números do próprio aluno — e a assinatura visual dos vilões (os picos nos dias de conector/statement ligados).

2. **A alavanca da retenção** — revisitar `retention.ms` de um tópico de alto volume do bootcamp; simular a conta (MB/s × dias × GB-mês) em dois cenários (7 × 30 dias).
   - **Observação-alvo:** a config do Curso 1 virando linha de fatura — a conta feita na tela, não no susto.

3. **Compaction no tópico certo** — apontar um tópico de estado do pipeline (o upsert/cadastro) e a diferença conceitual de crescimento (filme × foto) com a config na tela.
   - **Observação-alvo:** a régua "fatos × estado" aplicada ao inventário real do bootcamp.

4. **A conta de sizing do capstone** — preencher o esqueleto ①–④ com os números do pipeline do bootcamp (via Metrics API/UI) e confrontar com a tabela de cluster types.
   - **Observação-alvo:** o aluno sai sabendo fazer **a conta do próprio projeto** — e é exatamente o que o README do Desafio Final pede (throughput medido, decisões justificadas).

**Fechamento na tela** *(~1 min)*: "enxergamos, trancamos, e agora pagamos o justo. Falta o pior dia: região caiu, e agora? Multi-region, DR e a consolidação final das garantias — a última aula do bootcamp gravado."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Gerenciado é caro."** Compare TCO, não fatura: a fatura do CC é visível; o custo do self-managed (gente, plantão, infra ociosa) é invisível e maior no caso comum. O comparativo honesto é o do Curso 2, Aula 1.
- **"Otimizar custo = espremer tudo."** Otimizar é **pagar pelo que tem valor**: retention de 90 dias para auditoria é custo certo; retention infinita "por vai que" é desperdício certo. A justificativa é a otimização.
- **"Compaction economiza sempre."** Só em tópicos de **estado** — em tópicos de fatos ela destrói a história (e o replay que os Cursos 5–6 usam). Régua antes de alavanca.
- **"Sizing é chute educado."** É **telemetria + conta** — o pipeline medido (Aula 2) responde ①–④. Chute é o que se faz sem a Metrics API.
- **Preços e limites mudam** — a aula ensina o **modelo** (o que é medido, as alavancas); valores por eCKU/CKU/GB se confirmam na página de pricing do dia.

**Ganchos com a trilha:**

- retention/compaction → **Curso 1, Aula 3** (as configs que viraram fatura).
- Cluster types e a decisão elástico × provisionado → **Curso 2, Aulas 1–2**.
- Os vilões do teardown → **Cursos 3–6** (cada um com sua régua agora explicada).
- A Metrics API alimentando o sizing → **Aula 2** deste curso.
- No **Desafio Final**, a seção de trade-offs do README é onde essa aula aparece por escrito.

**Perguntas para checar entendimento:**

1. Um tópico recebe 5 MB/s e tem 4 consumidores. Estime o storage em regime com retenção de 7 × 30 dias, e o throughput out — qual das duas dimensões o fan-out afeta?
2. Telemetria de IoT: 800 MB/s, tolera segundos de latência, replay de 3 dias. Que cluster type você investiga primeiro, e quais duas alavancas desta aula entram no desenho?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **eCKU / CKU** | As unidades de capacidade: elástica (cobrada por uso) × provisionada (reservada por hora). |
| **GB-mês** | A unidade de cobrança de storage — onde a retenção vira dinheiro. |
| **CFU-minuto** | A unidade de cobrança do Flink — o relógio dos statements. |
| **Fan-out de consumo** | Quantas vezes cada byte produzido é lido — multiplica o throughput out. |
| **Tiered Storage** | Camada quente (disco) + fria (object storage) — a física da retenção longa barata. |
| **Freight (cluster type)** | O tipo para volume extremo tolerante a latência — custo de rede/replicação mínimo. |
| **Sizing** | A conta de volume, partições, conexões e retenção que dimensiona o cluster. |
| **Regime (storage em regime)** | O ponto em que o storage estabiliza: entrada × retenção. |
| **TCO** | O custo total (infra + operação + gente) — a base do comparativo honesto. |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Billing (o modelo de cobrança por recurso)* — https://docs.confluent.io/cloud/current/billing/overview.html
- Confluent — *Pricing (os valores do dia)* — https://www.confluent.io/confluent-cloud/pricing/
- Confluent Cloud — *Cluster Types (limites e réguas, incl. Freight)* — https://docs.confluent.io/cloud/current/clusters/cluster-types.html
- Confluent — *Infinite Storage / Tiered Storage (a arquitetura)* — https://www.confluent.io/blog/infinite-kafka-storage-in-confluent-platform/
- Apache Kafka — *Tiered Storage (KIP-405, o mecanismo no OSS)* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
