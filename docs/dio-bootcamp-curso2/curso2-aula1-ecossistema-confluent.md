# Curso 2 · Aula 1 — O Ecossistema Confluent: Kafka OSS, Confluent Platform e Confluent Cloud

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Aula 100% conceitual (posicionamento de plataforma)

---

## 1. Objetivo da Aula

Posicionar o portfólio — **Apache Kafka OSS**, **Confluent Platform** e **Confluent Cloud** — dissecando a **fronteira de gestão** (o que VOCÊ opera em cada modelo), entender o que o gerenciado resolve que o OSS não resolve, situar **KRaft** (o fim do ZooKeeper) e o **Kora Engine**, e comparar com a alternativa mais comum do mercado: o **AWS MSK**.

---

## 2. Conceitos Simplificados

### 2.1 A fronteira de gestão: a pergunta certa *(~5 min)*

A escolha entre OSS, Platform e Cloud não é sobre "qual é melhor" — é sobre **onde passa a linha do que é problema seu**:

```
                     │ Kafka OSS      │ Confluent Platform │ Confluent Cloud
─────────────────────┼────────────────┼────────────────────┼─────────────────
 aplicação/clientes  │ você           │ você               │ você
 schemas/governança  │ você (monta)   │ você (ferramentas) │ plataforma
 conectores          │ você (opera)   │ você (ferramentas) │ plataforma
 brokers/upgrade     │ você           │ você (ferramentas) │ plataforma
 SO/disco/rede       │ você           │ você               │ plataforma
 capacidade/sizing   │ você           │ você               │ elástico
─────────────────────┴────────────────┴────────────────────┴─────────────────
        self-managed ◄────────────────────────────────► fully managed
```

- **Kafka OSS** — gratuito na licença, caro na operação: você monta e opera tudo (o sandbox Docker do Curso 1 é OSS — e você sentiu que até "subir um broker" é uma decisão)
- **Confluent Platform** — o self-managed profissional: o Kafka + ferramentas enterprise (Schema Registry, Connect, Control Center, segurança, suporte) para rodar **na sua infraestrutura** — o caminho de quem tem requisito de on-premises/regulatório
- **Confluent Cloud** — a fronteira inteira do lado da plataforma: você cria recursos lógicos (tópicos, schemas, conectores) e consome; broker, disco, upgrade e capacidade deixam de existir como problema

A pergunta de arquiteto: **"operar Kafka é diferencial competitivo do meu negócio?"** Se não é — e para quase todo mundo não é — a fronteira deve ficar o mais alta possível.

### 2.2 KRaft: o fim do ZooKeeper *(~3 min)*

Historicamente o Kafka dependia de um segundo sistema distribuído — o **ZooKeeper** — para metadados e eleição de controller. O **KRaft** (Kafka Raft, KIP-500) internalizou isso:

- **Metadados viram um log interno do próprio Kafka** — coerente: o sistema do log passou a guardar seus metadados... num log
- **Um sistema a menos para operar**, clusters maiores, failover mais rápido
- Linha do tempo: KRaft nasce experimental, amadurece, e no **Kafka 4.x o ZooKeeper foi removido de vez** — cluster novo hoje é KRaft, sem discussão
- O sandbox do Curso 1 já rodava KRaft — o aluno nunca viu ZooKeeper e é exatamente essa a mensagem

### 2.3 Kora: o Kafka reescrito para nuvem *(~3 min)*

O Confluent Cloud **não roda o Kafka OSS empacotado** — roda o **Kora**, o engine da Confluent nativo de nuvem, que fala o **protocolo Kafka** (seus clientes não sabem a diferença):

- **Separação computação × armazenamento** — dados em object storage; brokers viram camada de serviço elástica
- **Multi-tenancy real** — a razão de existir cluster Basic barato: milhares de tenants isolados sobre infraestrutura compartilhada
- **Elasticidade sem rebalanceamento manual** — expandir/encolher deixa de ser projeto de fim de semana
- A lição de arquitetura: **protocolo é contrato; implementação é liberdade** — o mesmo princípio do serializer no Curso 1

### 2.4 O comparativo triplo: AWS MSK × Confluent Platform × Confluent Cloud *(~4 min)*

A decisão real do mercado raramente é binária — o triângulo completo, eixo a eixo:

| Eixo | **AWS MSK** | **Confluent Platform** | **Confluent Cloud** |
|---|---|---|---|
| O que é | Kafka OSS **provisionado** pela AWS | Distribuição enterprise **self-managed** (sua infra) | Plataforma completa **fully managed** (Kora) |
| Fronteira de gestão | AWS opera a infra; **você opera o Kafka** (sizing, tuning, upgrades) | **Você opera tudo**, com ferramentas enterprise e suporte | Plataforma opera infra **e** Kafka |
| Schema Registry | AWS Glue Schema Registry (serviço à parte) | Confluent SR self-managed | Confluent SR embutido + Data Rules (Aula 3) |
| Stream processing | Amazon Managed Flink (serviço separado, billing próprio) | Kafka Streams / Flink self-managed | **Flink serverless unificado** (lê topics e SR nativamente) |
| Connect | MSK Connect (você traz e configura o connector) | Connect self-managed | 90+ conectores **fully managed** |
| Governança / lineage | IAM + Glue (sem suíte de lineage equivalente) | Stream Governance self-managed | **Stream Governance** managed (seção 2.5) |
| Replicação / DR | MSK Replicator (base MM2) | Cluster Linking / Replicator | **Cluster Linking** byte a byte, offsets preservados (Aula 5) |
| Lakehouse | Montar com Flink + S3 + Glue | Roadmap | **Tableflow** nativo (Curso 5) |
| Multi-cloud / on-prem | Só AWS | On-premises / qualquer infra | AWS + Azure + GCP (+ híbrido via Cluster Linking) |
| SLA típico | 99,9% | Seu problema (você é o SLA) | 99,99% multi-zone |
| Onde brilha | 100% AWS, time forte de plataforma, caso Kafka "puro" | Data residency estrita, air-gap, customização profunda de broker | Time quer streaming, não operação; ecossistema completo |

E os **sinais de decisão** — a tabela que o arquiteto realmente usa:

| Sinal do cenário | Aponta para |
|---|---|
| Greenfield, time enxuto, foco em produto | **Confluent Cloud** |
| Multi-cloud ou cloud + on-premises | **CC + Cluster Linking** (híbrido com CP na ponta on-prem) |
| Data residency estrita / air-gap / soberania total | **Confluent Platform** |
| 100% AWS, sensível a custo de infra, aceita operar mais | **MSK** (os brokers Express reduziram bastante o gap operacional do Provisioned clássico) |
| Regulado (BACEN/LGPD) sem exigência de air-gap | **CC Enterprise/Dedicated** (private networking, BYOK, audit logs) |
| Precisa de lineage, Tableflow ou Flink unificado | **Confluent Cloud** |

Enquadramentos honestos para narrar: **MSK não é espantalho** — para pipeline de transporte AWS-native com time maduro, pode ser a resposta certa; e **CP não é "CC para quem não confia em cloud"** — é a escolha quando controle de perímetro ou customização são **requisito**, não preferência. Os dois anti-padrões simétricos: forçar Cloud onde há air-gap real, e forçar Platform "porque cloud é arriscado".

### 2.5 Stream Governance: o que o gerenciado entrega além do broker *(~3 min)*

A resposta mais concreta ao equívoco "Cloud é só Kafka hospedado" tem nome: **Stream Governance** — a suíte de governança embutida na plataforma, provisionada **por environment**, em três frentes:

- **Stream Catalog** — descoberta: busca de tópicos, schemas e campos; **tags** (ex.: `PII`, `sensitive`) e metadados de negócio. Responde "que dados existem e o que significam?"
- **Stream Lineage** — o mapa vivo do fluxo: quem produz, por onde o dado passa (Connect, Flink), quem consome — desenhado **automaticamente a partir do tráfego real**, não de documentação manual
- **Stream Quality** — o contrato garantido: o Schema Registry (Aula 3) e as **Data Rules** — regras que validam os **valores** dos dados, não só a estrutura (dissecadas na Aula 3)

Dois pacotes por environment: **Essentials** (incluído — Schema Registry + catálogo básico) e **Advanced** (pago — lineage estendido, business metadata, catálogo completo). O ponto de arquitetura: montar isso sobre OSS significa integrar catálogo, rastreio e validação por conta própria — uma dimensão inteira da fronteira de gestão que costuma ficar **invisível** na comparação de plataformas.

### 2.6 O mapa do bootcamp sobre o portfólio *(~2 min)*

- Curso 1 = fundamentos **no OSS** (o modelo mental é agnóstico — proposital)
- Cursos 2–7 = **Confluent Cloud** (a fronteira alta libera o tempo do bootcamp para o que importa: schemas, integração, Flink, Lakehouse, IA, produção)
- Confluent Platform aparece como contexto (clientes regulados, on-premises) e na conversa de migração (Aula 5 deste curso)

**Fechamento** *(~1 min)*: "portfólio posicionado, fronteira entendida. Na próxima aula, entramos de fato na plataforma: a hierarquia de recursos do Confluent Cloud — e criamos tudo por CLI."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Confluent Cloud é o Kafka OSS hospedado."** É o Kora falando protocolo Kafka — com Stream Governance (catálogo, lineage, quality), elasticidade, Flink e Tableflow embutidos. A distinção produto × protocolo é a alma da aula, e a seção 2.5 é o argumento mais tangível.
- **"OSS é grátis."** A licença é; a operação não — gente, plantão, upgrades, capacity planning. O TCO honesto compara **operação**, não licença.
- **"MSK e Confluent Cloud são equivalentes."** Os dois rodam Kafka de verdade (protocolo e clients compatíveis) — a diferença é **escopo de plataforma**: no MSK, tudo ao redor (Flink, schema, governança) vem de serviços AWS separados; no Confluent, vem unificado. E o comparativo justo usa o MSK atual (Express/Serverless), não o Provisioned clássico como espantalho.
- **Números envelhecem:** SLAs, limites e features das três opções evoluem — o vídeo deve narrar a **estrutura** da comparação e apontar que valores exatos se confirmam na documentação oficial dos dois fornecedores (nunca comparar MSK só por material da Confluent, e vice-versa).
- **"ZooKeeper ainda é necessário."** Kafka 4.x removeu; conteúdo/tutorial que começa com ZooKeeper está datado — bom critério para o aluno filtrar material na internet.
- **Neutralidade didática:** a aula posiciona; não evangeliza. O critério é sempre a pergunta da fronteira ("operar Kafka é seu diferencial?"), não marca.

**Ganchos com a trilha:**

- A hierarquia de recursos e os cluster types → **Aula 2** (na prática).
- Schema Registry integrado × montado à mão → **Aula 3**.
- Migração OSS/on-prem → Cloud sem parar a operação → **Aula 5** (Cluster Linking).
- Elasticidade, sizing e custos do gerenciado → **Curso 7, Aula 4**.

**Perguntas para checar entendimento:**

1. Sua empresa roda Kafka OSS com 2 SREs dedicados e sofre com upgrades. Que perguntas você faria antes de escolher entre Platform, Cloud e MSK?
2. Por que os clientes (producers/consumers) do Curso 1 funcionariam no Confluent Cloud sem mudança de código?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **Apache Kafka OSS** | A distribuição open source do Kafka — você opera tudo. |
| **Confluent Platform** | Distribuição enterprise self-managed: Kafka + ferramentas + suporte, na sua infraestrutura. |
| **Confluent Cloud** | Serviço totalmente gerenciado de data streaming, baseado no engine Kora. |
| **Fronteira de gestão** | A linha que separa o que você opera do que a plataforma opera. |
| **ZooKeeper** | Sistema externo de coordenação exigido pelo Kafka até a era KRaft; removido no Kafka 4.x. |
| **KRaft (KIP-500)** | Modo em que o próprio Kafka gerencia seus metadados via protocolo Raft. |
| **Kora** | Engine cloud-native do Confluent Cloud, compatível com o protocolo Kafka. |
| **AWS MSK** | Managed Streaming for Apache Kafka — o Kafka OSS provisionado pela AWS. |
| **Multi-tenancy** | Múltiplos clientes isolados sobre a mesma infraestrutura física. |
| **Stream Governance** | Suíte de governança do Confluent Cloud, por environment: Catalog, Lineage e Quality (pacotes Essentials/Advanced). |
| **Stream Catalog** | Descoberta de dados: busca, tags (ex.: PII) e metadados de negócio sobre tópicos e schemas. |
| **Stream Lineage** | Mapa automático do fluxo de dados (produtores → tópicos → processamento → consumidores), gerado do tráfego real. |
| **Stream Quality / Data Rules** | Camada de qualidade: Schema Registry + regras que validam os valores dos dados (Aula 3). |
| **TCO (Total Cost of Ownership)** | Custo total de propriedade: licença + infraestrutura + operação + pessoas. |

---

## 5. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 1 posiciona o ecossistema. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/

**Documentação e artigos oficiais:**

- KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum — https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum
- Apache Kafka — *KRaft* — https://kafka.apache.org/documentation/#kraft
- Confluent — *Kora: A Cloud-Native Event Streaming Platform for Kafka* (paper, VLDB 2023 — Best Industry Paper) — https://www.confluent.io/blog/cloud-native-data-streaming-kafka-engine/
- Confluent — *Confluent Cloud vs. Confluent Platform* — https://docs.confluent.io/cloud/current/overview.html
- Confluent Cloud — *Stream Governance Overview* (Catalog, Lineage, Quality e os pacotes Essentials/Advanced) — https://docs.confluent.io/cloud/current/stream-governance/index.html
- Confluent — *Confluent vs. Amazon MSK* (material comparativo do fornecedor — ler com esse filtro) — https://www.confluent.io/confluent-cloud-vs-amazon-msk/
- AWS — *Amazon MSK Features* (cluster types, Express brokers, integrações — a verdade sobre o MSK na fonte AWS) — https://aws.amazon.com/msk/features/
- AWS — *Amazon MSK Documentation* — https://docs.aws.amazon.com/msk/
- AWS — *MSK FAQs* (Replicator, Serverless) — https://aws.amazon.com/msk/faqs/
