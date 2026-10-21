# Curso 2 · Aula 1 — O ecossistema Confluent: OSS, Platform e Cloud

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~20 min · 📄 Exercício de decisão — **sem terminal, custo zero**

Esta aula não cria nada e não gasta crédito. Ela responde a pergunta que todo arquiteto enfrenta antes de escrever a primeira linha: **quem opera o Kafka?**

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter assistido à aula | ~20 min |
| Ter feito o [Curso 1](../../curso1/README.md) | Você operou um Kafka na mão — essa experiência é o contraste |
| Papel e caneta | Vai recomendar arquiteturas |

> 💡 Fez a [Aula 0](../aula0/README.md)? Ótimo, mas não é obrigatório para esta aula.
> Nada aqui toca a nuvem.

---

## O que você vai saber fazer ao final

- Explicar o que é "fronteira de gestão" e por que ela é a pergunta central
- Escolher entre MSK, Confluent Platform e Confluent Cloud para um cenário concreto
- Justificar a escolha por critérios, não por preferência
- Reconhecer o que o gerenciado entrega **além** do broker

---

## Os conceitos em 90 segundos

**A fronteira de gestão** — onde termina a sua responsabilidade e começa a do fornecedor:

```
Kafka OSS          │████████████████████████│  tudo seu
AWS MSK            │████████░░░░░░░░░░░░░░░░│  AWS opera a infra, você opera o Kafka
Confluent Platform │████████████████████░░░░│  seu, com ferramentas enterprise
Confluent Cloud    │░░░░░░░░░░░░░░░░░░░░░░░░│  infra E Kafka são da plataforma
                    ▲ você                 ▲ eles
```

O sandbox do Curso 1 era **OSS**: você escolheu a imagem, configurou listeners, decidiu partições, subiu e derrubou. Funcionou porque era um broker de estudo. Multiplique por 3 brokers replicados, com upgrade sem downtime e SLA de 99,99% — é aí que a fronteira importa.

> 📖 KRaft (o fim do ZooKeeper), Kora (o Kafka reescrito para nuvem) e a suíte de
> Stream Governance estão **na aula gravada**.

---

## Passo 1 — Recomende a arquitetura *(~10 min)*

Para cada empresa, recomende **MSK**, **Confluent Platform** ou **Confluent Cloud** — e justifique em uma frase.

| # | Cenário |
|---|---|
| 1 | Fintech greenfield, 6 engenheiros, quer lançar em 3 meses. Precisa de streaming, CDC do Postgres e um pipeline analítico. |
| 2 | Banco com exigência regulatória de **data residency** total: o dado não pode sair do datacenter próprio. Time de plataforma com 15 pessoas. |
| 3 | Empresa 100% AWS, time de infra forte, uso "Kafka puro" — só produz e consome, sem stream processing nem conectores. |
| 4 | Varejista com ERP **on-premises** que precisa alimentar analytics na nuvem, sem parar a operação. |
| 5 | Startup que já roda Confluent Cloud e quer processar os eventos em tempo real para detectar fraude. |

**Entregável**: 5 recomendações com justificativa.

---

## Passo 2 — Onde o gerenciado ganha *(~5 min)*

No Curso 1 você fez tudo na mão. Para cada tarefa, responda: **quanto tempo levaria** e **quem faria** numa empresa com Kafka self-managed?

| Tarefa | Self-managed | Confluent Cloud |
|---|---|---|
| Criar um cluster com RF=3 em 3 zonas | ? | ~1 min, um formulário |
| Fazer upgrade de versão sem downtime | ? | Transparente |
| Descobrir quem consome um tópico | ? | Stream Lineage |
| Adicionar um conector CDC do Postgres | ? | Formulário (Curso 3) |
| Garantir SLA de 99,99% | ? | Contratual |

**Entregável**: preencha a coluna do meio com uma estimativa honesta.

---

## Passo 3 — A pergunta de checagem *(~5 min)*

> Sua empresa está em três nuvens (AWS, Azure e GCP) e tem um datacenter próprio com
> um sistema legado que não sai de lá. Você precisa de um fluxo de eventos único
> atravessando tudo isso.
>
> Qual arquitetura você propõe — e por quê essa combinação?

---

## Gabarito

<details>
<summary><b>Passo 1 — as recomendações (clique para abrir)</b></summary>

**1. Fintech greenfield → Confluent Cloud.**
Time pequeno com prazo curto não pode gastar meses operando broker. E o pedido inclui
CDC e analytics — conectores gerenciados e Flink serverless entregam isso por
configuração. O custo de oportunidade de operar Kafka aqui é maior que a fatura.

**2. Banco com data residency → Confluent Platform.**
É o cenário clássico do self-managed: o dado não pode sair da infraestrutura própria.
O time de 15 pessoas comporta a operação, e o Platform dá as ferramentas enterprise
(RBAC, Cluster Linking, SR) sem abrir mão do controle. MSK não serve porque é AWS;
Cloud não serve pela exigência regulatória.

**3. 100% AWS, Kafka puro, time forte → AWS MSK.**
É onde o MSK brilha: se você só precisa de broker, está inteiro na AWS e tem gente
para operar, o MSK entrega Kafka provisionado com integração IAM nativa. O ecossistema
Confluent (Flink unificado, Tableflow, lineage) não seria aproveitado — pagar por ele
seria desperdício.

**4. ERP on-premises → híbrido: Platform on-prem + Cloud, ligados por Cluster Linking.**
O legado fica onde está; o Cluster Linking espelha os eventos para a nuvem, onde o
analytics roda. É exatamente o assunto da [Aula 5](../aula5/README.md).

**5. Startup já no Cloud, quer antifraude → Confluent Cloud + Flink.**
Não há decisão de plataforma aqui — há de **produto**. O Flink serverless lê os tópicos
e o Schema Registry nativamente, sem cluster separado nem billing à parte. É o Curso 4.

**O padrão**: a decisão quase nunca é sobre o Kafka em si. É sobre **quem você quer que
opere** e **de quanto do ecossistema você precisa**.

</details>

<details>
<summary><b>Passo 2 — as estimativas</b></summary>

| Tarefa | Self-managed (realista) |
|---|---|
| Cluster RF=3 em 3 zonas | **Dias a semanas** — provisionar VMs, configurar rede/listeners, testar failover |
| Upgrade sem downtime | **Semanas de planejamento** — rolling restart, testes de compatibilidade, janela de manutenção |
| Descobrir quem consome | **Horas a dias** — inspecionar consumer groups, perguntar nos times, montar planilha (que desatualiza) |
| Conector CDC | **Dias** — subir Connect workers, instalar plugin, dimensionar, monitorar |
| SLA 99,99% | **Não é uma tarefa** — é um programa contínuo de engenharia (on-call, DR, testes de caos) |

O ponto não é que self-managed seja errado — é que **cada linha dessa tabela é um time
que você precisa ter**. A pergunta é se streaming é o seu produto ou a sua fundação.

</details>

<details>
<summary><b>Passo 3 — multi-cloud + on-premises</b></summary>

**Proposta**: Confluent Cloud como espinha dorsal nas três nuvens + Confluent Platform
no datacenter, todos conectados por **Cluster Linking**.

**Por quê essa combinação**:

- **Cloud nas três nuvens**: é o único dos três que roda em AWS, Azure e GCP com a
  mesma experiência. MSK te prenderia à AWS.
- **Platform no datacenter**: o legado não sai de lá, então o broker vai até ele.
- **Cluster Linking como costura**: replica byte a byte **preservando offsets** — um
  consumidor pode migrar de cluster sem reprocessar nem perder posição. É o que
  diferencia de uma replicação genérica.

**O que essa arquitetura evita**: um zoológico de pipelines ponto a ponto entre nuvens,
cada um com seu formato, seu monitoramento e seu jeito de falhar.

</details>

---

## Erros comuns

| Se você… | O que está confundindo |
|---|---|
| recomendou Cloud para o banco com data residency | Requisito regulatório não se negocia com conveniência |
| recomendou Cloud para o cenário 3 | Pagar por ecossistema que não se usa é desperdício; MSK basta |
| tratou MSK como "Confluent na AWS" | MSK é **Kafka OSS provisionado**; SR, Connect e Flink são serviços separados, com billing próprio |
| achou que gerenciado significa "sem responsabilidade" | Você continua responsável por schema, partições, chaves, ACLs e **custo** |

---

## Glossário

| Termo | Definição |
|---|---|
| **Fronteira de gestão** | Onde termina a sua responsabilidade operacional e começa a do fornecedor. |
| **Kafka OSS** | Apache Kafka puro — o do sandbox do Curso 1. |
| **AWS MSK** | Kafka OSS provisionado pela AWS; você ainda opera o Kafka. |
| **Confluent Platform** | Distribuição enterprise self-managed, na sua infraestrutura. |
| **Confluent Cloud** | Plataforma fully managed (Kora), com o ecossistema integrado. |
| **KRaft** | Modo de consenso que eliminou o ZooKeeper. |
| **Kora** | O motor do Confluent Cloud — Kafka reescrito para nuvem. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Hierarquia e tipos de cluster | [Aula 2](../aula2/README.md) |
| Stream Governance na prática | [Aula 3](../aula3/README.md) e [Aula 4](../aula4/README.md) |
| Cluster Linking (o híbrido) | [Aula 5](../aula5/README.md) |
| Conectores gerenciados | Curso 3 |
| Flink serverless | Curso 4 |

**Fontes**

- Confluent — [Confluent vs. Apache Kafka](https://www.confluent.io/confluent-vs-apache-kafka/)
- Confluent — [Kora: The Cloud Native Engine for Apache Kafka](https://www.confluent.io/blog/cloud-native-data-streaming-kafka-engine/)
- Apache Kafka — [KRaft](https://kafka.apache.org/documentation/#kraft)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 0 · Setup](../aula0/README.md) | [Curso 2](../README.md) | [Aula 2 · Primeiros passos](../aula2/README.md) |
