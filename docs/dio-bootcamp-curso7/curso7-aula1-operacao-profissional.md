# Curso 7 · Aula 1 — Introdução à operação profissional no Confluent Cloud

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~15–18 min · Aula 100% conceitual (o mapa do curso)
> **Pré-requisitos:** Cursos 1–6 (o pipeline completo — que agora vamos operar)

---

## 1. Objetivo da Aula

Mudar o chapéu: de **construir** para **operar**. Entender o que muda no "Day 2" (o dia seguinte ao deploy), mapear a **superfície operacional** do pipeline que o bootcamp construiu — o que cada peça exige de atenção — e apresentar os quatro pilares que estruturam o curso: **observabilidade, segurança, custos e confiabilidade**.

---

## 2. Conceitos Simplificados

### 2.1 Day 1 × Day 2: a mudança de chapéu *(~4 min)*

- **Day 1** — fazer funcionar: os Cursos 1–6. A pergunta era *"como construir?"*
- **Day 2** — manter funcionando: para sempre, sob carga, com gente mudando código e o mundo mudando de volume. A pergunta vira *"como eu SEI que está funcionando — e quanto está custando?"*

O que muda de verdade em produção:

- **Falha deixa de ser 'se' e vira 'quando'** — o desenho passa a ser julgado pelo comportamento no pior dia, não no demo
- **O custo é contínuo** — o teardown do bootcamp era disciplina de estudo; em produção, cada recurso ocioso é dinheiro recorrente
- **Segurança deixa de ser configuração e vira processo** — credencial rotaciona, gente entra e sai, auditoria pergunta
- **E o gerenciado não te aposenta** — a fronteira de gestão (Curso 2) tirou o broker das suas costas; sobraram as decisões: o que alertar, quem acessa, quanto reter, como falhar

### 2.2 A superfície operacional do nosso pipeline *(~5 min)*

O inventário do que o bootcamp construiu — e o que cada peça cobra do operador:

| Peça (curso) | O que monitorar | O que pode doer |
|---|---|---|
| **Cluster + tópicos** (C1–C2) | Throughput, storage, partições | Partição quente; retention mal dimensionada |
| **Schema Registry** (C2) | Evoluções, compatibilidade | Breaking change forçada (congela metade do pipeline — C5!) |
| **Conectores** (C3) | Status, lag do sink, **profundidade do DLT** | Conector FAILED às 3h; DLT virando aterro |
| **Statements Flink** (C4) | Status, CFUs, lag de consumo, exceções | Statement morto = pipeline cego; esquecido = custo eterno |
| **Tableflow** (C5) | Status da materialização, lag, sync de catálogo | Suspensão por schema; analítico defasado |
| **IA: modelos e RTCE** (C6) | Latência/custo por chamada, status do RTCE | A conta do provedor; agente decidindo sobre contexto parado |

Duas leituras dessa tabela:

- **O consumer lag é o sinal transversal** — sink, Flink, Tableflow e RTCE são todos consumidores: *lag crescendo* é o sintoma universal de "alguém não está dando conta" (a Aula 2 o coroa como métrica rainha)
- **Cada peça que produz também pode falhar em silêncio** — e falha silenciosa é a pior categoria: o curso existe para transformar silêncio em alerta

### 2.3 Os quatro pilares (o mapa do curso) *(~4 min)*

```
 OPERAÇÃO PROFISSIONAL
 ├─ ① OBSERVABILIDADE (Aula 2) — "eu SEI o que está acontecendo"
 │     Metrics API, lag, alertas, dashboards
 ├─ ② SEGURANÇA (Aula 3) — "só entra quem deve, e fica registrado"
 │     RBAC, ACLs, service accounts na prática
 ├─ ③ CUSTOS (Aula 4) — "eu sei QUANTO custa e onde otimizar"
 │     retenção, compactação, Tiered Storage, Freight, sizing
 └─ ④ CONFIABILIDADE (Aula 5) — "eu sobrevivo ao pior dia"
       multi-region, DR, e as garantias de entrega consolidadas
```

- A ordem é deliberada: **não se protege, não se otimiza nem se recupera o que não se enxerga** — observabilidade primeiro
- E o critério de sucesso do curso é honesto: ao final, o aluno deve conseguir responder, sobre o próprio capstone, as quatro perguntas dos pilares — é exatamente o que o README do Desafio Final cobra (métricas, trade-offs, decisões)

### 2.4 Vocabulário de gente grande: SLI, SLO, SLA *(~3 min)*

- **SLI** (indicator) — a medida: "lag do consumidor de fraude", "latência p99 fim a fim"
- **SLO** (objective) — a meta interna: "lag < 10 s em 99% do tempo"
- **SLA** (agreement) — o contrato com consequência: o 99,99% do cluster (Curso 2) é o SLA **da plataforma com você**; o seu pipeline tem o **seu** SLA com o negócio
- A disciplina que muda tudo: **alertar sobre SLO, não sobre tudo** — alerta sem consequência definida é ruído, e ruído treina o time a ignorar (o anti-padrão nº 1 de observabilidade, que a Aula 2 ataca)

**Fechamento** *(~1 min)*: "construímos por seis cursos; agora aprendemos a dormir tranquilo com isso rodando. Primeiro pilar: enxergar. Metrics API, lag, alertas e dashboards — próxima aula."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"É gerenciado, não preciso operar."** A plataforma opera a **infraestrutura**; as **decisões** (alertas, acessos, retenção, DR) continuam suas. Fronteira de gestão ≠ terceirização de responsabilidade.
- **"Monitorar = olhar dashboard."** Dashboard é para investigar; **alerta** é para saber. Operação madura é ser acordado pelo sistema, não descobrir pelo cliente.
- **"Produção é o ambiente; o resto é igual."** Produção é um **modo de pensar**: pior dia, custo contínuo, acesso auditado. O curso ensina o modo, não o ambiente.
- **Tom do curso:** este é o curso menos "feature" e mais "disciplina" do bootcamp — as demos são sóbrias de propósito; o valor está nos critérios.

**Ganchos com a trilha:**

- Cada linha da tabela 2.2 vira prática nas Aulas 2–5.
- O README do **Desafio Final** pede exatamente as respostas dos quatro pilares — este curso é o gabarito da avaliação.
- A **Live 3** (governança e compliance) complementa o pilar de segurança com a visão regulatória.

**Perguntas para checar entendimento:**

1. Seu pipeline do capstone está "funcionando". Liste, peça a peça, como você **saberia** se deixasse de funcionar — sem abrir a UI.
2. Dê um exemplo de SLI → SLO → alerta para o consumidor de fraude do bootcamp. O que acontece quando o alerta dispara?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **Day 2 operations** | A disciplina de manter sistemas rodando após o deploy — o tema do curso. |
| **Superfície operacional** | O conjunto de peças e sinais que exigem atenção do operador. |
| **Falha silenciosa** | Degradação sem alerta — a categoria mais perigosa de incidente. |
| **SLI / SLO / SLA** | A medida / a meta interna / o contrato com consequência. |
| **p99 (percentil 99)** | A latência abaixo da qual ficam 99% das operações — a métrica do pior caso típico. |
| **Ruído de alerta (alert fatigue)** | Excesso de alertas sem ação definida — treina o time a ignorar o que importa. |

---

## 5. Referências

**Livros:**

- Betsy Beyer et al. — *Site Reliability Engineering* (O'Reilly/Google) — caps. de SLOs e monitoramento (gratuito online: https://sre.google/sre-book/table-of-contents/)
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* — cap. 13 ("Monitoring Kafka"). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/

**Documentação oficial:**

- Confluent Cloud — *Monitor Confluent Cloud (visão geral)* — https://docs.confluent.io/cloud/current/monitoring/index.html
- Confluent Cloud — *Service Level Agreement* — https://www.confluent.io/confluent-cloud/sla/
