# Curso 7 · Aula 5 — Multi-Region, Disaster Recovery e Garantias de Entrega (at-least-once e exactly-once)

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · **Aula conceitual+** (DR multi-region exige Dedicated/Enterprise — telas e diagramas do instrutor)
> **Pré-requisitos:** o bootcamp inteiro — esta é a aula que consolida
> *A última aula gravada do bootcamp.*

---

## 1. Objetivo da Aula

O quarto pilar: **sobreviver ao pior dia**. Subir a escada da disponibilidade — réplicas (zona), **multi-zone** (o default do Cloud) e **multi-region com Cluster Linking** — dominar o vocabulário do DR (**RPO/RTO**) e o runbook de failover; e fechar o bootcamp consolidando o arco das **garantias de entrega**: a tabela final de at-most/at-least/exactly-once atravessando o pipeline inteiro, do producer ao agente.

---

## 2. Conceitos Simplificados

### 2.1 A escada da disponibilidade *(~4 min)*

Cada degrau responde a uma pergunta de sobrevivência:

```
 ① BROKER caiu?      → RÉPLICAS (RF=3, ISR, acks=all — Curso 1, Aula 5)
                        o trio que o aluno carrega desde o Bloco 1
 ② ZONA caiu?        → MULTI-ZONE: réplicas espalhadas entre AZs
                        o default dos clusters do Cloud — o 99,99% do SLA (C2)
                        vem DAQUI; automático, invisível, já pago
 ③ REGIÃO caiu?      → MULTI-REGION: um SEGUNDO cluster, em outra região,
                        alimentado por CLUSTER LINKING (C2, Aula 5!) — o degrau
                        que é DECISÃO e projeto, não default
```

- Os degraus ① e ② **a plataforma já entrega** — a fronteira de gestão cobrindo a falha de máquina e de zona
- O degrau ③ é **seu**: custa (dois clusters + link + rede), exige runbook, e se justifica por requisito (regulatório, negócio crítico) — não por moda. A pergunta certa: *"quanto custa uma hora fora do ar?"* — a resposta dimensiona o investimento

### 2.2 O vocabulário do DR: RPO e RTO *(~4 min)*

As duas métricas que todo plano de DR precisa cravar **antes** do desenho:

```
                     o DESASTRE
 ────────────────────────█────────────────────────►  tempo
          ◄── RPO ──►    █    ◄──── RTO ────►
     "quanto DADO posso  █   "quanto TEMPO posso
      perder?" (o que    █    ficar fora?" (até o
      ainda não replicou)█    failover completar)
```

- **RPO (Recovery Point Objective)** — a janela de dado em risco: com Cluster Linking contínuo, é o **lag do link** no instante do desastre (segundos, tipicamente) — e ele se monitora como qualquer lag (Aula 2!)
- **RTO (Recovery Time Objective)** — o relógio da volta: detecção + decisão + failover + clientes reapontados
- A honestidade que separa plano de fantasia: **RPO zero e RTO zero não existem a custo finito** — DR é declarar números aceitáveis e desenhar (e TESTAR) para eles
- E o eco do Curso 4: a watermark era "quanto atraso o resultado tolera"; o RPO é "quanta perda o negócio tolera" — **produção madura é trade-off declarado**, sempre

### 2.3 Anatomia do DR com Cluster Linking *(~5 min)*

O desenho canônico, com as peças do Curso 2, Aula 5, agora em modo contingência:

```
 REGIÃO PRIMÁRIA (ativa)                REGIÃO DR (standby)
 cluster A ──── cluster link ────────►  cluster B
 topics ativos                          mirror topics (read-only,
 producers + consumers                  byte a byte, OFFSETS PRESERVADOS)
                                        consumers de leitura opcionais

 ── O DIA DO DESASTRE (o runbook) ──
 ① DETECTAR   — o alerta (Aula 2), não o cliente
 ② DECIDIR    — humano aciona o plano (failover é decisão, não reflexo)
 ③ FAILOVER   — promover os mirror topics (viram graváveis — C2A5)
 ④ REAPONTAR  — producers/consumers para o cluster B
               (offsets preservados: consumers CONTINUAM de onde estavam!)
 ⑤ o RPO real = o que o link ainda não tinha replicado
```

- **Offsets preservados é o superpoder do DR também** — sem eles, o failover vira o pesadelo de "de onde cada consumer recomeça?" (a dor do MM2, lembrada); com eles, reapontar é quase tudo
- **Failover é decisão com dono** — o runbook nomeia quem decide e o critério; automatizar a decisão de failover regional é receita de failover acidental
- **DR não testado é documento, não plano** — o game day (simular a perda, executar o runbook, medir RPO/RTO reais) é o que transforma slide em capacidade
- E o desenho vale além do desastre: o mesmo link serve **migração** (C2A5), **agregação de regiões** e **data sharing** — um mecanismo, várias histórias

### 2.4 A consolidação: as garantias fim a fim *(~5 min)*

O arco que começou na Aula 5 do Curso 1, fechado — **onde cada garantia mora no pipeline completo**:

| Trecho | O mecanismo | A garantia | Onde aprendemos |
|---|---|---|---|
| Producer → broker | `acks=all` + idempotência (PID/seq) + RF=3/ISR | Escrita durável, sem duplicata de retry | C1A5 |
| Broker → consumer | Commit **após** processar | At-least-once (a releitura possível) | C1A4–5 |
| Consumer → efeito | **Idempotência de negócio** (dedup/upsert) | Duplicata inofensiva ≈ exactly-once | C1A5, C3A3 |
| CDC (banco → topic) | WAL + posição de replicação | Nada do banco se perde | C3A2 |
| Sink (topic → destino) | Upsert por chave no destino | Entrega sem duplicar | C3A3 |
| Flink (topic → topic) | Checkpoint + transações + read_committed | **Exactly-once de processamento** | C4A4 |
| Tableflow / RTCE | Materialização contínua da fonte | A representação herda as garantias do log | C5–C6 |
| Entre regiões | Cluster link (byte a byte, offsets) | O DR com RPO mensurável | C2A5 + esta aula |

As três frases que resumem o bootcamp:

1. **A garantia fim a fim é a do elo mais fraco** — de nada vale EOS no Flink com um consumidor final que não deduplica: auditar o pipeline é auditar **cada trecho** da tabela
2. **A síntese pragmática venceu na maioria:** *at-least-once + idempotência onde o efeito acontece* — barata, robusta, suficiente; **EOS transacional onde duplicata é inaceitável** e o custo de latência cabe
3. **E tudo se apoia no mesmo alicerce:** o log imutável, replicado e replayável da Aula 3 do Curso 1 — a estrutura mais simples que existe, carregando o bootcamp inteiro nas costas

**Fechamento do bootcamp gravado** *(~1 min)*: "trinta e duas aulas atrás, um evento era um JSON com verbo no passado. Hoje ele atravessa CDC, contrato, Flink, Lakehouse e IA — observado, seguro, no custo certo e pronto pro pior dia. O que falta é SEU: o Desafio Final. Nos vemos na Live 3 — e no seu README."

---

## 3. Demonstração Visual — Especificação

> **Sem hands-on nesta aula** (DR real exige Dedicated/Enterprise em duas regiões — transparência de custo, como na C2A5). A demonstração é narrada sobre diagramas + telas da doc, com um momento interativo de consolidação.

**Roteiro sugerido:**

1. **A escada** — o diagrama ①②③ da seção 2.1, apontando o que o aluno **já tem** (o 99,99% multi-zone do cluster dele) versus o degrau que é projeto.
2. **O runbook animado** — o desenho da seção 2.3 passo a passo, com ênfase teatral no ④: "os consumers continuam **de onde estavam**" (ligar à demo de rebalance do C1A4 — a mesma sensação, escala de região).
3. **Telas reais** — a configuração de um cluster link e de mirror topics na doc/UI (instrutor), e o lag do link como métrica de RPO vivo.
4. **A tabela das garantias** — percorrer a tabela da seção 2.4 linha a linha, cada uma com o "lembra quando...?" da demo correspondente do bootcamp. É a revisão geral disfarçada de fechamento.
5. **O convite ao capstone** — o checklist do README do Desafio Final na tela, mapeado aos quatro pilares do curso.

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"O SLA de 99,99% já é meu DR."** Multi-zone cobre zona; **região** é outro degrau — e é decisão sua, com custo e runbook. Confundir os degraus é descobrir a diferença no pior dia.
- **"RPO/RTO zero, por favor."** A custo finito, não existe — DR maduro declara números e testa contra eles. Quem promete zero não fez a conta.
- **"Failover automático é mais seguro."** Para região, automatizar a **decisão** convida o failover acidental (e o split-brain de clientes). Detecção automática, decisão com dono.
- **"Exactly-once resolve o pipeline inteiro."** EOS tem **escopo** (o trecho Flink→Kafka, ex.); a garantia fim a fim é auditada trecho a trecho — a tabela 2.4 existe para essa auditoria.
- **"DR testado uma vez está testado."** Pipeline muda, gente muda — game day é rotina (semestral que seja), não cerimônia de inauguração.

**Ganchos com a trilha:**

- O trio RF/ISR/acks → **C1A5** (o degrau ① desde sempre).
- Cluster Linking e offsets preservados → **C2A5** (a migração que virou contingência).
- O lag do link como métrica → **Aula 2** deste curso.
- Checkpoints e EOS do Flink → **C4A4**.
- O **Desafio Final** cobra a semântica de entrega declarada e justificada — a tabela 2.4 é o gabarito do aluno.

**Perguntas para checar entendimento:**

1. Sua empresa declara RPO de 30 s e RTO de 15 min. Descreva o que cada número exige do desenho (link, monitoramento, runbook) — e como você **provaria** os dois números.
2. Audite o pipeline do bootcamp com a tabela 2.4: em qual trecho uma duplicata ainda chegaria ao usuário final se o time esquecer **uma** disciplina — e qual é ela?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Multi-zone (AZs)** | Réplicas distribuídas entre zonas de disponibilidade — o default do Cloud e a base do SLA. |
| **Multi-region DR** | Cluster de contingência em outra região, alimentado por Cluster Linking. |
| **RPO (Recovery Point Objective)** | O máximo de dado que se aceita perder — com link contínuo, ≈ o lag do link. |
| **RTO (Recovery Time Objective)** | O máximo de tempo até a operação voltar — o relógio do runbook. |
| **Failover / promote** | Promover os mirror topics e reapontar clientes para o cluster DR. |
| **Failback** | O caminho de volta à região primária após o desastre — parte do plano, não improviso. |
| **Runbook de DR** | O procedimento com dono, critérios e passos — o que se executa no game day. |
| **Game day** | O teste real do plano: simular a perda, executar, medir RPO/RTO. |
| **Garantia fim a fim** | A semântica de entrega auditada trecho a trecho — a do elo mais fraco. |

---

## 6. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* — caps. 7–8 e 10 (confiabilidade, EOS e replicação entre clusters). E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Betsy Beyer et al. — *Site Reliability Engineering* — caps. de gerenciamento de incidentes e testes de desastre — https://sre.google/sre-book/table-of-contents/

**Documentação oficial:**

- Confluent Cloud — *Cluster Linking: Disaster Recovery and Failover* — https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html
- Confluent Cloud — *Multi-Zone Clusters e SLA* — https://www.confluent.io/confluent-cloud/sla/
- Confluent — *Disaster Recovery no Confluent Cloud (whitepaper/blog)* — https://www.confluent.io/blog/disaster-recovery-multi-datacenter-apache-kafka-deployments/
- Apache Kafka — *Message Delivery Semantics (a consolidação, na fonte)* — https://kafka.apache.org/documentation/#semantics
