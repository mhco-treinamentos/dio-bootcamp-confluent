# Curso 7 · Aula 2 — Observabilidade: Metrics API, Consumer Lag, Alertas e Dashboards (Prometheus/Grafana)

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 1 do Curso 7

---

## 1. Objetivo da Aula

O primeiro pilar: **enxergar**. Dissecar as fontes de sinal do Confluent Cloud — a **Metrics API** (o dado bruto de tudo), o **endpoint de export** (Prometheus/Grafana falando com a plataforma), os **alertas/notificações** — e coroar o **consumer lag** como a métrica rainha do streaming, com a disciplina de alertar sobre o que tem ação.

---

## 2. Conceitos Simplificados

### 2.1 A anatomia das fontes de sinal *(~4 min)*

```
 CONFLUENT CLOUD (tudo emite métrica)
    │
    ├─► UI (dashboards prontos)      → investigar: humano olhando
    │
    ├─► METRICS API (REST)           → consultar: o dado bruto, sob demanda
    │     descriptors (o cardápio) + queries (a pergunta)
    │
    ├─► EXPORT ENDPOINT (/export)    → integrar: formato Prometheus,
    │     o SEU stack (Prometheus/Grafana/Datadog) raspando a plataforma
    │
    └─► NOTIFICATIONS / ALERTAS      → ser avisado: a plataforma te acorda
```

- **UI para investigar, API para automatizar, export para unificar, alerta para dormir** — os quatro papéis, sem competição entre eles
- A **Metrics API** tem dois lados: os **descriptors** (o catálogo do que existe para medir — por cluster, tópico, conector, Flink...) e as **queries** (agregações por recurso e janela). Autentica com **API key de escopo Cloud** — mais um caso do modelo do Curso 2
- O **export endpoint** entrega as métricas em formato Prometheus: o Grafana da empresa passa a enxergar o Confluent Cloud **ao lado** do resto do parque — observabilidade unificada, sem agente instalado (não há onde instalar: fronteira de gestão)

### 2.2 Consumer lag: a métrica rainha *(~5 min)*

O lag (Curso 1, Aula 4: `log end offset − committed offset`, por partição) merece o trono porque **quase tudo no pipeline é um consumidor**:

```
                     lag crescendo em...        significa...
 consumer da app  ──► o serviço                 processamento não acompanha
 sink connector   ──► a entrega (C3)            destino lento/fora
 statement Flink  ──► a transformação (C4)      query subdimensionada/parada
 Tableflow        ──► o analítico (C5)          materialização atrasada
 RTCE             ──► o contexto da IA (C6)     agente decidindo sobre o passado
```

Como ler o lag como gente grande:

- **Lag absoluto engana** — 10.000 mensagens de lag num tópico de 1M msg/s é nada; num de 10 msg/s é 16 minutos de atraso. A leitura certa é **lag em tempo** ("quantos segundos atrás está o consumidor?") ou a **tendência** (estável × crescendo)
- **Lag estável alto** = capacidade no limite (dimensionar); **lag crescendo sem parar** = incidente (algo parou ou não dá conta); **lag zerado sempre** = folga (ou ninguém produzindo — também vale checar!)
- O alerta canônico do streaming: **"lag em tempo > X por Y minutos"** — com X vindo do SLO da Aula 1, não de um número mágico

### 2.3 O que medir por peça (o cardápio mínimo) *(~4 min)*

A tabela 2.2 da Aula 1, agora com as métricas nomeadas:

| Peça | Métricas mínimas | O alerta que importa |
|---|---|---|
| Cluster/tópicos | throughput in/out, storage, nº de partições | Aproximação de limites do cluster type |
| Conectores | status, task failures, **DLT depth** | Status ≠ RUNNING; **DLT crescendo** (C3: o desvio é sintoma) |
| Flink | status do statement, CFUs, lag | Statement FAILED/parado; pool batendo no teto |
| Tableflow | status da materialização, lag, sync do catálogo | Suspensão (C5: schema!); catálogo em Failed |
| Schema Registry | evoluções por subject | (auditoria mais que alerta — quem mudou o contrato) |
| IA (C6) | latência/erro por chamada de modelo, custo | Erro do provedor acima de X%; custo/dia acima do orçado |

- Repare no padrão: **status + lag + a métrica de sintoma específica** (DLT, suspensão, custo) — três sinais por peça cobrem 90% dos incidentes
- E a disciplina da Aula 1 aplicada: cada linha da coluna "alerta" tem **ação definida** — alerta sem runbook é ruído

### 2.4 Dashboards: a hierarquia da investigação *(~3 min)*

- **O dashboard de topo** — uma tela, o pipeline inteiro: os status + lags em tempo da tabela acima. A pergunta que responde: *"está tudo bem?"* em 5 segundos
- **Os dashboards de peça** — o drill-down quando o topo acusa: o conector específico, o statement específico
- **Grafana sobre o export** — quando o pipeline convive com o resto da empresa, o dashboard de topo mora onde o time já olha (o argumento do export endpoint)
- Anti-padrão nomeado: o **dashboard-enfeite** — 40 gráficos que ninguém definiu quando olhar. Dashboard bom nasce de pergunta, não de métrica disponível

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: o pipeline do bootcamp com um consumidor artificialmente lento (para dar lag visível). Tempo de tela: ~7 min.

**O que a demo deve mostrar, nesta ordem:**

1. **A Metrics API na unha** — criar a API key de escopo Cloud; `curl` nos descriptors (o cardápio) e uma query de throughput do cluster.
   - **Observação-alvo:** tudo que a UI mostra vem **daqui** — automatizável, integrável; e o escopo da key sendo o do Curso 2.

2. **O lag acontecendo** — o consumidor lento no grupo `pedidos-app` com produção normal; a tela de consumer lag da UI mostrando o lag **crescendo**, por partição.
   - **Observação-alvo:** a métrica rainha ao vivo — e a leitura em tendência (crescendo = incidente em formação), não em número absoluto.

3. **O alerta que acorda** — criar uma notificação/alerta sobre a condição relevante (lag/status de recurso), com destino visível (e-mail/webhook).
   - **Observação-alvo:** a passagem de "olhar dashboard" para "ser avisado" — o alerta nasce do SLO, com ação definida na tela ("o que eu faço quando ele dispara").

4. **A cura visível** — matar o consumidor lento, subir um saudável (ou escalar o grupo — o rebalance do Curso 1 trabalhando), e assistir o lag **drenar**.
   - **Observação-alvo:** o ciclo completo detectar → agir → confirmar pela métrica — operação de verdade, em miniatura.

5. **O export para o mundo Prometheus** — mostrar o endpoint `/export` respondendo em formato Prometheus (curl), e a tela/conceito do Grafana raspando-o.
   - **Observação-alvo:** o Confluent Cloud entrando no stack de observabilidade da empresa **sem agente** — três linhas de configuração de scrape.

**Fechamento na tela** *(~1 min)*: "enxergamos tudo — agora vamos trancar as portas: quem pode fazer o quê, com RBAC e ACLs de verdade. Próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Lag alto = problema."** Lag é leitura de **tendência e tempo**, não de número — o lag estável de um consumidor batch-like pode ser saudável. Ensinar a pergunta ("quantos segundos atrás? crescendo?"), não o susto.
- **"Vou alertar sobre tudo, por garantia."** Alert fatigue é como se perde o incidente real. Alerta = SLO violado + ação definida. O resto é dashboard.
- **"Preciso instalar um agente para monitorar."** Não há onde — a plataforma **expõe** (API/export) e o seu stack consome. A fronteira de gestão vale para a observabilidade também.
- **"DLT com mensagens = tratado."** DLT crescendo é sintoma ativo (C3) — a métrica de profundidade do DLT merece alerta próprio, com o runbook de reprocesso.
- **Escopo:** tracing distribuído e observabilidade de aplicação (OpenTelemetry nos seus serviços) são a metade que vive do lado de fora — citar que o pipeline completo une os dois mundos.

**Ganchos com a trilha:**

- Lag e offsets → **Curso 1, Aula 4** (a mecânica que agora viramos sinal).
- DLT como métrica → **Curso 3, Aula 4**.
- Status de statements e CFUs → **Curso 4, Aula 4**.
- Suspensão do Tableflow → **Curso 5, Aula 3**.
- No **Desafio Final**, o README pede as métricas de throughput e latência — esta aula é a fonte delas.

**Perguntas para checar entendimento:**

1. O lag do sink está em 50.000 mensagens, estável há 3 dias. Incidente? Que duas informações faltam para responder — e qual alerta você configuraria?
2. Por que o export endpoint é arquiteturalmente mais importante que os dashboards prontos da UI para uma empresa com Grafana consolidado?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Metrics API** | A API REST de métricas do Confluent Cloud: descriptors (catálogo) + queries (consultas). |
| **Descriptor** | A definição de uma métrica disponível (nome, recurso, agregações). |
| **Export endpoint** | O endpoint em formato Prometheus para o stack de observabilidade externo raspar. |
| **Scrape (Prometheus)** | A coleta periódica de métricas de um endpoint. |
| **Consumer lag em tempo** | O atraso do consumidor medido em segundos — a leitura correta do lag. |
| **Notification / alerta** | A regra que transforma condição em aviso com destino (e-mail, webhook). |
| **Runbook** | O procedimento definido de resposta a um alerta — o que separa alerta de ruído. |
| **DLT depth** | A profundidade do Dead Letter Topic — a métrica de sintoma do tratamento de erros. |
| **Dashboard de topo** | A tela única que responde "está tudo bem?" para o pipeline inteiro. |

---

## 6. Referências

**Livros:**

- Betsy Beyer et al. — *Site Reliability Engineering* — cap. 6 ("Monitoring Distributed Systems") — https://sre.google/sre-book/monitoring-distributed-systems/
- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* — cap. 13 (as métricas do Kafka na fonte)

**Documentação oficial:**

- Confluent Cloud — *Metrics API (referência e descriptors)* — https://api.telemetry.confluent.cloud/docs
- Confluent Cloud — *Monitor Consumer Lag* — https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html
- Confluent Cloud — *Export Metrics (Prometheus/Grafana/Datadog)* — https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
- Confluent Cloud — *Notifications* — https://docs.confluent.io/cloud/current/monitoring/configure-notifications.html
