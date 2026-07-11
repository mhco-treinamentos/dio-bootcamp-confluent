# Curso 1 · Aula 6 — Do Monólito ao EDA: a relação com microsserviços e quando migrar

> **Bootcamp Confluent Cloud 2026 · Bloco 1 — Fundamentos**
> Duração alvo: ~15 min · Aula 100% conceitual (fecha o Bloco 1)

---

## 1. Objetivo da Aula

Situar EDA na evolução das arquiteturas — do **monólito** aos **microsserviços** e destes à **arquitetura orientada a eventos** — entendendo o que cada salto resolve, o que ele **não** resolve, e os critérios honestos de **quando migrar** (e quando não).

---

## 2. Conceitos Simplificados

### 2.1 O monólito: onde quase todo mundo começa *(~3 min)*

Uma aplicação, um deploy, um banco. E isso **não é xingamento**:

- **Forças** — simplicidade operacional, transações ACID no mesmo banco, chamadas locais (sem rede), debugging direto
- **Onde dói** — quando o sistema e o time crescem: deploy de tudo para mudar qualquer coisa, escala do bloco inteiro para aliviar um gargalo, times pisando no mesmo código

A dor do monólito é **dor de crescimento**, não defeito de nascença. Migrar sem essa dor é pagar complexidade sem receber benefício.

### 2.2 Microsserviços: quebrar o deploy — e ganhar um problema novo *(~4 min)*

Microsserviços fatiam o monólito em serviços independentes, cada um com seu deploy, sua escala e (idealmente) seu dado. Resolvem a autonomia dos times — mas a pergunta que define esta aula é:

> **Como os serviços conversam?**

Se a resposta for "REST chamando REST", nasce o **monólito distribuído**: as mesmas dependências de antes, agora com rede no meio.

- A cadeia síncrona da Aula 1 (Checkout → Pagamento → Estoque → Notificação) volta — com latência somada, falha em cascata e **acoplamento temporal**: todos precisam estar de pé **ao mesmo tempo**
- A disponibilidade composta despenca: 4 serviços com 99,5% cada ≈ **98%** o fluxo inteiro — quebrou-se o deploy, não a dependência

### 2.3 EDA: quebrar a dependência *(~4 min)*

O terceiro salto muda a comunicação: serviços deixam de **chamar** uns aos outros e passam a **publicar fatos** num log central (o Kafka das Aulas 3–5) e a **reagir** aos fatos dos outros.

O que muda em relação ao monólito distribuído:

- **Acoplamento temporal quebrado** — o produtor publica mesmo com consumidores fora do ar; quando voltam, o log os espera (retention + offsets, Aula 3)
- **Dependência invertida** — o Checkout não conhece o antifraude; o antifraude é que decide ouvir `PedidoCriado`. Adicionar consumidor não toca no produtor
- **O dado vira produto** — o fluxo de eventos é reutilizável: o mesmo `PedidoCriado` serve operação, análise e IA (é a ponte para os Blocos 2 e 3)

E o que **não** muda: consistência forte imediata entre serviços não existe mais — o mundo passa a ser **eventualmente consistente**, e isso exige desenho (os padrões da Live 2: Event Sourcing, CQRS, Saga).

### 2.4 Quando migrar — e quando não *(~3 min)*

Critérios honestos, sem evangelismo:

**Sinais de que EDA paga a conta:**
- Vários consumidores interessados nos mesmos fatos (o dado de pedido alimenta 4+ sistemas)
- Picos de carga que pedem amortecimento (o log como buffer natural)
- Necessidade de auditoria/replay ("o que aconteceu e em que ordem?")
- Times autônomos sofrendo com dependência síncrona entre domínios

**Sinais de que ainda não:**
- CRUD simples, um consumidor por dado, time pequeno — o monólito bem feito vence
- A organização não está pronta para consistência eventual (produto e negócio inclusos)
- Migração "big bang" no plano — a rota sensata é **incremental**: começar por **um** fluxo de alto valor (ex.: pedidos), publicar eventos dele, plugar consumidores aos poucos (o padrão *strangler*; e a ponte legado → streaming via CDC é exatamente o **Curso 3**)

**Fechamento do Bloco 1** *(~1 min)*: "vocabulário (Aula 1), semânticas de distribuição (Aula 2), o log (Aula 3), a mecânica de escala (Aula 4), as garantias (Aula 5) e agora o mapa de decisão arquitetural. O modelo mental está completo — no Bloco 2, ele ganha uma plataforma: o Confluent Cloud. Antes, nos vemos na Live 1: por que o mercado não aguenta mais batch."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Monólito é atraso; microsserviço é evolução obrigatória."** Arquitetura é resposta a contexto. Monólito bem modularizado atende à maioria das empresas por muito tempo — a migração se justifica pela **dor**, não pela moda.
- **"Microsserviços resolvem acoplamento."** Resolvem acoplamento de **deploy**. Comunicação síncrona mantém o acoplamento **temporal e de disponibilidade** — o monólito distribuído é o anti-padrão central da aula.
- **"EDA é tudo assíncrono, sempre."** Consulta que precisa de resposta imediata continua síncrona (reforço da Aula 1). EDA domina a comunicação **entre domínios** e a propagação de fatos.
- **"Consistência eventual é defeito."** É trade-off consciente: troca-se a ilusão de consistência imediata distribuída por resiliência e autonomia — e desenha-se para isso (Live 2).

**Ganchos com a trilha:**

- Padrões que domesticam a consistência eventual (Event Sourcing, CQRS, Saga) → **Live 2**.
- Migração incremental do legado com CDC (o strangler na prática) → **Curso 3**.
- ROI e o business case da migração → **Live 1**, na sequência imediata desta aula.
- O "dado como produto" floresce no **Bloco 3** (Lakehouse e Real-Time AI).

**Perguntas para checar entendimento:**

1. Uma empresa quebrou o monólito em 12 microsserviços que se chamam por REST. Que problema da Aula 1 ela reencontrou, e que propriedade do log resolve?
2. Cite um cenário em que você **recomendaria ficar** no monólito, e o sinal concreto que te faria reavaliar.

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **Monolith (monólito)** | Aplicação única com um deploy e, tipicamente, um banco compartilhado. |
| **Microservices (microsserviços)** | Arquitetura de serviços pequenos e independentes em deploy, escala e dados. |
| **Distributed monolith (monólito distribuído)** | Anti-padrão: serviços separados em deploy, mas acoplados por chamadas síncronas em cadeia. |
| **Temporal coupling (acoplamento temporal)** | Dependência em que as partes precisam estar disponíveis ao mesmo tempo para funcionar. |
| **Eventual consistency (consistência eventual)** | Modelo em que os sistemas convergem para o estado correto com o tempo, sem consistência imediata global. |
| **Strangler pattern** | Estratégia de migração incremental: o novo sistema cresce em volta do legado até substituí-lo. |
| **CDC (Change Data Capture)** | Técnica que captura mudanças do banco legado como eventos — a ponte de migração (Curso 3). |
| **Buffer / amortecimento** | Papel do log de absorver picos de produção sem derrubar consumidores. |

---

## 5. Referências

**Livros:**

- Sam Newman — *Building Microservices, 2ª ed.* (O'Reilly) — comunicação entre serviços, acoplamento e os trade-offs síncrono × assíncrono
- Sam Newman — *Monolith to Microservices* (O'Reilly) — o manual da migração incremental (strangler fig na prática)
- Ben Stopford — *Designing Event-Driven Systems* (O'Reilly/Confluent) — serviços orientados a eventos sobre um log compartilhado. E-book gratuito: https://www.confluent.io/designing-event-driven-systems/
- Adam Bellemare — *Building Event-Driven Microservices* (O'Reilly) — o dado como produto e a comunicação entre domínios via eventos

**Documentação e artigos oficiais:**

- Martin Fowler — *Microservices* — https://martinfowler.com/articles/microservices.html
- Martin Fowler — *Strangler Fig Application* — https://martinfowler.com/bliki/StranglerFigApplication.html
- Martin Fowler — *What do you mean by "Event-Driven"?* — https://martinfowler.com/articles/201701-event-driven.html
- Confluent — *Event-Driven Microservices* — https://www.confluent.io/learn/event-driven-microservices/
