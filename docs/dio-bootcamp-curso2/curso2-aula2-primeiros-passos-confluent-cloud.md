# Curso 2 · Aula 2 — Primeiros Passos no Confluent Cloud: Clusters, Environments, Service Accounts e API Keys

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisito:** Aula 0 concluída (conta, créditos, CLI autenticada)

---

## 1. Objetivo da Aula

Dissecar a organização de recursos do Confluent Cloud — **Organization → Environment → Cluster → Topic** — entender os **tipos de cluster** e quando usar cada um, e dominar o modelo de identidade e acesso: **Service Accounts** e a anatomia de uma **API Key** com escopo correto.

---

## 2. Conceitos Simplificados

### 2.1 Anatomia da hierarquia de recursos *(~4 min)*

```
ORGANIZATION  (sua empresa — billing, usuários, SSO)
   └── ENVIRONMENT  (fronteira lógica: dev | staging | prod)
          ├── Schema Registry  (1 por environment — governança dos contratos)
          └── CLUSTER  (o "Kafka": onde vivem os dados)
                 └── TOPIC  (o log do Curso 1, agora gerenciado)
                        └── partições, retention, configs...
```

- **Organization** — o topo: billing, usuários humanos, integração SSO. Uma por empresa (ou por conta de estudo)
- **Environment** — a fronteira de isolamento lógico: separar `dev`, `staging`, `prod` — cada um com **seu** Schema Registry e seu pacote de **Stream Governance** (Essentials/Advanced — Aula 1). É o anti-acidente: schema de teste não polui produção
- **Cluster** — onde tópicos e dados vivem; a unidade que tem tipo, região e custo
- **Topic** — velho conhecido do Curso 1; mesma anatomia (partições, retention), agora com defaults gerenciados

Regra prática de organização no mundo real: environments espelham o ciclo de vida do software; clusters separam por domínio/carga quando necessário.

### 2.2 Tipos de cluster: escolhendo o tamanho do problema *(~4 min)*

| Tipo | Infraestrutura | Elasticidade | Caso de uso | No bootcamp |
|---|---|---|---|---|
| **Basic** | Multi-tenant | Paga pelo uso, zero fixo | Estudo, dev, protótipo | **O nosso** |
| **Standard** | Multi-tenant | Paga pelo uso + SLA maior | Produção de entrada | — |
| **Enterprise** | Multi-tenant com isolamento reforçado | Elástico, private networking | Produção com requisitos de rede/segurança | — |
| **Dedicated** | Single-tenant (CKUs provisionados) | Provisionada (dimensiona em CKU) | Alto volume, requisitos estritos, Cluster Linking completo | Citado na Aula 5 |
| **Freight** | Otimizado para custo de rede | Elástico | Cargas de altíssimo volume tolerantes a latência | Citado no Curso 7 |

Duas ideias pra fixar:

- **Basic/Standard/Enterprise = elasticidade por consumo** (o Kora multi-tenant da Aula 1 em ação); **Dedicated = capacidade provisionada** medida em **CKU** (Confluent Kafka Unit — a unidade de dimensionamento)
- Tipo de cluster é **decisão reversível por migração, não por botão** — escolher bem cedo evita mudança depois (sizing é papo do Curso 7)

### 2.3 Identidade: usuários humanos × Service Accounts *(~3 min)*

Dois tipos de identidade, dois propósitos:

- **User Account** — pessoa: entra pela UI, tem MFA, pertence a alguém que pode sair da empresa
- **Service Account** — **aplicação**: identidade não-humana para producers, consumers, conectores e pipelines

A regra de ouro da aula:

> **Aplicação NUNCA usa credencial de pessoa.** Pessoa sai de férias, troca de emprego, perde o laptop — e a aplicação não pode cair junto.

### 2.4 Anatomia de uma API Key *(~4 min)*

A credencial que as aplicações usam para falar com o cluster:

```
┌────────────────────── API KEY ──────────────────────┐
│ key     → o "usuário" (ex.: ABCDEF123456)           │
│ secret  → a "senha" — exibida UMA vez, na criação   │
│ owner   → de quem ela é: user OU service account ◄── a decisão
│ scope   → a que recurso dá acesso:                  │
│            · cluster específico (Kafka API)         │
│            · Schema Registry                        │
│            · Cloud (gestão de recursos)             │
└──────────────────────────────────────────────────────┘
```

- **O secret aparece uma única vez** — não anotou, perdeu: revoga e cria outra. (E NUNCA vai para o Git)
- **Escopo mínimo** — key de cluster para falar com o cluster; key de Schema Registry para schemas; key Cloud só para automação de infraestrutura (Terraform). Uma key global "pra tudo" é o anti-padrão
- **Dona certa** — key de aplicação pertence a **service account**; key de estudo/exploração pode ser do seu user, com consciência de que é descartável
- Rotação: revogar + recriar deve ser rotina barata — mais um motivo para escopo pequeno

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — sequência de CLI no Confluent Cloud. Pré-requisito: Aula 0 (CLI autenticada). Tempo de tela: ~7–8 min. **Termina com teardown parcial.**

**O que a demo deve mostrar, nesta ordem:**

1. **Explorar a hierarquia via CLI** — listar environments, entrar no `bootcamp-dio` (Aula 0), listar clusters, descrever o cluster Basic (ID, região, tipo, endpoint).
   - **Observação-alvo:** a hierarquia da seção 2.1 existe de verdade e é navegável — cada nível tem ID próprio (env-xxx, lkc-xxx).

2. **Criar um tópico gerenciado** — `pedidos` com 3 partições (o mesmo desenho do Curso 1), e descrever o tópico exibindo as configs default.
   - **Observação-alvo:** mesma anatomia do Curso 1 (partições, retention) — mas repare: **ninguém subiu broker nenhum**. A fronteira de gestão da Aula 1, sentida na prática.

3. **Criar uma Service Account** — `app-pedidos`, com descrição.
   - **Observação-alvo:** a identidade da aplicação existe **antes** de qualquer credencial — identidade e credencial são coisas separadas.

4. **Criar uma API Key ESCOPADA** — key de recurso Kafka, apontando pro cluster, com owner = service account `app-pedidos`.
   - **Observação-alvo:** a tela mostra o **secret uma única vez** (dramatizar isso); a key lista owner e escopo — a anatomia da seção 2.4, viva.

5. **Usar a key** — produzir 2–3 mensagens no tópico `pedidos` autenticando com a key da service account, e consumir de volta.
   - **Observação-alvo:** a aplicação (representada pela SA) fala com o cluster com credencial própria — nenhum login humano envolvido no caminho do dado.

6. **Teardown parcial** — apagar o tópico de teste; **manter** cluster, SA e key (serão usados nas Aulas 3–4). Conferir que nada além do cluster ocioso ficou ativo.
   - **Observação-alvo:** o ritual da Aula 0 virou hábito — e o critério do que fica × o que morre é consciente.

**Fechamento na tela** *(~1 min)*: "hierarquia navegada, identidade certa, credencial escopada. Falta o contrato do dado: na próxima aula, o Schema Registry entra em cena."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Environment é frescura pra conta de estudo."** É o hábito que evita o desastre clássico em empresa (schema/tópico de teste em produção). Custo zero, disciplina alta — criar `dev` desde o dia 1.
- **"Uma API key resolve tudo."** Key global é o equivalente moderno do usuário `admin` compartilhado. Escopo mínimo + dona certa + rotação barata.
- **"Perdi o secret, deixa eu procurar no histórico."** Não existe recuperação — revoga e cria. Ensinar isso como fluxo normal, não como falha.
- **"Service account é burocracia."** É o que separa "aplicação caiu porque o dev saiu da empresa" de operação séria. RBAC fino sobre essas identidades é o **Curso 7, Aula 3**.
- **Disciplina de relógio:** a tabela de cluster types é consulta — em vídeo, narrar Basic (nosso), Dedicated/CKU (contraste) e citar que o resto está na documentação de apoio.

**Ganchos com a trilha:**

- Schema Registry por environment → **Aula 3** (é lá que ele trabalha).
- A API key da SA volta a ser usada na **Aula 4** (mão na massa ponta a ponta).
- RBAC, ACLs e o modelo completo de autorização → **Curso 7, Aula 3**.
- CKU, sizing e custos por tipo de cluster → **Curso 7, Aula 4**.

**Perguntas para checar entendimento:**

1. Sua empresa tem `dev` e `prod`. Um pipeline novo precisa produzir num tópico de prod. Descreva a cadeia identidade → credencial → escopo correta.
2. Por que o secret ser exibido só uma vez é uma feature, não uma limitação?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Organization** | Nível raiz da conta: billing, usuários e configurações globais. |
| **Environment** | Fronteira lógica de isolamento (dev/staging/prod), cada uma com seu Schema Registry. |
| **Cluster** | Recurso onde tópicos e dados vivem; tem tipo, região e modelo de custo. |
| **Cluster types** | Basic, Standard, Enterprise (elásticos multi-tenant), Dedicated (provisionado, CKU) e Freight. |
| **CKU (Confluent Kafka Unit)** | Unidade de capacidade provisionada de clusters Dedicated. |
| **User Account** | Identidade humana: UI, MFA, pertence a uma pessoa. |
| **Service Account** | Identidade de aplicação, sem vínculo com pessoa. |
| **API Key (key + secret)** | Credencial de acesso; o secret é exibido apenas na criação. |
| **Escopo (scope)** | O recurso a que a key dá acesso: cluster, Schema Registry ou gestão Cloud. |
| **Rotação de credencial** | Prática de revogar e recriar keys periodicamente ou em incidente. |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Environments* — https://docs.confluent.io/cloud/current/access-management/hierarchy/cloud-environments.html
- Confluent Cloud — *Cluster Types* — https://docs.confluent.io/cloud/current/clusters/cluster-types.html
- Confluent Cloud — *Service Accounts* — https://docs.confluent.io/cloud/current/access-management/identity/service-accounts.html
- Confluent Cloud — *API Keys — Best Practices* — https://docs.confluent.io/cloud/current/access-management/authenticate/api-keys/best-practices-api-keys.html
- Confluent CLI — *Command Reference* — https://docs.confluent.io/confluent-cli/current/command-reference/index.html
