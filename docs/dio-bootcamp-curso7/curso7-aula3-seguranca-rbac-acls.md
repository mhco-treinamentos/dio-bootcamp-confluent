# Curso 7 · Aula 3 — Segurança em Produção: RBAC, ACLs e Service Accounts na Prática

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Curso 2, Aula 2 (identidades e API keys) + Aula 1 do Curso 7

---

## 1. Objetivo da Aula

O segundo pilar: **só entra quem deve — e fica registrado**. Separar **autenticação de autorização**, dissecar os dois modelos de autorização do Confluent Cloud — **RBAC** (papéis sobre a hierarquia) e **ACLs** (regras granulares nativas do Kafka) — e praticar o princípio que governa tudo: **menor privilégio**, com a anatomia de um **role binding** de verdade.

---

## 2. Conceitos Simplificados

### 2.1 Autenticação × autorização: as duas perguntas *(~3 min)*

```
 ① AUTENTICAÇÃO — "quem é você?"
    identidades (user, service account) + credenciais (API keys, SSO/OAuth)
    → o Curso 2 resolveu esta metade
 ② AUTORIZAÇÃO — "o que VOCÊ pode fazer?"
    RBAC e ACLs
    → a metade desta aula
```

- O estado do bootcamp até aqui, dito com honestidade: nossas service accounts autenticam bem, mas foram usadas com permissões largas — **aceitável em sandbox, inaceitável em produção**. Hoje fechamos essa porta
- E a terceira pergunta, que fecha o trio da confiança: *"o que você FEZ?"* — **audit logs**: cada ação de gestão registrada (quem criou a key, quem mudou o binding) — a resposta para o auditor e para o incidente

### 2.2 RBAC: papéis sobre a hierarquia *(~5 min)*

O **Role-Based Access Control** dá permissões por **papel + escopo na hierarquia** (a hierarquia do Curso 2 trabalhando de novo):

```
 ROLE BINDING = ① principal (quem)  +  ② role (qual papel)  +  ③ recurso (onde)

 exemplo:  service account app-pedidos
           + DeveloperWrite
           + tópico `pedidos` do cluster lkc-xxx
           = "esta aplicação PRODUZ neste tópico. E só."
```

Os papéis que resolvem o dia a dia (a escada do menor ao maior):

| Role | O que permite | Uso típico |
|---|---|---|
| **DeveloperRead** | Consumir de recursos específicos | O consumidor; o time que só lê |
| **DeveloperWrite** | Produzir em recursos específicos | O producer; o conector source |
| **DeveloperManage** | Criar/gerir recursos específicos (sem dados) | Automação de provisionamento |
| **ResourceOwner** | Tudo sobre recursos específicos, incluindo conceder acesso | O dono do domínio/tópico |
| **CloudClusterAdmin** | Administra um cluster | Plataforma/DevOps |
| **EnvironmentAdmin / OrganizationAdmin** | Administra env / a organização | Pouquíssimas pessoas — e auditadas |

- **Escopo herda pela hierarquia**: binding no environment vale para os clusters dele; no cluster, para os tópicos dele — e dá para escopar até o **prefixo** (`pedidos-*`): o padrão de times por domínio
- A leitura de arquiteto: RBAC transforma autorização em **modelo organizacional** — papéis espelham funções (quem desenvolve, quem opera, quem é dono), não permissões avulsas

### 2.3 ACLs: a granularidade nativa do Kafka *(~4 min)*

As **ACLs** (Access Control Lists) são o mecanismo histórico do protocolo Kafka: regras `principal + operação + recurso + allow/deny`:

| | **RBAC** | **ACLs** |
|---|---|---|
| Modelo | Papéis sobre a hierarquia do CC | Regras por operação (READ, WRITE, DESCRIBE...) |
| Granularidade | Papel (pacote de permissões) | Operação a operação |
| Gestão em escala | **Melhor** — poucos bindings dizem muito | Explode em centenas de regras |
| Portabilidade | Confluent | **Protocolo Kafka** (OSS/MSK também falam ACL) |
| Quando usar | **O padrão no Confluent Cloud** | Casos finos que o papel não expressa; legado/ferramentas que falam ACL |

- A recomendação prática do curso: **RBAC como regra, ACL como exceção documentada** — e nunca os dois modelos misturados no mesmo recurso sem critério (a confusão de "por que essa app acessa isso?" nasce aí)
- O aluno que entende ACL também conversa com o mundo OSS/MSK — o vocabulário é do protocolo, não do produto

### 2.4 O princípio que governa: menor privilégio *(~4 min)*

A disciplina, em quatro regras práticas:

1. **Uma service account por aplicação, um papel por necessidade** — `app-pedidos` produz em `pedidos`; ela **não** lê `clientes` "porque um dia pode precisar"
2. **Ninguém opera com admin** — admin é para administrar; o dado flui por Developer*. (A key global "pra testar" do Curso 2 morre oficialmente aqui)
3. **Prefixos espelham domínios** — `fraude-*` para o time de fraude: o binding conta a organização
4. **Revisão é rotina** — bindings de gente que saiu, apps desligadas, permissões largas de emergência que ficaram: a auditoria trimestral que ninguém faz e todo incidente cobra

E o fecho com o Curso 6: **agentes de IA são principals também** — o acesso do RTCE/MCP passa pelo mesmo modelo (RBAC + audit), e menor privilégio para um agente que **age** não é higiene: é contenção de dano.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Base: uma service account nova, "zerada". Tempo de tela: ~7 min.

**O que a demo deve mostrar, nesta ordem:**

1. **A SA sem permissão tenta e falha** — criar `app-relatorios` + API key; tentar consumir `pedidos` → **erro de autorização** (não de autenticação — apontar a diferença na mensagem).
   - **Observação-alvo:** autenticado ≠ autorizado — as duas perguntas da seção 2.1, separadas na prática. O default é **negar**.

2. **O role binding mínimo** — conceder `DeveloperRead` **escopado ao tópico `pedidos`**; repetir o consumo → funciona.
   - **Observação-alvo:** a anatomia principal + role + recurso viva — e o escopo cirúrgico: a mesma SA ainda **não lê** outros tópicos (provar tentando).

3. **O prefixo em ação** — um binding com escopo de prefixo (`pedidos-*`) e a demonstração de que os tópicos derivados (C4/C5) entram no pacote.
   - **Observação-alvo:** o padrão de domínio — um binding governando a família de recursos do time.

4. **Listar e auditar** — a lista de role bindings da organização (quem pode o quê, onde) e uma olhada no **audit log** (a criação do binding registrada: quem, quando).
   - **Observação-alvo:** a resposta pronta para o auditor — e para o "quem deu esse acesso?" do incidente.

5. **Teardown** — revogar a key e remover os bindings de demo (a revisão da regra 4, praticada).

**Fechamento na tela** *(~1 min)*: "portas trancadas, chaves nominais, registro de tudo. Terceiro pilar: a conta — quanto isso custa e onde otimizar. Próxima aula."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Erro 401/403 é tudo 'problema de acesso'."** Autenticação falhou (credencial) ≠ autorização negada (permissão) — diagnósticos e correções diferentes. A cena 1 da demo existe para separar os dois na cabeça do aluno.
- **"Dou admin agora e ajusto depois."** "Depois" não existe — o acesso largo de emergência vira permanente e invisível. Menor privilégio custa 2 minutos a mais no dia 1 e economiza o incidente do dia 300.
- **"RBAC e ACL, uso os dois em tudo."** Um modelo por recurso, com critério — a mistura sem doutrina é onde nasce o "ninguém sabe por que essa app acessa isso".
- **"Segurança trava o time."** Bindings por prefixo + papéis Developer* dão autonomia **dentro do domínio** — o time se move rápido no que é dele. Segurança boa é invisível para quem está no lugar certo.
- **Escopo:** OAuth/identity providers, mTLS e private networking são o degrau enterprise — citar que existem (e que o comparativo do Curso 2 os mencionou), apontar a doc.

**Ganchos com a trilha:**

- Identidades e API keys → **Curso 2, Aula 2** (a metade da autenticação).
- O acesso de agentes via RTCE/MCP → **Curso 6, Aula 4** (principals não-humanos com poder de ação).
- Anonimização, LGPD e a visão regulatória → **Live 3** (o complemento desta aula).
- No **Desafio Final**, o README deve declarar o modelo de acesso do pipeline — service accounts e papéis por camada.

**Perguntas para checar entendimento:**

1. O time de fraude precisa: consumir `pedidos`, produzir em `fraude-alertas` e criar tópicos `fraude-*`. Desenhe os bindings mínimos (principal, role, escopo) — quantos são?
2. Uma API key vazou no log de CI. Qual a sequência de resposta — e o que o audit log responde na investigação?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Autenticação (authn)** | Provar quem você é — identidade + credencial. |
| **Autorização (authz)** | O que a identidade pode fazer — RBAC/ACLs. |
| **RBAC (Role-Based Access Control)** | Autorização por papéis com escopo na hierarquia de recursos. |
| **Role binding** | A tripla principal + role + recurso — a unidade do RBAC. |
| **Principal** | A identidade do binding: user, service account — ou o agente (C6). |
| **DeveloperRead/Write/Manage · ResourceOwner** | A escada de papéis do dia a dia, do menor ao dono. |
| **Escopo por prefixo** | Binding sobre uma família de recursos (`pedidos-*`) — o padrão de domínio. |
| **ACL (Access Control List)** | Regras granulares por operação, nativas do protocolo Kafka. |
| **Menor privilégio** | Conceder exatamente o necessário — a doutrina da aula. |
| **Audit log** | O registro de quem fez o quê na gestão da plataforma. |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *RBAC (papéis e escopos)* — https://docs.confluent.io/cloud/current/security/access-control/rbac/overview.html
- Confluent Cloud — *Predefined RBAC Roles* — https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html
- Confluent Cloud — *ACLs* — https://docs.confluent.io/cloud/current/security/access-control/acl.html
- Confluent Cloud — *Audit Logs* — https://docs.confluent.io/cloud/current/monitoring/audit-logging/cloud-audit-log-concepts.html
- Apache Kafka — *Security: Authorization and ACLs* — https://kafka.apache.org/documentation/#security_authz
