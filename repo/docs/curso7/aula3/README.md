# Curso 7 · Aula 3 — Segurança: autenticado ≠ autorizado

> **Guia do aluno** · Bloco 3 — Operação
> ⏱ ~45 min · ☁️ **Custo ≈ zero** (só ACLs e uma service account) · 1 terminal

Você vai criar uma identidade que **se autentica perfeitamente** e mesmo assim não
consegue ler nada. Depois vai abrir a porta certa — e só ela.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Curso 2, Aula 2 | Service accounts e API keys |
| Tópicos com dados | `pg-payments.*` do Curso 3 |
| 1 terminal com `confluent` logado | |

> 💡 Se você fez teardown do Curso 3, qualquer tópico com dados serve. Substitua os
> nomes nos comandos.

---

## O que você vai saber fazer ao final

- Separar **erro de autenticação** de **erro de autorização** na mensagem
- Criar uma service account de menor privilégio, do zero
- Conceder ACLs cirúrgicas — e provar que o escopo é mesmo cirúrgico
- Usar **prefixo** para governar uma família de tópicos com uma regra
- Saber quando RBAC funciona e quando ele **não está disponível**

---

## Os conceitos em 90 segundos

Duas perguntas diferentes, que muita gente confunde:

```
① AUTENTICAÇÃO — "quem é você?"      → API key, SSO      (Curso 2 resolveu)
② AUTORIZAÇÃO  — "o que você pode?"  → RBAC, ACLs        (esta aula)
```

E dois modelos para responder a segunda:

| | **RBAC** | **ACLs** |
|---|---|---|
| Modelo | Papéis sobre a hierarquia | Regra por operação |
| Portabilidade | Confluent | **Protocolo Kafka** (OSS, MSK) |
| Escala | Poucos bindings dizem muito | Explode em centenas de regras |

> ⚠️ **A regra que muda esta aula**: no cluster **Basic**, RBAC escopado a tópico
> **não funciona**. Você vai ver o erro no Passo 3. O caminho é ACL — e por isso a
> aula inteira é ACL.

> 📖 A escada de papéis (`DeveloperRead` → `ResourceOwner` → `OrganizationAdmin`) e as
> quatro regras do menor privilégio estão **na aula gravada**.

---

## Passo 1 — Uma identidade zerada *(~6 min)*

**🖱 Pela Console**: ☰ → **Accounts & access** → **Service accounts** → **+ Add service account** (`app-relatorios`); a key em **API Keys** do cluster, dono = a service account.

**⌨️ Pela CLI**:

```bash
confluent iam service-account create "app-relatorios" \
  --description "Consumidor somente-leitura de relatorios"
```

**Output esperado**
```
+-------------+--------------------------------+
| ID          | sa-zgv3vdz                     |
| Name        | app-relatorios                 |
| Description | Consumidor somente-leitura de  |
|             | relatorios                     |
+-------------+--------------------------------+
```

```bash
export SA=$(confluent iam service-account list -o json | python3 -c "import json,sys; print([s['id'] for s in json.load(sys.stdin) if s['name']=='app-relatorios'][0])")
export CLUSTER=$(confluent kafka cluster list -o json | python3 -c "import json,sys;print(json.load(sys.stdin)[0]['id'])")
```

Agora a credencial:

```bash
confluent api-key create --resource $CLUSTER --service-account $SA \
  --description "app-relatorios" -o json
```

**Output esperado**
```json
{
  "api_key": "SUA-API-KEY",
  "api_secret": "cflt-SEU-SECRET-AQUI"
}
```

```bash
export AK="SUA-API-KEY"
export AS="cflt..."
```

**O que observar**: esta service account **acabou de nascer sem nenhuma permissão**. O
default do Confluent Cloud é **negar**. É o oposto do que muita ferramenta faz — e é o
default certo.

---

## Passo 2 — Autenticado, e mesmo assim barrado *(~7 min)*

```bash
confluent kafka topic consume pg-payments.payments.transactions \
  --api-key $AK --api-secret "$AS" \
  --group relatorios-teste --from-beginning
```

**Output esperado**
```
Error: topic "pg-payments.payments.transactions" does not exist or user does not
have the ACLs or role bindings required to describe it

Suggestions:
    To list topics for Kafka cluster lkc-9kwm1py, use confluent kafka topic list --cluster lkc-9kwm1py.
    To list ACLs use confluent kafka acl list --cluster lkc-9kwm1py.
    To list role bindings use confluent iam rbac role-binding list.
```

**O que observar — e isto é o coração da aula:**

A credencial **funcionou**. Você não recebeu "invalid credentials" nem "authentication
failed". O broker sabe exatamente quem você é. Ele apenas decidiu não te contar nada.

Note a redação: *"does not exist **or** user does not have the ACLs"*. Não é vagueza —
é **proteção deliberada**. Se o broker dissesse "o tópico existe, mas você não pode
lê-lo", ele estaria vazando a existência do tópico para quem não deveria saber. Um
principal sem permissão vê um universo vazio.

```
authentication falhou  → credencial errada       → verifique a key
authorization negada   → credencial certa, sem permissão → verifique a ACL
```

Diagnósticos diferentes, correções diferentes. Confundir os dois é passar uma hora
regenerando uma key que estava perfeita.

---

## Passo 3 — O erro que define o caminho *(~5 min)*

O instinto seria RBAC. Tente:

```bash
export ENV=$(confluent environment list -o json | python3 -c "import json,sys;print([e for e in json.load(sys.stdin) if e['is_current']][0]['id'])")

confluent iam rbac role-binding create \
  --principal User:$SA --role DeveloperRead \
  --environment $ENV \
  --cloud-cluster $CLUSTER --kafka-cluster $CLUSTER \
  --resource "Topic:pg-payments.payments.transactions"
```

**Output esperado**
```
Error: Forbidden - Basic Clusters can not use resource roles
```

**O que observar**: RBAC **escopado a um recurso** (um tópico específico) é feature de
cluster **Standard ou superior**. No Basic, os role bindings existem só no nível de
organização e environment.

Não é limitação de segurança — é de **produto**. E é bom você descobrir isso aqui, num
lab, e não no dia em que precisa dar acesso a alguém.

| Cluster | RBAC escopado a tópico | ACLs |
|---|---|---|
| **Basic** | ❌ | ✅ |
| Standard, Enterprise, Dedicated | ✅ | ✅ |

**ACL é o mecanismo do protocolo Kafka** — funciona em qualquer cluster, em qualquer
distribuição, inclusive OSS e MSK. É o denominador comum.

---

## Passo 4 — A ACL cirúrgica *(~8 min)*

Um consumidor precisa de **duas** permissões, em **dois** recursos diferentes:

```bash
confluent kafka acl create --allow --service-account $SA \
  --operations read,describe \
  --topic pg-payments.payments.transactions --cluster $CLUSTER
```

**Output esperado**
```
     Principal    | Permission | Operation | Resource Type |           Resource Name           | Pattern Type
------------------+------------+-----------+---------------+-----------------------------------+---------------
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | TOPIC         | pg-payments.payments.transactions | LITERAL
  User:sa-zgv3vdz | ALLOW      | READ      | TOPIC         | pg-payments.payments.transactions | LITERAL
```

```bash
confluent kafka acl create --allow --service-account $SA \
  --operations read,describe \
  --consumer-group relatorios- --prefix --cluster $CLUSTER
```

**Output esperado**
```
     Principal    | Permission | Operation | Resource Type | Resource Name | Pattern Type
------------------+------------+-----------+---------------+---------------+---------------
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | GROUP         | relatorios-   | PREFIXED
  User:sa-zgv3vdz | ALLOW      | READ      | GROUP         | relatorios-   | PREFIXED
```

> ⚠️ **O erro nº 1 de ACL para consumidor**: conceder só no tópico e esquecer o
> consumer group. O consumidor lê e trava no commit, com um erro que parece não ter
> nada a ver com permissão.

Agora repita o consumo do Passo 2:

```bash
confluent kafka topic consume pg-payments.payments.transactions \
  --api-key $AK --api-secret "$AS" \
  --group relatorios-teste --from-beginning
```

**Output esperado**
```
Starting Kafka Consumer. Use Ctrl-C to exit.
  ...H89a2688b-12c1-46e0-9998-5f15ff002d4d...CREDIT 4121 PIX SETTLED
  2026-06-16T12:08:42.000000Z ... postgresql pg-payments ... neondb payments transactions
```

**O que observar**: os bytes vêm ilegíveis porque é Avro binário (Curso 2, Aula 5), mas
os dados chegaram. Mesma credencial do Passo 2 — mudou só a ACL.

---

## Passo 5 — Prove que o escopo é mesmo cirúrgico *(~7 min)*

Uma permissão que você não testou é uma permissão que você não conhece. Tente **outro**
tópico:

```bash
confluent kafka topic consume pg-payments.payments.accounts \
  --api-key $AK --api-secret "$AS" \
  --group relatorios-teste2 --from-beginning
```

**Output esperado**
```
Error: topic "pg-payments.payments.accounts" does not exist or user does not have
the ACLs or role bindings required to describe it
```

**Barrado.** A ACL do Passo 4 é `LITERAL` — vale para **aquele nome exato**, e nada
mais.

Agora tente **escrever** no tópico que ela pode ler:

```bash
echo '{"teste":1}' | confluent kafka topic produce pg-payments.payments.transactions \
  --api-key $AK --api-secret "$AS"
```

**Output esperado**
```
failed to produce offset -1: Broker: Topic authorization failed
```

**O que observar**: `READ` e `WRITE` são operações **independentes**. Conceder leitura
não concede escrita nem por acidente. Este é o menor privilégio funcionando de verdade
— e você acabou de **provar**, não de assumir.

---

## Passo 6 — Um prefixo governa uma família *(~6 min)*

Conceder tópico a tópico não escala. O padrão de produção é **prefixo por domínio**:

```bash
confluent kafka acl create --allow --service-account $SA \
  --operations read,describe \
  --topic pg-payments --prefix --cluster $CLUSTER
```

**Output esperado**
```
     Principal    | Permission | Operation | Resource Type | Resource Name | Pattern Type
------------------+------------+-----------+---------------+---------------+---------------
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | TOPIC         | pg-payments   | PREFIXED
  User:sa-zgv3vdz | ALLOW      | READ      | TOPIC         | pg-payments   | PREFIXED
```

Agora o tópico que estava barrado no Passo 5:

```bash
confluent kafka topic consume pg-payments.payments.accounts \
  --api-key $AK --api-secret "$AS" \
  --group relatorios-teste3 --from-beginning
```

**Output esperado**
```
Starting Kafka Consumer. Use Ctrl-C to exit.
  ...H16419f82-8b9d-4434-a465-e150bd9c66b3...967518121404420
```

Funciona — e funcionará para **todo tópico futuro** que comece com `pg-payments`.

**O que observar**: `PREFIXED` é o que transforma ACL de burocracia em modelo
organizacional. Uma regra que diz *"o time de relatórios lê o domínio de pagamentos"* —
sem precisar de manutenção quando o time criar o próximo tópico.

O inventário completo:

```bash
confluent kafka acl list --service-account $SA --cluster $CLUSTER
```

**Output esperado**
```
     Principal    | Permission | Operation | Resource Type |           Resource Name           | Pattern Type
------------------+------------+-----------+---------------+-----------------------------------+---------------
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | GROUP         | relatorios-                       | PREFIXED
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | TOPIC         | pg-payments                       | PREFIXED
  User:sa-zgv3vdz | ALLOW      | DESCRIBE  | TOPIC         | pg-payments.payments.transactions | LITERAL
  User:sa-zgv3vdz | ALLOW      | READ      | GROUP         | relatorios-                       | PREFIXED
  User:sa-zgv3vdz | ALLOW      | READ      | TOPIC         | pg-payments                       | PREFIXED
  User:sa-zgv3vdz | ALLOW      | READ      | TOPIC         | pg-payments.payments.transactions | LITERAL
```

**Repare na redundância**: a ACL `LITERAL` do Passo 4 virou supérflua — o `PREFIXED` já
a cobre. Em produção, isso é exatamente o entulho que a revisão trimestral remove.
Guarde a observação: ela é o Passo 7.

---

## Passo 7 — Quem tem as chaves *(~4 min)*

**🖱 Pela Console**: **Accounts & access** → aba **Access** — a árvore de role bindings da organização, navegável por environment/cluster. A resposta visual de "quem pode o quê".

**⌨️ Pela CLI**:

```bash
confluent iam rbac role-binding list --current-user
```

**Output esperado**
```
     ID     |   Principal    |        Email        |       Role        | Environment | ...
------------+----------------+---------------------+-------------------+-------------+----
  rb-l9q8W5 | User:u-97x6685 | voce@exemplo.com    | OrganizationAdmin |             |
```

**O que observar — e é desconfortável de propósito**: você operou o bootcamp inteiro
como `OrganizationAdmin`. Aceitável num sandbox de estudo. **Inaceitável** em produção,
onde a regra é: *admin administra; dados fluem por identidades Developer\*.*

E o audit log, que responde a terceira pergunta ("o que você **fez**?"):

```bash
confluent audit-log describe
```

**Output esperado**
```
+-----------------+----------------------------+
| Cluster         | lkc-0onyvp                 |
| Environment     | env-ywm1op                 |
| Service Account | sa-oq399zy                 |
| Topic Name      | confluent-audit-log-events |
+-----------------+----------------------------+
```

**O que observar**: o audit log é **um tópico Kafka**, num cluster separado que a
Confluent gerencia. Cada ACL que você criou nesta aula está lá, com autor e timestamp.
Ele é consumível como qualquer outro tópico — e é a resposta pronta para *"quem deu
esse acesso?"* no dia do incidente.

---

## Passo 8 — Teardown *(~3 min)*

A regra 4 do menor privilégio — revisão é rotina — praticada agora:

```bash
confluent kafka acl delete --allow --service-account $SA \
  --operations read,describe --topic pg-payments.payments.transactions \
  --cluster $CLUSTER --force

confluent kafka acl delete --allow --service-account $SA \
  --operations read,describe --topic pg-payments --prefix --cluster $CLUSTER --force

confluent kafka acl delete --allow --service-account $SA \
  --operations read,describe --consumer-group relatorios- --prefix \
  --cluster $CLUSTER --force
```

**Output esperado** (uma linha por comando)
```
Deleted 2 ACLs.
```

```bash
confluent api-key delete $AK --force
confluent iam service-account delete $SA --force
```

**Validação — deve vir vazio:**
```bash
confluent kafka acl list --service-account $SA --cluster $CLUSTER
```
```
None found.
```

> 💡 **Ordem importa**: ACLs → API key → service account. Apagar a SA primeiro deixa
> ACLs órfãs apontando para um principal que não existe mais — entulho invisível que
> ninguém remove depois.

---

## Passo 9 — Fixe os conceitos *(~5 min)*

1. Por que o erro diz "não existe **ou** você não tem permissão"?
2. O time de fraude precisa: ler `pg-payments.*`, escrever em `fraude-alertas` e criar
   tópicos `fraude-*`. Quantos comandos de ACL?
3. Uma API key vazou no log do CI. Qual a sequência de resposta?
4. Por que você não conseguiu usar RBAC nesta aula?

<details>
<summary><b>Gabarito (clique para abrir)</b></summary>

**1. Para não vazar a existência do recurso.** Se a mensagem distinguisse os dois
casos, qualquer principal poderia mapear todos os tópicos do cluster tentando lê-los um
a um e lendo a diferença nas mensagens de erro. A ambiguidade é a proteção —
inconveniente para você, essencial contra reconhecimento.

**2. Quatro comandos:**
```bash
# ler o domínio de pagamentos
confluent kafka acl create --allow --service-account $SA \
  --operations read,describe --topic pg-payments --prefix

# o consumer group (sempre esquecido)
confluent kafka acl create --allow --service-account $SA \
  --operations read,describe --consumer-group fraude- --prefix

# escrever nos alertas
confluent kafka acl create --allow --service-account $SA \
  --operations write,describe --topic fraude-alertas

# criar tópicos do próprio domínio
confluent kafka acl create --allow --service-account $SA \
  --operations create,describe --topic fraude- --prefix
```
O que **não** aparece: nenhuma permissão de escrita em `pg-payments`. O time lê o
domínio de pagamentos e escreve só no seu.

**3. Nesta ordem:**
1. **Revogar a key imediatamente** (`confluent api-key delete`) — antes de investigar.
   Chave viva é dano em curso.
2. **Criar uma nova** e atualizar o segredo no CI.
3. **Consultar o audit log**: o que essa key acessou desde o commit? Consumo de dado
   sensível muda a resposta de "trocamos a senha" para "houve incidente de dados".
4. **Perguntar o que a key podia fazer**. Se era `DeveloperRead` num tópico, o dano é
   limitado. Se era admin, é incidente de segurança com escopo total. **A resposta ao
   vazamento é decidida no dia em que a permissão foi concedida** — este é o argumento
   inteiro do menor privilégio.

**4. Porque o cluster é Basic**: `Forbidden - Basic Clusters can not use resource
roles`. RBAC escopado a recurso exige Standard ou superior. ACLs funcionam em todos os
tiers, porque são do protocolo Kafka e não do produto Confluent.

</details>

---

## Se algo não funcionou

**Consumo falha mesmo com ACL no tópico**
Falta a ACL do **consumer group**. É o erro mais comum.

**`Error: Forbidden - Basic Clusters can not use resource roles`**
Esperado. Use ACL (Passo 4).

**`Broker: Topic authorization failed` ao produzir**
Também esperado no Passo 5 — `READ` não inclui `WRITE`.

**`Deleted 0 ACLs`**
Os filtros do delete precisam bater exatamente com os do create — inclusive
`--prefix`. Confira com `confluent kafka acl list` antes.

**A SA continua na lista após o delete**
Propagação leva alguns segundos. Rode de novo.

---

## Glossário

| Termo | Definição |
|---|---|
| **Autenticação (authn)** | Provar quem você é — identidade + credencial. |
| **Autorização (authz)** | O que a identidade pode fazer — RBAC/ACLs. |
| **ACL** | Regra `principal + operação + recurso` do protocolo Kafka. |
| **LITERAL × PREFIXED** | Nome exato × família de nomes por prefixo. |
| **Role binding** | A tripla principal + role + recurso — a unidade do RBAC. |
| **Resource role** | RBAC escopado a um recurso — exige Standard+. |
| **Menor privilégio** | Conceder exatamente o necessário, e nada mais. |
| **Audit log** | Tópico Kafka com o registro de cada ação de gestão. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| Service accounts e API keys | [Curso 2 · Aula 2](../../curso2/aula2/README.md) |
| Custo do cluster Standard vs Basic | [Aula 4](../aula4/README.md) |
| Cluster link como principal | [Aula 5](../aula5/README.md) |

**Fontes**

- Confluent — [ACLs](https://docs.confluent.io/cloud/current/security/access-control/acl.html)
- Confluent — [Predefined RBAC Roles](https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html)
- Confluent — [Audit Logs](https://docs.confluent.io/cloud/current/monitoring/audit-logging/cloud-audit-log-concepts.html)
- Apache Kafka — [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 2 · Observabilidade](../aula2/README.md) | [Curso 7](../README.md) | [Aula 4 · Custos](../aula4/README.md) |
