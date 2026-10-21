# Curso 2 · Aula 0 — Setup: conta, CLI e o contrato de custo

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~25 min · ☁️ **Cria recursos na nuvem** · pré-requisito dos Cursos 2 a 7

Esta aula monta o ambiente que **todo o resto do bootcamp** vai usar. Ao final você terá um environment, um cluster e a CLI apontada para eles — e, mais importante, o **hábito de teardown** que impede seus créditos de evaporarem.

---

## ⚠️ Leia antes de criar a conta

Duas coisas que economizam dinheiro e frustração:

1. **O relógio começa no signup.** Os créditos gratuitos têm validade em dias corridos. Crie a conta **quando for de fato começar** o hands-on — não semanas antes "para deixar pronto".
2. **Pode pedir cartão de crédito.** Dependendo da promoção vigente, o signup exige cartão mesmo com créditos. Não haverá cobrança enquanto houver saldo, mas saiba disso antes.

> 💡 Valores, planos e telas do Confluent Cloud mudam com frequência. Este guia cita
> ordens de grandeza; **confira sempre a página de Billing atual** da sua conta.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito o [Curso 1](../../curso1/README.md) | O vocabulário de tópico, partição e offset é assumido |
| E-mail (ou conta Google/GitHub) | Para o signup |
| Terminal | Para instalar a CLI |
| Docker | **Não** — a partir daqui é tudo nuvem |

---

## O que você vai saber fazer ao final

- Criar conta, environment e cluster no Confluent Cloud
- Instalar a CLI `confluent` e autenticar
- Apontar a CLI para o seu ambiente (para não repetir IDs em todo comando)
- Consultar seu saldo de créditos
- Executar o **ritual de teardown** — e saber o que pode ficar ligado

---

## Passo 1 — Criar a conta *(~5 min)*

1. Acesse [confluent.cloud/signup](https://www.confluent.io/confluent-cloud/tryfree/)
2. Cadastre-se por e-mail ou SSO (Google/GitHub)
3. Confirme o e-mail
4. Escolha o plano com **créditos gratuitos**

Depois de entrar, vá em **Billing & payment → Credits** e localize seu saldo.

**O que observar**: o valor disponível **e a data de validade**. Esse é o seu orçamento
para o bootcamp inteiro. Anote a data.

---

## Passo 2 — Criar environment e cluster pela UI *(~6 min)*

Faça este passo **pela interface** — é onde você vê o custo antes de confirmar.

### O environment

1. **Environments → Add cloud environment**
2. Nome: `bootcamp-dio`
3. Stream Governance: **Essentials** (inclui o Schema Registry que a Aula 3 usa)

### O cluster

1. Dentro do environment → **Create cluster**
2. Tipo: **Basic** — leia o painel de custo à direita antes de confirmar
3. Cloud e região: qualquer uma, mas **anote a escolha** e use a mesma para tudo
4. Nome: `bootcamp-basic`

**O que observar**: o cluster nasce em **segundos**. Você não dimensionou disco, não
escolheu número de brokers, não configurou réplica. Compare com o `docker compose` do
Curso 1 — e agora imagine fazer isso para 3 brokers em produção.

> 💡 **Sobre a região**: regiões dos EUA costumam ser mais baratas que `sa-east-1`
> (São Paulo), mas a latência é maior. Para estudo, o custo pesa mais. O que **não**
> pode é misturar — todos os recursos do bootcamp na mesma região.

---

## Passo 3 — Instalar e autenticar a CLI *(~5 min)*

```bash
# macOS
brew install confluentinc/tap/cli

# Linux / macOS (alternativa)
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest

# Windows: baixe em https://docs.confluent.io/confluent-cli/current/install.html
```

Confirme a versão — o bootcamp exige **v4 ou superior**:

```bash
confluent version
```

**Output esperado**
```
confluent - Confluent CLI

Version:     v4.35.0
```

Autentique:

```bash
confluent login --save
```

O `--save` guarda a sessão para você não repetir o login a cada comando.

---

## Passo 4 — Apontar a CLI para o seu ambiente *(~4 min)*

A CLI enxerga o que você criou na UI — são **duas portas para o mesmo lugar**:

```bash
confluent environment list
```

**Output esperado**
```
  Current |     ID     |     Name     | Stream Governance Package
----------+------------+--------------+---------------------------
          | env-537w6n | bootcamp-dio | ESSENTIALS
```

Copie o ID do seu environment (o seu será diferente) e selecione:

```bash
confluent environment use env-537w6n
```
```
Using environment env-537w6n.
```

Agora o cluster:

```bash
confluent kafka cluster list
```
```
  Current |     ID      |      Name      | Type  | Cloud |  Region   | Status |  Endpoint
----------+-------------+----------------+-------+-------+-----------+--------+----------------------------------------------------------
          | lkc-223x0gy | bootcamp-basic | BASIC | aws   | us-east-1 | UP     | SASL_SSL://pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092
```

Troque pelo **seu** `lkc-`, da lista acima:

```bash
confluent kafka cluster use lkc-223x0gy
```
```
Set Kafka cluster lkc-223x0gy as the active cluster for environment env-537w6n.
```

Confirme que o contexto ficou gravado — repare no `*`:

```bash
confluent environment list
confluent kafka cluster list
```
```
  Current |     ID     |     Name     | Stream Governance Package
----------+------------+--------------+---------------------------
  *       | env-537w6n | bootcamp-dio | ESSENTIALS

  Current |     ID      |      Name      | Type  | ... | Status
----------+-------------+----------------+-------+-----+--------
  *       | lkc-223x0gy | bootcamp-basic | BASIC | ... | UP
```

**O que observar**: o `*` na coluna `Current`. A partir daqui, os comandos do bootcamp
**não precisam repetir `--environment` e `--cluster`** — o contexto está configurado.

### Os prefixos de ID

Você vai ver esses prefixos o curso inteiro. Eles identificam o tipo do recurso:

| Prefixo | Recurso |
|---|---|
| `env-` | Environment |
| `lkc-` | Kafka cluster |
| `lsrc-` | Schema Registry cluster |
| `sa-` | Service account |
| `u-` | Usuário humano |

---

## Passo 5 — Anote o seu saldo *(~2 min)*

**🖱 Pela Console**: ☰ (canto superior direito) → **Billing & payment** — os créditos promocionais e o consumo até agora, na tela que você vai revisitar a cada teardown.

**⌨️ Pela CLI**:

```bash
confluent billing promo list
```

**Output esperado** (o seu vai diferir)
```
      Code     |      Balance       |  Expiration
---------------+--------------------+---------------
  ILW8KTWS     | $400.00/400.00 USD | Sep 17, 2026
```

E o consumo dos últimos dias:

```bash
confluent billing cost list \
  --start-date $(date -v-7d +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d)
```

> No Linux, troque `date -v-7d +%Y-%m-%d` por `date -d '7 days ago' +%Y-%m-%d`.

**O que observar**: com apenas um cluster Basic ocioso, os valores ficam próximos de
zero. É isso que torna o Basic seguro para estudo — **você paga pelo uso, não por
existir**.

---

## Passo 6 — O ritual de teardown *(~3 min)*

Este é o passo mais importante da aula. Não porque é difícil, mas porque **vira hábito
ou vira prejuízo**.

### A regra do bootcamp

> **Cluster Basic ocioso = ok.**
> **Conector e Flink statement ligados = crédito queimando.**

O Basic cobra por uso; conectores e compute pools de Flink cobram **por existirem**.
Esses você deleta ao fim de cada sessão, sempre.

### O checklist de fim de sessão

Ao terminar de estudar, rode:

**🖱 Pela Console**: as abas do cluster (**Connectors**, **Topics**, e **Flink** no environment) são o inventário visual do que está vivo. Na demo, mostre as abas; no dia a dia, o comando é mais rápido:

**⌨️ Pela CLI**:

```bash
# O que existe no meu environment?
confluent kafka cluster list
confluent kafka topic list

# Existe conector ligado? (a partir do Curso 3)
confluent connect cluster list

# Existe Flink consumindo? (a partir do Curso 4)
confluent flink compute-pool list
```

Se as duas últimas listas não estiverem vazias no fim da sessão, **você está pagando
por nada**.

### Pratique agora

Crie um tópico descartável e apague:

```bash
confluent kafka topic create teste-teardown --partitions 1
confluent kafka topic list
confluent kafka topic delete teste-teardown --force
confluent kafka topic list
```

**O que observar**: a lista volta vazia. Faça esse ciclo — criar, conferir, apagar,
conferir — virar reflexo.

---

## Passo 7 — Preparar o plugin do Flink *(~2 min, opcional)*

Adiantando o Curso 4 (nada será executado agora):

```bash
confluent plugin install confluent-flink-quickstart
```

Se o comando não existir na sua versão, ignore — o Curso 4 retoma isso.

---

## O que fica de pé

Ao contrário das outras aulas, **aqui você não destrói nada ao final**. Este é o
tabuleiro do bootcamp:

| Recurso | Por quê |
|---|---|
| Environment `bootcamp-dio` | Contém todo o resto |
| Cluster `bootcamp-basic` | Usado dos Cursos 2 ao 7 |
| Schema Registry (Essentials) | Provisionado junto com o environment; a Aula 3 usa |
| Contexto da CLI | Para não repetir IDs |

**Nenhuma API key foi criada** — e isso é deliberado. Criar uma key "global só pra
testar" é o hábito que a Aula 2 vai te ensinar a não ter.

---

## Se algo não funcionou

**`confluent: command not found`**
A instalação não entrou no PATH. Reabra o terminal ou confira o caminho do binário.

**`Error: not logged in`**
Rode `confluent login --save`. Se a sessão expirar no meio do curso, é só repetir.

**`confluent kafka cluster list` retorna vazio**
Você não selecionou o environment. Rode `confluent environment use env-xxxxx`.

**O cluster fica em `PROVISIONING`**
Normal — leva até 5 minutos. Acompanhe com:
```bash
confluent kafka cluster describe lkc-xxxxx
```

**Não sei se estou gastando crédito**
`confluent billing cost list` com um intervalo de datas mostra o consumo real. Com um
Basic ocioso, deve ficar perto de zero.

---

## Glossário

| Termo | Definição |
|---|---|
| **Environment** | Agrupador lógico de recursos (`env-`); contém clusters e o Schema Registry. |
| **Cluster Basic** | Cluster gerenciado que cobra por uso, sem custo fixo — o do bootcamp. |
| **Stream Governance Essentials** | Pacote que inclui Schema Registry e Stream Lineage. |
| **Créditos promocionais** | Saldo gratuito com **validade em dias corridos** a partir do signup. |
| **Teardown** | Ritual de destruir recursos ao fim da sessão para não queimar crédito. |
| **Contexto da CLI** | Environment e cluster ativos (`use`), que evitam repetir IDs. |

---

## Para ir além

| Conceito | Volta em |
|---|---|
| Hierarquia de recursos e IDs | Aula 2 — em profundidade |
| Service accounts e API keys | Aula 2 |
| Schema Registry | Aula 3 |
| Conectores (o vilão do teardown) | Curso 3 |
| Compute pools de Flink | Curso 4 |

**Fontes**

- Confluent — [Get started with Confluent Cloud](https://docs.confluent.io/cloud/current/get-started/index.html)
- Confluent — [CLI installation](https://docs.confluent.io/confluent-cli/current/install.html)
- Confluent — [Billing and payment](https://docs.confluent.io/cloud/current/billing/overview.html)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Curso 1 · Aula 6](../../curso1/aula6/README.md) | [Curso 2](../README.md) | [Aula 1 · O ecossistema Confluent](../aula1/README.md) |
