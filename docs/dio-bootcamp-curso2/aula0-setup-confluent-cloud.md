# Aula 0 — Setup do Ambiente: Confluent Cloud, CLI e Créditos em 20 Minutos

> **Bootcamp Confluent Cloud 2026 · Bloco 2 (pré-requisito dos Cursos 2–7)**
> Duração alvo: ~10–15 min · Vídeo curto de setup, 100% mão na massa guiada

---

## 1. Objetivo da Aula

Deixar o aluno com o ambiente pronto para todo o hands-on do bootcamp: conta no Confluent Cloud criada, **créditos gratuitos ativos**, **CLI `confluent` instalada e autenticada**, primeiro environment e cluster de pé — e, tão importante quanto, a **disciplina de teardown** para os créditos durarem o bootcamp inteiro.

---

## 2. Conceitos Simplificados

*Aula operacional — conceito mínimo, só o necessário para os passos fazerem sentido. A teoria da hierarquia de recursos é a Aula 2 do Curso 2.*

- **Confluent Cloud** — o Kafka (e o ecossistema) como serviço gerenciado: você não sobe broker, não configura disco; cria recursos lógicos e usa
- **Créditos gratuitos** — novas contas recebem **US$ 400 para usar nos primeiros 30 dias**; é o orçamento do aluno no bootcamp. Cluster Basic parado custa perto de zero, mas recursos esquecidos ligados (conectores, Flink) queimam crédito — daí o teardown
- **CLI `confluent`** — a ferramenta de linha de comando; todo o hands-on do bootcamp alterna entre a UI (visão) e a CLI (repetibilidade)
- **Teardown** — o ritual de fim de sessão: destruir o que não precisa persistir. No bootcamp: **cluster pode ficar (Basic ocioso ≈ zero), conectores e statements Flink NUNCA ficam ligados**

---

## 3. Roteiro de Tela — Especificação

> Passo a passo que o vídeo mostra na íntegra. O aluno executa junto.

1. **Criar a conta** — signup no Confluent Cloud (e-mail ou SSO Google/Github), confirmar e-mail, escolher o plano com créditos gratuitos.
   - **Observação-alvo:** onde os **créditos e a validade de 30 dias** aparecem na UI (Billing) — o aluno precisa saber monitorar o próprio saldo.

2. **Criar o primeiro environment e cluster pela UI** — environment `bootcamp-dio` (mantendo o pacote de Stream Governance **Essentials**, o incluído — o que ele significa é assunto do Curso 2), cluster **Basic**, cloud e região próximas (ex.: AWS `sa-east-1` ou `us-east-1` — mencionar o trade-off preço × latência).
   - **Observação-alvo:** o cluster Basic nasce em segundos e o custo por uso aparece na tela de criação — sem susto de infraestrutura.

3. **Instalar a CLI `confluent`** — instalação por sistema (macOS/Linux/Windows), `confluent version`, `confluent login`.
   - **Observação-alvo:** a CLI autenticada enxerga o environment e o cluster criados na UI — são as duas portas do mesmo lugar.

4. **Apontar a CLI para o ambiente** — selecionar environment e cluster default (`confluent environment use`, `confluent kafka cluster use`).
   - **Observação-alvo:** os comandos seguintes do bootcamp não precisam repetir IDs — contexto configurado uma vez.

5. **Instalar o plugin `flink quickstart`** — preparo antecipado do Curso 4 (mencionar que será usado lá; não executar nada de Flink agora).

6. **O ritual de teardown** — demonstrar explicitamente: listar o que existe, apagar um recurso de teste, conferir no Billing que nada ficou consumindo.
   - **Observação-alvo:** o hábito. Fechar TODA sessão de estudo com "o que deixei ligado?". Regra do bootcamp na tela: **conector e Flink statement ligados = crédito queimando; cluster Basic ocioso = ok**.

**Fechamento** *(~1 min)*: "ambiente pronto, créditos monitorados, CLI no jeito. No Curso 2 entendemos o que acabamos de criar — e produzimos a primeira mensagem real."

---

## 4. Pontos de Atenção

- **Cartão de crédito**: o signup pode pedir cartão dependendo da promoção vigente — avisar o aluno com antecedência e reforçar que os créditos cobrem o bootcamp inteiro se houver disciplina de teardown.
- **Região importa duas vezes**: preço (regiões dos EUA costumam ser mais baratas que `sa-east-1`) e latência de CLI/UI. Para o bootcamp, qualquer uma serve; consistência é o que importa (todos os recursos no mesmo lugar).
- **A validade dos 30 dias** é o relógio do bootcamp — recomendar criar a conta quando for de fato começar o hands-on, não semanas antes.
- **Nunca compartilhar API keys/secrets** — serão criadas no Curso 2 com escopo correto; na Aula 0 não se cria key nenhuma (evitar o hábito de key global "só pra testar").
- **Valores e telas mudam**: créditos, planos e UI do Confluent Cloud evoluem — o vídeo deve citar "confira a página atual de billing" em vez de cravar valores como eternos.

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Confluent Cloud** | Plataforma de data streaming totalmente gerenciada baseada em Apache Kafka. |
| **Environment** | Agrupador lógico de recursos (clusters, schemas) — detalhado no Curso 2, Aula 2. |
| **Cluster Basic** | Tipo de cluster de entrada, multi-tenant, com cobrança por uso — ideal para estudo. |
| **CLI `confluent`** | Ferramenta de linha de comando oficial para operar o Confluent Cloud. |
| **Créditos gratuitos** | Saldo promocional (US$ 400 / 30 dias) concedido a novas contas. |
| **Teardown** | Ritual de destruir recursos ao fim de cada sessão para não consumir créditos à toa. |

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Get Started (Quick Start)* — https://docs.confluent.io/cloud/current/get-started/index.html
- Confluent CLI — *Install and Configure* — https://docs.confluent.io/confluent-cli/current/install.html
- Confluent Cloud — *Billing and Free Trial* — https://docs.confluent.io/cloud/current/billing/overview.html
- Confluent Cloud — *Cluster Types* (visão de custos por tipo) — https://docs.confluent.io/cloud/current/clusters/cluster-types.html
