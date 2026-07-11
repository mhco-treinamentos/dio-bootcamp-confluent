# Curso 2 · Aula 4 — Mão na Massa: produzindo e consumindo sua primeira mensagem real

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~15–18 min · **Aula demo-first**: ~5 min de conceito, ~12 min de tela
> **Pré-requisitos:** Aulas 2 e 3 (cluster, service account, API keys, schema registrado)

---

## 1. Objetivo da Aula

Consolidar tudo o que o bootcamp construiu até aqui num único fluxo executado ponta a ponta: um **evento com contrato**, produzido por uma **identidade de aplicação**, num **tópico gerenciado**, consumido e **inspecionado pela plataforma**. É a primeira mensagem "de gente grande" — e a prova de que as peças se encaixam.

---

## 2. Conceitos Simplificados

### 2.1 O caminho completo: todas as peças no mesmo mapa *(~4 min)*

A única anatomia desta aula é a **síntese** — cada estação do caminho aponta para a aula que a explicou:

```
 PRODUCER (identidade: service account · Aula 2)
    │  autentica: API key escopada (Aula 2)
    │  serializa: Avro validado no Schema Registry (Aula 3)
    │  particiona: hash da key → partição (Curso 1, Aulas 3–5)
    ▼
 TOPIC `pedidos` — log gerenciado no cluster Basic (Aula 2 + Curso 1)
    │  wire format: magic byte + schema ID + payload (Aula 3)
    ▼
 CONSUMER (consumer group · Curso 1, Aula 4)
    │  deserializa: consulta o schema ID no Registry (Aula 3)
    │  commita offset: progresso do grupo (Curso 1, Aula 4)
    ▼
 PLATAFORMA: tudo isso visível na UI — mensagens, schema, lag
```

Mensagem didática: **nada aqui é novo** — a aula é o teste de integração do conhecimento. Se alguma estação parecer nebulosa, é o endereço de revisão.

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local) — CLI + UI do Confluent Cloud **lado a lado** (split screen é parte da didática: o comando de um lado, o efeito visível do outro). Tempo de tela: ~12 min.

**O que a demo deve mostrar, nesta ordem:**

1. **Checagem do ambiente (o checklist herdado)** — CLI logada, environment/cluster selecionados, service account `app-pedidos` e keys (cluster + Schema Registry) existentes, subject `pedidos-value` com as 2 versões da Aula 3.
   - **Observação-alvo:** o inventário do que as Aulas 0–3 construíram — o aluno confere que está com o mesmo tabuleiro.

2. **Produzir eventos com chave + schema** — producer autenticado com a key da SA, serializando Avro contra o Registry, publicando o ciclo de 2–3 pedidos com `key = order_id` (o dataset narrativo do Curso 1, agora no Cloud).
   - **Observação-alvo:** o comando une **as três credenciais/contratos** (key do cluster, key do SR, schema) — cada flag do comando é uma aula anterior aparecendo.

3. **Ver as mensagens na UI** — abrir o tópico no console do Confluent Cloud: mensagens visíveis com key, value **deserializado** (a UI usa o Registry), partição, offset e timestamp.
   - **Observação-alvo:** a anatomia do record (Curso 1, Aula 3) renderizada pela plataforma — e o value legível **porque** há contrato; a UI é um consumer como outro qualquer.

4. **Consumir pela CLI com consumer group nomeado** — group `pedidos-app`, do início, exibindo key + partição + offset; deixar o grupo commitado.
   - **Observação-alvo:** ordem por chave confirmada no Cloud (eventos do mesmo pedido, mesma partição, em ordem) — o Curso 1 vale inteiro aqui.

5. **Inspecionar o consumer group e o lineage na UI** — a tela de consumer lag do grupo `pedidos-app` (offsets por partição, lag zerado) e, na sequência, o **Stream Lineage** do tópico: o fluxo producer → `pedidos` → consumer desenhado automaticamente a partir do tráfego que acabou de acontecer.
   - **Observação-alvo:** o progresso do grupo (Curso 1, Aula 4) como cidadão de primeira classe da plataforma — e o lineage provando o que a Aula 1 prometeu: o mapa do fluxo é gerado do tráfego real, ninguém desenhou nada. Sementes da observabilidade (Curso 7) e da governança (Live 3).

6. **Teardown consciente** — encerrar consumidores; manter tópico, schema, SA e keys (base dos próximos cursos); conferência rápida de billing.

**Fechamento na tela** *(~1 min)*: "evento com contrato, identidade de aplicação, log gerenciado, consumo inspecionável — o fluxo fundamental está de pé, e é sobre ele que TODO o resto do bootcamp constrói: conectores (Curso 3), Flink (4), Lakehouse (5), IA (6), produção (7). Antes: como conviver com o legado que já existe — Cluster Linking, na próxima aula."

---

## 4. Pontos de Atenção

**Onde a demo costuma quebrar (antecipar em aula):**

- **Key errada no lugar errado** — usar key de cluster para o Schema Registry (ou vice-versa) é o erro nº 1; a mensagem de erro é de autenticação, e o aluno procura no lugar errado. Reforçar: **dois escopos, duas keys** (Aula 2/3).
- **Consumir sem o deserializer do SR** — o "lixo binário" da Aula 3 reaparecendo; agora o aluno já sabe diagnosticar (magic byte + schema ID).
- **`auto.offset.reset`** — consumidor novo que "não vê nada": provavelmente `latest` sem mensagens novas chegando. Ligar à tabela de configs do Curso 1, Aula 4.
- **Ambiente divergente** — quem pulou a Aula 3 não tem o subject; o checklist da cena 1 existe exatamente para isso.

**Ganchos com a trilha:**

- Este fluxo é a **camada 1 do Desafio Final** — o aluno pode (e deve) começar o capstone replicando este desenho no domínio escolhido.
- Conectores substituem o producer manual na ingestão real → **Curso 3**.
- O mesmo tópico vira input de Flink SQL → **Curso 4**.

**Perguntas para checar entendimento:**

1. Liste as três credenciais/contratos que o producer da cena 2 precisou e o papel de cada um.
2. A UI mostra o value legível em JSON. Isso significa que o dado trafega em JSON? O que está acontecendo por baixo?

---

## 5. Glossário

*Aula de síntese — o vocabulário é o acumulado dos glossários anteriores (Curso 1 completo + Aulas 2–3). Termos novos: nenhum, por design.*

---

## 6. Referências

**Documentação oficial:**

- Confluent Cloud — *Produce and Consume with the CLI* — https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_produce.html
- Confluent Cloud — *Message Browser (UI)* — https://docs.confluent.io/cloud/current/clusters/message-browser.html
- Confluent Cloud — *Monitor Consumer Lag* — https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html
- Confluent — *Getting Started with Apache Kafka (tutoriais por linguagem — ponte para o aluno levar o fluxo para código)* — https://developer.confluent.io/get-started/
