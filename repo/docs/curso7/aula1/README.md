# Curso 7 · Aula 1 — Operação profissional: o mapa dos quatro pilares

> **Guia do aluno** · Bloco 3 — Operação
> ⏱ ~30 min · 📄 **Exercício em papel** — sem Docker, sem terminal, sem custo

Seis cursos construindo. Agora você troca de chapéu: de quem **constrói** para quem
**opera**. Este é o único exercício do bootcamp que se faz com caneta.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter feito os Cursos 1 a 4 | O pipeline que você vai operar |
| Papel e caneta | Sério |

Nada para instalar. Nada para pagar.

---

## O que você vai saber fazer ao final

- Fazer o **inventário operacional** do seu próprio pipeline
- Identificar quais peças podem **falhar em silêncio**
- Escrever um **SLI → SLO → alerta** que não vira ruído
- Saber o que cada uma das próximas 4 aulas resolve

---

## Os conceitos em 90 segundos

**Day 1** é fazer funcionar. **Day 2** é manter funcionando — sob carga, com gente
mudando código, para sempre.

Três coisas mudam:

| | Day 1 | Day 2 |
|---|---|---|
| Falha | "e se acontecer" | **"quando acontecer"** |
| Custo | teardown ao fim do lab | **recorrente, todo mês** |
| Segurança | configuração | **processo** (rotação, auditoria) |

E os quatro pilares, nesta ordem — que não é arbitrária:

```
① OBSERVABILIDADE (Aula 2) — "eu SEI o que está acontecendo"
② SEGURANÇA       (Aula 3) — "só entra quem deve, e fica registrado"
③ CUSTOS          (Aula 4) — "eu sei QUANTO custa"
④ CONFIABILIDADE  (Aula 5) — "eu sobrevivo ao pior dia"
```

**Não se protege, não se otimiza nem se recupera o que não se enxerga.** Por isso
observabilidade vem primeiro.

> 📖 A analogia da fronteira de gestão e o desenvolvimento de SLI/SLO/SLA estão **na
> aula gravada**.

---

## Passo 1 — O inventário do seu pipeline *(~10 min)*

Liste, no papel, **cada peça** que você construiu nos Cursos 1 a 4 e que estaria
rodando se isso fosse produção. Para cada uma, responda três perguntas:

| Peça | O que eu monitoraria | Como eu saberia que quebrou | Falha em silêncio? |
|---|---|---|---|
| | | | |

Comece por esta lista (é o que o bootcamp realmente construiu):

- Cluster + tópicos (Cursos 1–2)
- Schema Registry (Curso 2)
- Conector CDC Debezium (Curso 3)
- Conector sink Postgres (Curso 3)
- DLQ do sink (Curso 3, Aula 4)
- Statements Flink (Curso 4)

**O que observar**: a coluna que importa é a última. Um conector que vai para `FAILED`
é barulhento — a UI fica vermelha. Um DLQ enchendo é **silencioso**: tudo parece
verde, e os dados desviados se acumulam sem que ninguém veja.

<details>
<summary><b>Gabarito comentado (responda antes de abrir)</b></summary>

| Peça | Monitorar | Como sei que quebrou | Silencioso? |
|---|---|---|---|
| **Cluster + tópicos** | throughput, storage, partições | métricas param / storage dispara | **Sim** — partição quente não avisa |
| **Schema Registry** | evoluções, compatibilidade | producer rejeitado no deploy | Não — quebra na cara do dev |
| **CDC Debezium** | status, lag de replicação, tamanho do slot | status ≠ `RUNNING` | Parcialmente — slot crescendo é silencioso |
| **Sink Postgres** | status, **consumer lag** | lag subindo sem parar | **Sim** — o mais perigoso |
| **DLQ** | **profundidade** (nº de mensagens) | ninguém, se você não medir | **Sim, totalmente** |
| **Statement Flink** | status, CFU, lag, exceções | statement em `FAILED` | Não — mas **esquecido ligado** é custo silencioso |

**A leitura transversal:** sink, Flink, Tableflow e qualquer agente de IA são todos
**consumidores**. Lag crescendo é o sintoma universal de "alguém não está dando conta".
É por isso que a Aula 2 coroa o consumer lag como a métrica rainha.

**O DLQ é a armadilha do bootcamp.** Você o criou no Curso 3, Aula 4, e provavelmente
nunca mais olhou. Na Aula 2 você vai medir e descobrir que ele é o **maior tópico do
seu cluster**.

</details>

---

## Passo 2 — Um SLI, um SLO, um alerta *(~8 min)*

Escolha **uma** peça do seu inventário e complete a cadeia:

```
SLI  (a medida) ........... _______________________________
SLO  (a meta) ............. _______________________________
ALERTA (a condição) ....... _______________________________
CONSEQUÊNCIA .............. _______________________________
```

A última linha é a que separa alerta de ruído: **se ninguém faz nada quando dispara,
não é alerta — é notificação decorativa**, e ela treina o time a ignorar a próxima.

<details>
<summary><b>Um exemplo completo</b></summary>

Para o consumidor de fraude do pipeline:

```
SLI ........... consumer lag do grupo `fraud-detector`, em segundos
SLO ........... lag < 30 s em 99% das janelas de 5 minutos
ALERTA ........ lag > 30 s por 5 minutos consecutivos
CONSEQUÊNCIA .. plantão é acionado; runbook = verificar se o statement Flink
                está RUNNING, depois checar se houve pico de produção
```

**Por que o "por 5 minutos consecutivos"?** Sem ele, todo pico momentâneo de tráfego
dispara o alerta. Com ele, só dispara o que persiste — que é o que realmente indica
que o consumo não acompanha.

**Por que a consequência é explícita?** Porque um alerta sem runbook acorda alguém que
não sabe o que fazer. O tempo de resposta vira tempo de investigação do zero.

Um contra-exemplo — SLO ruim:

```
SLI ........... consumer lag
SLO ........... lag deve ser zero          ← impossível; lag zero é o buffer vazio
ALERTA ........ lag > 0                     ← dispara 24h por dia
CONSEQUÊNCIA .. (nenhuma definida)          ← ninguém vai olhar em uma semana
```

</details>

---

## Passo 3 — Onde dói primeiro *(~7 min)*

São 3h da manhã. Uma destas quatro coisas aconteceu. Para cada uma, escreva no papel:
**quanto tempo até alguém perceber?**

1. O conector CDC foi para `FAILED`.
2. O statement Flink continua `RUNNING`, mas o consumer lag está subindo há 6 horas.
3. Uma API key com permissão de admin foi commitada num repositório público.
4. Alguém subiu a retenção de um tópico de alto volume de 7 para 90 dias.

<details>
<summary><b>Gabarito</b></summary>

**1. Conector `FAILED`** — minutos, *se* houver alerta de status; horas ou dias se
depender de alguém abrir a UI. É o caso **fácil**: a plataforma sabe que quebrou.

**2. Lag subindo há 6 horas** — este é o caso **difícil**. Tudo está `RUNNING`, tudo
está verde. Só quem mede a **tendência** do lag percebe. Sem isso, você descobre pelo
cliente reclamando que o alerta de fraude chegou tarde demais. → **Aula 2**

**3. API key vazada** — potencialmente **nunca**, se não houver rotação nem auditoria.
E o dano é ilimitado: com permissão de admin, quem tem a key pode ler tudo, escrever
em tudo e apagar tudo. → **Aula 3**

**4. Retenção de 7 → 90 dias** — cerca de **30 dias**, quando chegar a fatura. E o
efeito não é linear: a retenção multiplica em todos os tópicos derivados também. →
**Aula 4**

**O padrão:** o que a plataforma detecta, você fica sabendo. O que depende de
**tendência** (lag, custo, DLQ enchendo) só chega se você tiver construído o
instrumento. É exatamente isso que as próximas quatro aulas constroem.

</details>

---

## Passo 4 — Fixe os conceitos *(~5 min)*

1. Por que observabilidade é o primeiro pilar e não segurança?
2. Qual a diferença prática entre dashboard e alerta?
3. "É gerenciado, não preciso operar." Onde está o erro?

<details>
<summary><b>Gabarito</b></summary>

**1. Porque os outros três pilares dependem de dados.** Você não sabe qual acesso
revogar sem saber quem usa o quê; não sabe onde otimizar sem saber onde o dinheiro vai;
não sabe se o DR funcionou sem métricas do failover. Observabilidade é o instrumento
que os outros três usam.

**2. Dashboard é para investigar; alerta é para saber.** Dashboard exige que alguém
esteja olhando — e às 3h da manhã ninguém está. Operação madura é ser **acordado pelo
sistema**, não descobrir pelo cliente. Dashboard responde "por quê"; alerta responde
"agora".

**3. A plataforma opera a infraestrutura; as decisões continuam suas.** A Confluent
cuida de broker, disco, patch, replicação. Ela **não** decide o que alertar, quem
acessa o quê, quanto reter ou como você falha. Fronteira de gestão não é terceirização
de responsabilidade — é redistribuição dela.

</details>

---

## Glossário

| Termo | Definição |
|---|---|
| **Day 2 operations** | Manter sistemas rodando após o deploy — o tema do curso. |
| **Superfície operacional** | O conjunto de peças e sinais que exigem atenção. |
| **Falha silenciosa** | Degradação sem alerta — a categoria mais perigosa. |
| **SLI / SLO / SLA** | A medida / a meta interna / o contrato com consequência. |
| **p99** | A latência abaixo da qual ficam 99% das operações. |
| **Alert fatigue** | Excesso de alertas sem ação — treina o time a ignorar. |
| **Runbook** | O procedimento a executar quando o alerta dispara. |

---

## Para ir além

| Pilar | Aula |
|---|---|
| ① Observabilidade | [Aula 2](../aula2/README.md) — Metrics API, lag, Prometheus |
| ② Segurança | [Aula 3](../aula3/README.md) — service accounts, ACLs, menor privilégio |
| ③ Custos | [Aula 4](../aula4/README.md) — retenção, compactação, sizing |
| ④ Confiabilidade | [Aula 5](../aula5/README.md) — multi-region, DR, garantias |

**Fontes**

- Google SRE — [Service Level Objectives](https://sre.google/sre-book/service-level-objectives/)
- Confluent — [Monitor Confluent Cloud](https://docs.confluent.io/cloud/current/monitoring/index.html)
- Confluent — [SLA](https://www.confluent.io/confluent-cloud/sla/)

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Curso 4 · Aula 4](../../curso4/aula4/README.md) | [Curso 7](../README.md) | [Aula 2 · Observabilidade](../aula2/README.md) |
