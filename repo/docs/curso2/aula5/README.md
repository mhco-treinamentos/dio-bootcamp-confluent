# Curso 2 · Aula 5 — Convivendo com o legado: Cluster Linking

> **Guia do aluno** · Bloco 2 — Confluent Cloud
> ⏱ ~25 min · 📄 Exercício de arquitetura — **sem terminal, custo zero**

Ninguém nasce no Confluent Cloud. Esta aula é sobre a pergunta que toda migração real enfrenta: **como sair do Kafka que já existe sem parar a operação?**

---

## 🛑 Não tente reproduzir esta aula

Cluster Linking exige um cluster de destino **Dedicated ou Enterprise**. Diferente do Basic (que cobra por uso), o Dedicated cobra **capacidade provisionada por hora ligada** — em CKUs.

> Um único CKU rodando pode consumir uma fração relevante dos seus créditos **por dia**.
> Provisionar um "só para testar" é o jeito mais rápido de acabar com o orçamento do
> bootcamp inteiro.

Por isso esta aula é conceitual — **por custo, não por complexidade**. O conteúdo é real e importante; o que não cabe é a fatura.

Se você quiser ver telas, a [documentação oficial](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html) tem capturas de todo o fluxo.

---

## Antes de começar

| Você precisa | Detalhe |
|---|---|
| Ter assistido à aula | ~20 min |
| Ter feito a [Aula 1](../aula1/README.md) | O cenário híbrido foi antecipado lá |
| Papel e caneta | Vai desenhar um plano de migração |

---

## O que você vai saber fazer ao final

- Explicar por que espelhar offsets é diferente de espelhar mensagens
- Comparar Cluster Linking e MirrorMaker 2 por critérios objetivos
- Desenhar uma migração em fases que não para a operação
- Identificar o ponto de não-retorno de uma migração

---

## Os conceitos em 90 segundos

**O que o Cluster Linking faz**: replica tópicos entre clusters **byte a byte, com os
offsets preservados**.

```
   CLUSTER ORIGEM                      CLUSTER DESTINO
   (legado: OSS, Platform,             (Confluent Cloud
    ou outro Cloud)                     Dedicated/Enterprise)

   topic pedidos                       topic pedidos (mirror)
   ├── p0: offsets 0..1000    ═══►     ├── p0: offsets 0..1000   ← MESMOS offsets
   ├── p1: offsets 0..980     ═══►     ├── p1: offsets 0..980
   └── p2: offsets 0..1020    ═══►     └── p2: offsets 0..1020
                                            (somente leitura até o "promote")
```

**Por que os offsets importam tanto**: um consumidor que parou no offset 847 pode ser
apontado para o cluster novo e **continuar do 847**. Sem isso, ele reprocessaria tudo
(duplicatas) ou pularia (perda) — exatamente o dilema da Aula 5 do Curso 1.

> 📖 A anatomia completa do link, o modo de failover e as telas estão **na aula gravada**.

---

## Passo 1 — Linking ou MirrorMaker? *(~7 min)*

Para cada requisito, marque qual ferramenta atende: **Cluster Linking (CL)**, **MirrorMaker 2 (MM2)**, ou **ambos**.

| # | Requisito | CL / MM2 / ambos |
|---|---|---|
| 1 | Replicar mensagens entre dois clusters | |
| 2 | Preservar os offsets exatos, sem tradução | |
| 3 | Rodar sem infraestrutura adicional (sem Connect workers) | |
| 4 | Replicar de um Kafka OSS na sua infra para a nuvem | |
| 5 | Não pagar licença de plataforma | |
| 6 | Migrar consumidores sem reprocessar nem perder mensagens | |

**Entregável**: 6 marcações.

---

## Passo 2 — Desenhe a migração *(~10 min)*

Uma varejista tem Kafka OSS on-premises com 12 tópicos, 40 aplicações produtoras/consumidoras e **operação 24×7 que não pode parar**. Ela vai para o Confluent Cloud.

Desenhe as **4 fases** da migração, respondendo em cada uma:

1. O que é ligado/criado?
2. Quem escreve e quem lê, nessa fase?
3. O que acontece se precisar voltar atrás?

> 💡 Pista: a ordem certa é sempre **espelhar → mover leitores → mover escritores →
> desligar**. Migrar produtores antes dos consumidores cria um cluster com dado novo
> que ninguém lê.

**Entregável**: 4 fases descritas, com o plano de rollback de cada uma.

---

## Passo 3 — A pergunta que decide *(~5 min)*

> Em qual fase a migração passa do **ponto de não-retorno** — aquele em que voltar
> atrás custa perda de dados? E o que você faria **antes** de cruzar essa linha?

---

## Gabarito

<details>
<summary><b>Passo 1 — Linking × MirrorMaker (clique para abrir)</b></summary>

| # | Requisito | Resposta | Por quê |
|---|---|---|---|
| 1 | Replicar mensagens | **Ambos** | É o trabalho básico dos dois |
| 2 | Preservar offsets exatos | **CL** | MM2 **traduz** offsets — o 847 na origem pode ser outro no destino |
| 3 | Sem infraestrutura adicional | **CL** | É recurso do broker; MM2 roda sobre Kafka Connect (workers que você opera) |
| 4 | De OSS on-prem para a nuvem | **Ambos** | CL suporta origem OSS; o **destino** é que precisa ser Confluent |
| 5 | Sem licença de plataforma | **MM2** | MM2 é open source; CL é recurso pago |
| 6 | Migrar consumidores sem reprocessar | **CL** | Consequência direta do item 2 |

**A síntese**: MM2 é a escolha quando você não pode pagar plataforma e aceita a
complexidade operacional (workers + tradução de offset). Cluster Linking é a escolha
quando **a migração sem downtime é o requisito** — e o item 6 é o que você não
consegue replicar com MM2 sem dor.

</details>

<details>
<summary><b>Passo 2 — as 4 fases</b></summary>

**Fase 1 — Espelhar**
- *Liga*: o cluster link origem → destino; os tópicos viram **mirror topics** (somente leitura no destino)
- *Escreve*: só o legado. *Lê*: só o legado
- *Rollback*: trivial — apagar o link. Ninguém depende do destino ainda

**Fase 2 — Migrar consumidores**
- *Liga*: as aplicações consumidoras passam a apontar para o cluster novo
- *Escreve*: ainda o legado. *Lê*: **o destino** (com os offsets preservados — cada consumidor continua de onde parou)
- *Rollback*: apontar os consumidores de volta. Ainda seguro, porque o legado continua sendo a fonte

**Fase 3 — Promover e migrar produtores**
- *Liga*: o `promote` converte os mirror topics em tópicos **graváveis**; o link é encerrado
- *Escreve*: **o destino**. *Lê*: o destino
- *Rollback*: **aqui está o ponto de não-retorno** — o dado novo só existe no destino

**Fase 4 — Desligar o legado**
- Depois de um período de observação (dias ou semanas), o cluster antigo é desligado
- *Rollback*: não existe, e não precisa — a fase 3 já era definitiva

**O princípio**: consumidores migram **antes** dos produtores. Assim, se algo der
errado nas fases 1 e 2, o legado ainda é a fonte da verdade e o rollback é só
reapontar configuração.

</details>

<details>
<summary><b>Passo 3 — o ponto de não-retorno</b></summary>

**A Fase 3, no momento do `promote`.** Antes disso, o legado continua recebendo as
escritas e o destino é um espelho descartável. Depois, o dado novo nasce no destino e
**não existe no legado** — voltar significaria perder tudo o que foi escrito desde a
promoção.

**O que fazer antes de cruzar**:

1. **Validar a paridade**: contagem de mensagens e offsets finais por partição, nos dois lados
2. **Confirmar que todos os consumidores estão saudáveis no destino** — lag estável, sem erro
3. **Testar o caminho de escrita** com um tópico de baixo risco antes do fluxo crítico
4. **Combinar a janela** — o `promote` é rápido, mas o momento é uma decisão de negócio
5. **Ter o plano de "e se"**: se o destino falhar depois da promoção, a recuperação é
   restaurar do próprio destino (backup/retention), não voltar ao legado

É a mesma disciplina de qualquer cutover — a diferença é que o Cluster Linking torna as
fases 1 e 2 **reversíveis**, o que a maioria das migrações não consegue.

</details>

---

## Erros comuns

| Se você… | O que está confundindo |
|---|---|
| migrou produtores antes dos consumidores | Cria dado novo que ninguém lê, e quebra o rollback |
| achou que MM2 preserva offsets | MM2 **traduz** — é a diferença que mais dói na migração |
| tratou o `promote` como reversível | É o ponto de não-retorno; tudo antes dele é ensaio |
| planejou um big bang | Migração de 40 aplicações num fim de semana é como se perde emprego |
| provisionou um Dedicated para testar | Leia o aviso no topo desta página 🙂 |

---

## Glossário

| Termo | Definição |
|---|---|
| **Cluster Linking** | Replicação byte a byte entre clusters, com offsets preservados. |
| **Mirror topic** | Tópico do destino que espelha a origem; somente leitura até o `promote`. |
| **Promote** | Operação que torna o mirror topic gravável e encerra o link. |
| **MirrorMaker 2 (MM2)** | Ferramenta OSS de replicação sobre Kafka Connect; traduz offsets. |
| **CKU** | Unidade de capacidade do cluster Dedicated — cobra por hora provisionada. |
| **Cutover** | O momento em que o tráfego passa do sistema antigo para o novo. |

---

## Para ir além

| Conceito desta aula | Volta em |
|---|---|
| CDC como alternativa ao linking (do banco, não do Kafka) | **Curso 3** |
| Estratégia de migração incremental | Curso 1, Aula 6 — o padrão strangler |
| DR e multi-região | Curso 7 |

**Fontes**

- Confluent — [Cluster Linking](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html)
- Confluent — [Cluster Linking vs. MirrorMaker 2](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/mirror-topics.html)
- Apache Kafka — [MirrorMaker 2](https://kafka.apache.org/documentation/#georeplication)

---

## Você terminou o Curso 2 🎉

| Aula | O que você levou |
|---|---|
| 0 | Ambiente provisionado e o ritual de teardown |
| 1 | Fronteira de gestão: quem opera o Kafka |
| 2 | Identidade (service account) ≠ credencial (API key) |
| 3 | Contrato de dados e o portão de compatibilidade |
| 4 | O caminho completo, e o que viaja dentro da mensagem |
| 5 | Como sair do legado sem parar a operação |

**No Curso 3**, você para de escrever producers: os dados vão entrar no Kafka por
**configuração** — conectores gerenciados e CDC.

Antes de seguir, o ritual:

```bash
confluent connect cluster list
confluent flink compute-pool list
```

- `confluent connect cluster list` — None found.
- `confluent flink compute-pool list` — None found.

---

| ← Anterior | ↑ Curso | Próxima → |
|---|---|---|
| [Aula 4 · Primeira mensagem](../aula4/README.md) | [Curso 2](../README.md) | [Curso 3 · Kafka Connect e CDC](../../curso3/README.md) |
