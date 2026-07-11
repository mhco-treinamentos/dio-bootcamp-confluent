# Curso 2 · Aula 5 — Convivendo com o Legado: migração sem parar a operação com Cluster Linking

> **Bootcamp Confluent Cloud 2026 · Bloco 2 — Confluent Cloud**
> Duração alvo: ~20 min (teto) · **Aula 100% conceitual** (Cluster Linking exige cluster Dedicated/Enterprise — fora do orçamento de créditos do aluno)

---

## 1. Objetivo da Aula

Responder a pergunta que toda empresa com Kafka faz antes de adotar o Cloud: **"e o que já existe?"** Dissecar o **Cluster Linking** — a replicação nativa, byte a byte, com **offsets preservados** — compará-lo honestamente com o **MirrorMaker 2**, e mapear os padrões de migração que não param a operação.

---

## 2. Conceitos Simplificados

### 2.1 O problema: ninguém nasce no Cloud *(~3 min)*

O cenário real da maioria: um Kafka OSS/on-premises em produção, dezenas de aplicações conectadas, e a decisão de ir para o gerenciado. As restrições que definem o problema:

- **Parar a operação não é opção** — o pipeline processa negócio agora
- **Big bang é aposta, não plano** — migrar tudo num fim de semana concentra risco máximo no momento de menor controle
- A migração precisa ser **incremental, testável e reversível** — o strangler pattern do Curso 1 (Aula 6), agora em escala de plataforma

A peça que viabiliza isso: **replicar dados entre clusters de forma contínua** enquanto as aplicações migram no seu ritmo.

### 2.2 Anatomia do Cluster Linking *(~5 min)*

O Cluster Linking é replicação **nativa do protocolo**, de cluster para cluster, sem infraestrutura no meio:

```
 CLUSTER ORIGEM (legado: OSS / Platform / outro CC)
    topic `pedidos`
    │ part 0 │ off 0 │ off 1 │ off 2 │ ...
    │
    │   CLUSTER LINK  (conexão broker→broker, sem workers)
    ▼
 CLUSTER DESTINO (Confluent Cloud — Dedicated/Enterprise)
    MIRROR TOPIC `pedidos`  (read-only enquanto espelho)
    │ part 0 │ off 0 │ off 1 │ off 2 │ ...
              ▲ MESMOS offsets, MESMOS bytes ▲
```

As três propriedades que definem o mecanismo:

1. **Byte-for-byte** — o mirror topic é uma cópia fiel: mesmas mensagens, mesma partição, **mesmos offsets**. Não há re-serialização nem re-particionamento
2. **Offsets preservados** — a consequência prática que muda tudo: um consumer group migrado **continua exatamente de onde parou** — o offset commitado no origem vale no destino. Sem essa propriedade, toda migração de consumidor vira um problema de "reprocesso ou perco?"
3. **Sem infraestrutura intermediária** — o link é conversa direta entre clusters (protocolo Kafka), não um exército de workers para operar

Mecânica do espelho: o mirror topic é **read-only** — produzir nele é proibido enquanto for espelho. A migração de um tópico termina com a **promoção**: o espelho corta o cordão (promote/failover) e vira um tópico normal, gravável.

### 2.3 Cluster Linking × MirrorMaker 2 *(~4 min)*

O MM2 é a ferramenta open source de replicação — construída **sobre o Kafka Connect**:

| Dimensão | **MirrorMaker 2** | **Cluster Linking** |
|---|---|---|
| Mecanismo | Consumers + producers (Connect) re-publicando | Replicação nativa broker→broker |
| Offsets | **NÃO preservados** — re-produção gera offsets novos; migrar consumer exige tradução de offsets (checkpoints/mapeamento) | **Preservados** — consumer migra e continua do mesmo offset |
| Fidelidade | Re-serializa/re-publica (timestamps e compressão podem variar) | Byte-for-byte |
| Infraestrutura | Cluster Connect para dimensionar e operar | Nenhuma adicional |
| Onde roda | Qualquer Kafka (OSS↔OSS inclusive) | Requer Confluent (destino Dedicated/Enterprise no Cloud) |
| Custo | "Grátis" na licença; caro em operação e no risco da tradução de offsets | Recurso pago da plataforma |

Enquadramento honesto: **MM2 é a resposta quando as duas pontas são OSS** e não há Confluent na jogada; entre clusters Confluent, o Cluster Linking elimina a classe inteira de problemas de tradução de offset — que é onde migrações com MM2 costumam sangrar.

### 2.4 Padrões de migração sem parar a operação *(~5 min)*

O plano incremental canônico com Cluster Linking, tópico a tópico:

```
FASE 1 · ESPELHAR    origem ──link──► mirror topics no Cloud
                     (produção intocada; dados fluindo em tempo real)

FASE 2 · MIGRAR      consumers apontam pro Cloud, um a um
  CONSUMERS          (offsets preservados = continuam de onde pararam;
                      rollback = apontar de volta)

FASE 3 · PROMOVER    promote do mirror topic + producers apontam pro Cloud
  E MIGRAR           (a janela crítica é POR TÓPICO, minutos — não big bang)
  PRODUCERS

FASE 4 · DESLIGAR    legado esvazia tópico a tópico até apagar a luz
```

- **Consumers primeiro, producers depois** — espelho garante que o destino tem tudo; consumidores validam o Cloud em leitura antes de qualquer escrita mudar de lugar
- **Reversibilidade por fase** — até a promoção, voltar atrás é reapontar conexão
- O mesmo mecanismo serve além da migração: **DR/multi-region** (link contínuo para um cluster de contingência) e **compartilhamento entre organizações** — sementes do Curso 7, Aula 5

**Fechamento do curso** *(~1 min)*: "portfólio posicionado, plataforma operada, contrato de dados, fluxo ponta a ponta e a ponte com o legado. O Bloco 2 continua no Curso 3 — onde os dados param de ser digitados por nós e passam a vir de onde eles realmente vivem: os bancos de dados, via Kafka Connect e CDC."

---

## 3. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Migrar Kafka = parar tudo num fim de semana."** O anti-padrão que a aula inteira desmonta. Migração madura é por tópico, com espelho validado e rollback barato.
- **"MM2 e Cluster Linking fazem a mesma coisa."** A diferença de mecanismo (re-publicação × replicação nativa) gera a diferença que importa: **offsets**. Quem já traduziu offset com MM2 em produção sabe o preço.
- **"Mirror topic é um tópico normal."** É read-only até a promoção — desenhar o fluxo de produção sabendo disso evita surpresa no dia da virada.
- **"Cluster Linking é só para migração."** Migração é o caso de entrada; DR, agregação multi-region e data sharing usam o mesmo mecanismo (Curso 7).
- **Transparência com a turma:** a demo não acontece por **custo** (Dedicated/Enterprise), não por complexidade — dizer isso explicitamente e mostrar telas/diagramas da documentação oficial. Honestidade didática vale mais que demo forçada.

**Ganchos com a trilha:**

- Strangler pattern e migração incremental (o fundamento) → **Curso 1, Aula 6**.
- CDC como a outra ponte com o legado (dados de bancos, não de outro Kafka) → **Curso 3** (na sequência).
- Multi-region, DR e RPO/RTO com links contínuos → **Curso 7, Aula 5**.

**Perguntas para checar entendimento:**

1. Por que "offsets preservados" elimina o problema mais doloroso da migração de consumidores? O que acontece nesse ponto numa migração via MM2?
2. Na fase 2 do plano, o que exatamente valida que o Cloud está pronto — e qual é o rollback se algo der errado?

---

## 4. Glossário

| Termo | Definição |
|---|---|
| **Cluster Linking** | Replicação nativa broker→broker entre clusters, byte a byte, com offsets preservados. |
| **Cluster link** | O recurso de conexão configurado entre cluster origem e destino. |
| **Mirror topic** | Tópico-espelho read-only no destino, réplica fiel do tópico de origem. |
| **Promote / failover** | Operação que desliga o espelhamento e torna o mirror topic gravável (promote = planejado; failover = contingência). |
| **MirrorMaker 2 (MM2)** | Ferramenta OSS de replicação entre clusters, baseada no Kafka Connect (re-publicação). |
| **Tradução de offsets** | Mapeamento origem→destino exigido pelo MM2 para migrar consumidores (os offsets divergem). |
| **Strangler pattern** | Migração incremental em que o novo cresce em volta do legado (Curso 1, Aula 6). |
| **DR (Disaster Recovery)** | Estratégia de contingência entre regiões/clusters — caso de uso contínuo do Cluster Linking (Curso 7). |

---

## 5. Referências

**Livros:**

- Gwen Shapira et al. — *Kafka: The Definitive Guide, 2ª ed.* (O'Reilly) — cap. 10 ("Cross-Cluster Data Mirroring"): o problema e o MM2 na fonte. E-book gratuito: https://www.confluent.io/resources/kafka-the-definitive-guide/
- Sam Newman — *Monolith to Microservices* (O'Reilly) — a disciplina de migração incremental que a aula aplica a plataformas

**Documentação e artigos oficiais:**

- Confluent Cloud — *Cluster Linking Overview* — https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html
- Confluent Cloud — *Mirror Topics* — https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/mirror-topics-cc.html
- Confluent — *Migrate to Confluent Cloud with Cluster Linking* — https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/migrate-cc.html
- Apache Kafka — *MirrorMaker 2 (Geo-Replication)* — https://kafka.apache.org/documentation/#georeplication
- KIP-382: MirrorMaker 2.0 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0
