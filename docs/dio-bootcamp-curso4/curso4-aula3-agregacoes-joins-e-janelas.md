# Curso 4 · Aula 3 — Agregações, Joins e Janelas Temporais (Tumbling, Hopping e Session)

> **Bootcamp Confluent Cloud 2026 · Bloco 3 — Recursos Avançados e Melhorias**
> Duração alvo: ~20 min (teto) · Conceito + demo no Confluent Cloud
> **Pré-requisitos:** Aula 2 do Curso 4

---

## 1. Objetivo da Aula

Domar o tempo: entender por que **agregar um fluxo infinito exige janelas**, dissecar as três janelas clássicas — **Tumbling, Hopping e Session** — conhecer o relógio do stream (**event time e watermarks**, em nível fundamentos) e fazer os dois joins que resolvem 90% dos casos: **enriquecimento** (stream × tabela) e **correlação temporal** (stream × stream).

---

## 2. Conceitos Simplificados

### 2.1 O problema: `GROUP BY` num fluxo que não acaba *(~3 min)*

Na Aula 2, o `GROUP BY` contava **tudo desde sempre** — um resultado que muda eternamente. Útil às vezes; mas a pergunta de negócio quase sempre tem **tempo dentro dela**:

- "Total de vendas **por minuto**" · "pedidos **na última hora**" · "sessão de navegação do usuário"
- Um fluxo infinito não tem "fim" para fechar a conta — **a janela cria o fim**: recorta o infinito em pedaços finitos, cada um com resultado próprio
- Janela não é firula de sintaxe: é **a resposta arquitetural** à pergunta "quando um resultado está pronto?"

### 2.2 Anatomia das três janelas *(~5 min)*

```
 TUMBLING (fixa, sem sobreposição)      "vendas por minuto"
 ├────5min────┤├────5min────┤├────5min────┤
 [ A  B  C    ][ D  E       ][ F  G  H    ]   cada evento cai em EXATAMENTE 1 janela

 HOPPING (fixa, deslizante)             "última hora, atualizada a cada 5 min"
 ├──────────60min──────────┤
       ├──────────60min──────────┤            janelas se SOBREPÕEM:
             ├──────────60min──────────┤      1 evento → N janelas

 SESSION (dinâmica, por atividade)      "a visita do usuário ao site"
 [A B C]......gap>15min......[D E].....gap.....[F]
  sessão 1                    sessão 2          sessão 3
  a janela FECHA quando a chave fica quieta por mais que o gap
```

| | **Tumbling** | **Hopping** | **Session** |
|---|---|---|---|
| Tamanho | Fixo | Fixo, com passo menor | Dinâmico (por atividade) |
| Sobreposição | Nunca | Sim (evento conta em N janelas) | Nunca |
| Parâmetros | `size` | `size` + `slide` | `gap` de inatividade |
| Pergunta típica | "por minuto/hora/dia" | "janela móvel: última hora, a cada 5 min" | "por sessão/visita/rajada" |
| Pegadinha | — | Custo: cada evento processado N vezes | Fecha só quando o gap passa (paciência!) |

### 2.3 O relógio do stream: event time e watermarks *(~4 min)*

A pergunta traiçoeira: um pedido feito às **11:59** que chega ao pipeline às **12:01** conta na janela de qual minuto?

- **Processing time** (quando chegou) diria 12:01 — e o relatório mente sempre que a rede atrasa
- **Event time** (quando aconteceu — o `$rowtime` da Aula 2) diz 11:59 — a verdade do negócio. **Janela séria usa event time**
- Mas event time cria o dilema: se eventos podem atrasar, **quando é seguro fechar a janela das 11:55–12:00?** Esperar para sempre = nunca responder

A resposta do Flink é a **watermark** — o relógio de "até aqui eu acredito que já vi tudo":

```
 eventos:   11:58 ─ 11:59 ─ 12:01 ─ 11:59* ─ 12:02 ─ 12:03
                                    (atrasado!)
 watermark: "já vi tudo até ~12:00" ──► a janela [11:55–12:00) FECHA e emite
 o 11:59* chegou ANTES da watermark passar de 12:00? entra na conta ✓
 chegou DEPOIS? é um late event — descartado (ou tratado, em nível avançado)
```

- Watermark = **o atraso tolerado, declarado**: "espero até X de atraso; depois disso, fecho a conta". No Flink do CC, as tabelas já vêm com uma estratégia default sensata — e ela é ajustável
- O trade-off na veia: watermark **apertada** = resposta rápida, mais eventos atrasados perdidos; **folgada** = resultado mais completo, mais latência. Não existe grátis — existe **escolha declarada**
- Nível bootcamp: entender o mecanismo e o trade-off; estratégias custom e tratamento de late events são intermediate

### 2.4 Joins: enriquecer e correlacionar *(~4 min)*

Os dois joins do dia a dia, com naturezas bem diferentes:

**Stream × tabela — o enriquecimento** (o caso do bootcamp: pedido + cadastro do cliente):

- Cada evento do stream busca **o estado atual** da chave na tabela (a dualidade da Aula 2 trabalhando: `clientes` como updating table vinda do CDC)
- Estado necessário: a tabela — previsível, proporcional às chaves
- É o join "barato" e onipresente — todo pipeline de contexto é ele

**Stream × stream — a correlação temporal** (ex.: pedido + pagamento em até 10 min):

- Juntar dois fluxos infinitos exige **limite temporal** (interval join) — senão o estado cresce para sempre esperando o par que talvez nunca venha
- "Pedido **sem** pagamento na janela" é o padrão de detecção (fraude, SLA) — e o motivo de este join ser a base do caso do capstone
- Estado necessário: os eventos "esperando o par" dentro do intervalo — **o tempo limita o estado** (a deixa da Aula 4)

**Sintaxe de referência:**

| Construção | Para quê | Ponto didático |
|---|---|---|
| `TUMBLE(TABLE pedidos, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE)` | Janela fixa | A TVF de janela: a tabela "ganha" colunas window_start/end |
| `HOP(..., INTERVAL '5' MINUTE, INTERVAL '1' HOUR)` | Janela deslizante | slide + size |
| `SESSION(..., INTERVAL '15' MINUTE)` | Janela por atividade | o gap |
| `GROUP BY window_start, window_end, ...` | Fecha a conta por janela | O resultado nasce quando a watermark fecha a janela |
| `JOIN clientes FOR SYSTEM_TIME AS OF ...` | Enriquecimento (temporal join) | "o cadastro válido NO MOMENTO do evento" |
| `a.ts BETWEEN b.ts - INTERVAL ... AND b.ts + INTERVAL ...` | Interval join stream×stream | O tempo limitando o estado |

---

## 3. Demo — Especificação

> **Implementação:** projeto de demos (Claude Code local). Fontes: tópicos `pedidos`/`clientes` do Curso 3 (dataset placeholder) + **Datagen pontual para volume contínuo** (janela sem fluxo constante é aula morta). Tempo de tela: ~7 min. **Statements e Datagen parados ao final.**

**O que a demo deve mostrar, nesta ordem:**

1. **Agregação tumbling ao vivo** — vendas por janela de 1 minuto (`TUMBLE` + `GROUP BY window_start`), com o Datagen alimentando; assistir 2–3 janelas fecharem.
   - **Observação-alvo:** o contraste com o `GROUP BY` da Aula 2 — aqui cada janela **fecha e emite um resultado definitivo**. A janela criou o "fim".

2. **A mesma conta em hopping** — última hora, atualizada a cada minuto (parâmetros encolhidos pro relógio da demo).
   - **Observação-alvo:** o mesmo evento aparecendo em **várias** janelas — a sobreposição visível, e o porquê do custo.

3. **O evento atrasado** — injetar manualmente um evento com timestamp "do passado recente" (dentro da tolerância) e um "do passado distante" (fora).
   - **Observação-alvo:** o primeiro **entra** na janela correta (event time funcionando); o segundo **não** (a watermark já passou) — o trade-off da seção 2.3, encenado. É a cena mais importante da aula.

4. **O join de enriquecimento** — `pedidos` × `clientes` (temporal join): o evento de pedido saindo com nome/segmento do cliente anexados; materializar via CTAS em `pedidos_enriquecidos`.
   - **Observação-alvo:** o pipeline de contexto do mundo real em uma query — e o resultado virando tópico (Aula 2) pronto pro Lakehouse (Curso 5) e pra IA (Curso 6).

5. **Teardown** — parar statements e Datagen; `pedidos_enriquecidos` fica (é insumo dos próximos cursos).

**Fechamento na tela** *(~1 min)*: "janelas criando o fim, watermark decidindo quando acreditar, joins criando contexto. Tudo isso é **estado** — e estado levanta a pergunta de produção: o que acontece quando o processo cai? Checkpoints e exactly-once, na última aula do curso."

---

## 4. Pontos de Atenção

**Equívocos comuns a desfazer em aula:**

- **"Janela é sintaxe, escolho qualquer uma."** Cada janela responde uma pergunta diferente — tumbling pra relatório periódico, hopping pra métrica móvel, session pra comportamento. Errar a janela = responder a pergunta errada com precisão.
- **"Processing time é mais simples, uso ele."** É mais simples e **mente** — o relatório muda conforme a rede do dia. Event time é a verdade do negócio; processing time é exceção consciente (ex.: métricas do próprio pipeline).
- **"Watermark é um detalhe interno do Flink."** É uma **decisão de negócio declarada**: quanto atraso o resultado tolera. Quem define não é o engenheiro sozinho — é a pergunta "esse relatório pode esperar 30 s a mais para estar completo?"
- **"Join de stream é igual join de banco."** Stream × stream sem limite temporal é estado infinito — o interval join existe por sobrevivência, não por estilo. (Stream × tabela, por sua vez, é o join "de banco" que o aluno conhece — com a tabela viva.)
- **Disciplina de relógio:** cumulate windows, late-event handling avançado e custom watermark strategies ficam fora — citar que existem, apontar a doc.

**Ganchos com a trilha:**

- Todo o estado desta aula (janelas abertas, joins esperando par) → **Aula 4**: como ele sobrevive a falhas.
- `pedidos_enriquecidos` → o **shift-left do Curso 5** (enriquecer ANTES de materializar no Lakehouse) e o contexto fresco do **Curso 6**.
- A detecção "pedido sem pagamento na janela" → padrão-base para o domínio de fraude do **Desafio Final**.

**Perguntas para checar entendimento:**

1. "Quero o total de vendas da última hora, atualizado a cada 5 minutos" e "quero o total de vendas por hora fechada" — qual janela para cada, e quantas vezes um mesmo pedido é contado em cada caso?
2. Um evento chega 2 minutos atrasado e a watermark tolera 5. Entra na janela certa? E se a tolerância fosse 1 minuto — o que o negócio ganhou e perdeu com essa escolha?

---

## 5. Glossário

| Termo | Definição |
|---|---|
| **Janela (window)** | Recorte finito de um fluxo infinito — define quando um resultado pode fechar. |
| **Tumbling window** | Janela de tamanho fixo, sem sobreposição — cada evento em exatamente uma. |
| **Hopping window** | Janela fixa que desliza em passos menores — eventos contam em várias. |
| **Session window** | Janela dinâmica que fecha após um gap de inatividade da chave. |
| **Event time** | Quando o evento aconteceu no mundo (o `$rowtime`) — a base correta das janelas. |
| **Processing time** | Quando o evento foi processado — simples e enganoso. |
| **Watermark** | O relógio de progresso do stream: declara o atraso tolerado e autoriza janelas a fechar. |
| **Late event** | Evento que chega após a watermark ter passado seu tempo — fora da conta por padrão. |
| **Temporal join (`FOR SYSTEM_TIME AS OF`)** | Enriquecimento: o evento junta com o estado da tabela válido no seu instante. |
| **Interval join** | Join stream × stream limitado por uma janela temporal entre os eventos. |
| **TVF de janela (`TUMBLE`/`HOP`/`SESSION`)** | As funções de tabela que aplicam janelas no Flink SQL. |

---

## 6. Referências

**Livros:**

- Tyler Akidau et al. — *Streaming Systems* (O'Reilly) — caps. 2–4: janelas, watermarks e triggers (a fonte conceitual desta aula)
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly) — cap. 11 (raciocínio sobre tempo em streams)

**Documentação oficial:**

- Confluent Cloud — *Flink SQL: Window TVFs (TUMBLE, HOP, SESSION, CUMULATE)* — https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html
- Confluent Cloud — *Time and Watermarks no Flink* — https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html
- Confluent Cloud — *Joins no Flink SQL (temporal, interval)* — https://docs.confluent.io/cloud/current/flink/reference/queries/joins.html
- Apache Flink — *Windowing TVF* e *Time Attributes* — https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/
