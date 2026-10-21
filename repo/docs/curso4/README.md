# Curso 4 — Apache Flink: processamento de streams

> **Guia do aluno** · Bloco 3 — Processamento de Streams

Até aqui os dados **passaram** pelo Kafka: produzidos, capturados por CDC, entregues por sink. Neste curso eles são **transformados** — com estado, janelas e joins.

## 💸 A regra de custo do Flink

Diferente de tudo até agora, e a boa notícia vem primeiro:

> **Compute pool ocioso custa ZERO.** Só statement rodando consome CFU.

| Recurso | Cobra? |
|---|---|
| Pool provisionado, 0 CFU | **Não** |
| Statement `RUNNING` | **Sim** — $0,21/CFU-hora |

Um statement esquecido: **~$5/dia**, **~$151/mês**.

```
O pool pode ficar. O statement, nunca.
```

Ao fim de toda sessão:

```bash
confluent flink statement list | grep RUNNING
confluent flink compute-pool describe lfcp-xxxxx | grep CFU
```

- `confluent flink statement list | grep RUNNING` — vazio
- `confluent flink compute-pool describe lfcp-xxxxx | grep CFU` — Current CFU: 0

## ⚠️ Este curso é UI-first

Os Cursos 1 a 3 eram CLI puro. Aqui muda, por um motivo prático:

**O CLI cria e gerencia statements, mas não mostra as linhas do resultado.** E o ponto
das aulas é justamente *ver dados atualizando ao vivo*.

| Tarefa | Onde |
|---|---|
| Escrever queries, ver resultados | **UI** — workspace SQL |
| Criar pool, listar/parar statements, teardown | **CLI** |

Nos passos de ciclo de vida (pool, stop, teardown), os guias mostram a dupla
**🖱 Pela Console** / **⌨️ Pela CLI** — como nos Cursos 2, 3 e 7.

> O `confluent flink shell` (interativo) exige terminal TTY e pode não funcionar em
> todos os ambientes. O workspace da UI é o caminho confiável.

## As aulas

| Aula | Tema | O que você faz |
|---|---|---|
| [1](aula1/README.md) | A query que não termina | Cria o pool, vê o CFU sair de 0 |
| [2](aula2/README.md) | Tables e Streams | Append × updating, primeiro CTAS |
| [3](aula3/README.md) | Janelas e joins | TUMBLE/HOP/SESSION, evento atrasado, enriquecimento |
| [4](aula4/README.md) | Flink em produção | Estado, checkpoint, teardown do curso |

## Pré-requisitos

O Curso 4 consome os tópicos do [Curso 3](../curso3/README.md):

| Tópico | Vem de | Usado em |
|---|---|---|
| `pg-payments.payments.transactions` | CDC (Curso 3, Aula 2) | Aulas 1, 2 |
| `pg-payments.payments.accounts` | CDC — **veja a nota** | Aula 3 (join) |
| Fluxo append-only **contínuo** | Datagen (a Aula 3 cria) | Aula 3 — TUMBLE/HOP: janela parada não fecha |
| Rajadas com silêncio | `pedidos_sink` manual (a Aula 3 cria) | Aula 3 — SESSION: fluxo contínuo nunca fecha sessão |

> ⚠️ **Ajuste necessário no conector CDC**: o guia do Curso 3 captura só
> `payments.transactions`. Para o join da Aula 3, inclua também `accounts`:
> ```json
> "table.include.list": "payments.transactions,payments.accounts"
> ```
> A publication do Neon já cobre as duas tabelas — é só recriar o conector.

## A armadilha que vale saber antes

**Janela não funciona sobre tópico CDC.** O Flink recusa com:

```
'$rowtime' in cdc source may cause wrong result... use a non-cdc source
that only has insert messages
```

CDC é **changelog** (eventos se retratam), não fluxo de fatos. A [Aula 3](aula3/README.md)
explica o porquê e o que fazer. Guarde isto: economiza meia hora.

## Ao terminar

O tópico enriquecido (`transacoes_enriquecidas`) **fica** — é insumo do Curso 5
(Lakehouse) e do Curso 6 (IA).

---

| ↑ Trilha |
|---|
| [Todos os cursos](../README.md) |
