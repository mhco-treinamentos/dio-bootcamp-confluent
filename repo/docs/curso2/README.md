# Curso 2 — Confluent Cloud, Schema Registry e Data Contracts

> **Guia do aluno** · Bloco 2 — Confluent Cloud

Aqui o Docker sai de cena. Você vai operar um cluster gerenciado de verdade, criar identidades de aplicação, dar contrato aos seus dados — e aprender a não queimar seus créditos.

## 🖱⌨️ Console e CLI, lado a lado

Nos passos hands-on, cada ação aparece em dupla:

- **🖱 Pela Console** — o caminho de cliques, como você vê **na aula gravada**
- **⌨️ Pela CLI** — o comando copy-pasteável, com o output esperado capturado de execução real

Use a Console para *ver* e a CLI para *reproduzir e verificar*. Se um menu da Console
mudar de nome (acontece), o comando ao lado continua valendo — ele é a fonte de verdade
do guia.

## 💰 Antes de tudo: o contrato de custo

Este é o primeiro curso que **gasta dinheiro**. A regra que vale do Curso 2 ao 7:

> **Cluster Basic ocioso = ok** (cobra por uso, praticamente zero parado)
> **Conector e Flink statement ligados = crédito queimando**

Ao fim de **toda** sessão de estudo:

```bash
confluent connect cluster list
confluent flink compute-pool list
```

- `confluent connect cluster list` — deve dizer "None found."
- `confluent flink compute-pool list` — deve dizer "None found."

E não crie a conta semanas antes de estudar — **os créditos têm validade em dias corridos** a partir do signup.

## As aulas

| Aula | Tema | Prática | Gasta crédito? |
|---|---|---|---|
| [0](aula0/README.md) | Setup: conta, CLI e teardown | CLI + UI | ☁️ Sim (cria a base) |
| [1](aula1/README.md) | O ecossistema Confluent | Exercício de decisão | Não |
| [2](aula2/README.md) | Clusters, service accounts e API keys | CLI | ☁️ Sim |
| [3](aula3/README.md) | Schema Registry e data contracts | CLI + UI | ☁️ Sim |
| [4](aula4/README.md) | Mão na massa: primeira mensagem real | CLI + UI | ☁️ Sim |
| [5](aula5/README.md) | Cluster Linking | Exercício de arquitetura | Não |

Duas aulas custam **zero** (1 e 5). As outras quatro cabem todas no **cluster Basic** — nenhuma exige Dedicated.

## O tabuleiro acumulado

As Aulas 0, 2, 3 e 4 constroem **um estado único**, usado até o Curso 7:

```
Aula 0 → environment bootcamp-dio + cluster Basic + CLI apontada
Aula 2 → service account app-pedidos + API key (cluster) + ACLs
Aula 3 → tópico pedidos + API key (Schema Registry) + subject com 2 versões
Aula 4 → mensagens Avro + consumer group pedidos-app
```

Por isso cada guia começa com um **checklist de pré-requisitos**. Se você pulou uma aula, ele diz exatamente onde voltar.

> ⚠️ **O erro nº 1 deste curso**: usar a API key do **cluster** (`lkc-`) no **Schema
> Registry** (`lsrc-`), ou vice-versa. O erro que aparece é de autenticação, e manda
> você procurar no lugar errado. São recursos diferentes, com credenciais diferentes.

## Ordem recomendada

A Aula 1 é conceitual e pode ser lida a qualquer momento. As demais têm dependência:

```
Aula 0 ──► Aula 2 ──► Aula 3 ──► Aula 4
   (Aulas 1 e 5: independentes, custo zero)
```

## Ao terminar o curso

**Não destrua nada.** O tabuleiro do Curso 2 é a base do Curso 3 (conectores e CDC). Só confirme que nenhum conector ou compute pool ficou ligado.

Se quiser mesmo zerar tudo (só ao final do bootcamp):

```bash
confluent environment delete env-xxxxx
```

- `confluent environment delete env-xxxxx` — ⚠️ apaga TUDO que está dentro

---

| ↑ Trilha |
|---|
| [Todos os cursos](../README.md) |
