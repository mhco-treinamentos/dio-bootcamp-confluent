# Curso 7 — Operação profissional

> **Guia do aluno** · Bloco 3 — Operação

Seis cursos construindo. Agora você troca de chapéu: de quem **constrói** para quem
**opera**. Este é o curso menos "feature" e mais **disciplina** do bootcamp — e é o que
o Desafio Final cobra por escrito.

## 🖱⌨️ Console e CLI, lado a lado

Nos passos hands-on, cada ação aparece em dupla:

- **🖱 Pela Console** — o caminho de cliques, como você vê **na aula gravada**
- **⌨️ Pela CLI** — o comando copy-pasteável, com o output esperado capturado de execução real

Use a Console para *ver* e a CLI para *reproduzir e verificar*. Se um menu da Console
mudar de nome (acontece), o comando ao lado continua valendo — ele é a fonte de verdade
do guia.

## 💸 A boa notícia sobre o custo

Este é **o curso mais barato do bootcamp**. Você quase não cria recursos — você lê os
que já existem.

| O que você faz | Cobra? |
|---|---|
| Consultar a Metrics API | **Não** |
| Criar/deletar ACLs | **Não** |
| Ler o catálogo de preços | **Não** |
| Mudar `retention.ms` | **Não** (mas muda o storage cobrado) |
| Uma service account temporária | **Não** |

O único recurso criado no curso inteiro é uma service account de demonstração — que a
Aula 3 deleta no próprio teardown.

## Os quatro pilares

A ordem não é arbitrária: **não se protege, não se otimiza nem se recupera o que não se
enxerga.**

```
① OBSERVABILIDADE (Aula 2) — "eu SEI o que está acontecendo"
② SEGURANÇA       (Aula 3) — "só entra quem deve, e fica registrado"
③ CUSTOS          (Aula 4) — "eu sei QUANTO custa"
④ CONFIABILIDADE  (Aula 5) — "eu sobrevivo ao pior dia"
```

## As aulas

| Aula | Tema | O que você faz | Tipo |
|---|---|---|---|
| [1](aula1/README.md) | Operação profissional | Inventário do seu pipeline, SLI→SLO→alerta | 📄 papel |
| [2](aula2/README.md) | Observabilidade | Metrics API, consumer lag, export Prometheus | 💻 terminal |
| [3](aula3/README.md) | Segurança | Service account zerada, ACLs cirúrgicas, prefixos | 💻 terminal |
| [4](aula4/README.md) | Custos | Preços pela CLI, storage real, sizing | 💻 terminal |
| [5](aula5/README.md) | DR e garantias | RPO/RTO, runbook, auditoria das garantias | 📄 papel |

## Pré-requisitos

Este curso **lê** o que os anteriores construíram:

| Você precisa de | Vem de | Usado em |
|---|---|---|
| Tópicos com histórico | Cursos 3 e 4 | Aulas 2, 3, 4 |
| Uma API key de escopo **`cloud`** | Criada na Aula 2 | Aulas 2 e 4 |
| Cluster ativo | Curso 2 | Todas |

> 💡 **Não depende dos Cursos 5 e 6.** Se você ainda não fez Tableflow nem Real-Time AI,
> pode fazer o Curso 7 normalmente — eles aparecem só como linhas de tabela.

> 💡 **Fez teardown de tudo?** Qualquer tópico com dados serve. Substitua os nomes nos
> comandos, ou rode um `DatagenSource` por alguns minutos (Curso 3, Aula 1).

## A terceira API key

O bootcamp já pediu duas: uma de **cluster** (Curso 2) e uma de **Schema Registry**
(Curso 3). A Metrics API exige uma terceira, de escopo **`cloud`**:

```bash
confluent api-key create --resource cloud --description "Metrics API"
```

É o mesmo modelo de sempre — **cada recurso, sua credencial**. E esta key vê métricas
de **toda a organização**: guarde-a como credencial de observabilidade, nunca de
producer.

## Duas descobertas que economizam tempo

**No cluster Basic, RBAC escopado a tópico não funciona:**
```
Error: Forbidden - Basic Clusters can not use resource roles
```
Resource roles exigem Standard ou superior. O caminho é **ACL** — e é por isso que a
Aula 3 é toda em ACL. Bônus: ACL é do protocolo Kafka, então funciona em OSS e MSK
também.

**A maior parte do seu storage provavelmente não é dado de negócio.** A Aula 4 mede: no
cluster de validação, **81%** eram DLQs de conectores já deletados e tópicos de
heartbeat do Debezium — 24 tópicos que ninguém criou de propósito. Os DLQs **sobrevivem
aos conectores** que os criaram, e a Aula 4 termina limpando (14,95 MB → 2,89 MB).

## Ao terminar

Você deve conseguir responder, sobre o **seu** capstone:

1. Como eu **saberia** que parou de funcionar — sem abrir a UI?
2. Quem tem acesso a quê, e como eu revogo?
3. Quanto custa por mês, e qual é a maior alavanca?
4. O que acontece no pior dia — e qual é a minha garantia de entrega?

São exatamente as quatro seções que o README do Desafio Final cobra.

---

| ← Anterior | ↑ Trilha |
|---|---|
| [Curso 4 · Flink](../curso4/README.md) | [Todos os cursos](../README.md) |
