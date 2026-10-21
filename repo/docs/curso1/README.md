# Curso 1 — Fundamentos de EDA e Kafka

> **Guia do aluno** · Bloco 1 — Fundamentos
> Procurando o roteiro de gravação? → [demos/curso1/README.md](../../demos/curso1/README.md)

O arco do curso: você começa sem vocabulário de eventos (Aula 1) e termina sabendo por que uma transação de cartão nunca se perde nem sai de ordem num pipeline Kafka (Aula 5) — e quando vale migrar um monólito para esse modelo (Aula 6).

## As aulas

| Aula | Tema | Prática | Precisa de Docker? |
|---|---|---|---|
| [1](aula1/README.md) | O que é EDA? Evento, mensagem e comando | Exercício em papel | Não |
| [2](aula2/README.md) | Message Queue vs Data Streaming | CLI do Kafka na mão | Sim |
| [3](aula3/README.md) | O log do Kafka: tópicos, partições e offsets | CLI do Kafka na mão | Sim |
| [4](aula4/README.md) | Produtores e consumidores: grupos, rebalance e commit | **Python** (`confluent-kafka`) · [versão CLI](aula4/cli.md) | Sim |
| [5](aula5/README.md) | Ordem por chave, idempotência e semânticas de entrega | **Python** (`confluent-kafka`) · [versão CLI](aula5/cli.md) | Sim |
| [6](aula6/README.md) | Do monólito ao EDA | Exercício de decisão | Não |

## Antes das aulas práticas

A Aula 1 não precisa de nada instalado. Da Aula 2 em diante:

```bash
make install
make up
make status
```

- `make install` — uma vez: cria o venv e instala as dependências
- `make up` — sobe Kafka + Schema Registry (~30s)
- `make status` — confirma que o broker responde

O sandbox sobe **uma vez** e serve às Aulas 2 a 5. Prefere a IDE? → [PYCHARM.md](../../PYCHARM.md).

> ⚠️ **O limite honesto deste sandbox**: um broker só, então RF=1 e não existe ISR.
> As garantias de replicação do bootcamp (RF=3, `min.insync.replicas=2`) são
> **conceito** aqui e **realidade** no Confluent Cloud, a partir do Curso 2.

## Ao terminar o curso

```bash
make curso1-teardown
make down
```

- `make curso1-teardown` — remove os topics de todas as aulas
- `make down` — derruba o sandbox e apaga os volumes

---

| ↑ Trilha |
|---|
| [Todos os cursos](../README.md) |
