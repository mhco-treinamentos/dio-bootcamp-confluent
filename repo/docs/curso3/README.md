# Curso 3 — Kafka Connect e CDC

> **Guia do aluno** · Bloco 2 — Confluent Cloud

Neste curso você para de escrever producers. Os dados entram e saem do Kafka por **configuração** — e um banco legado passa a publicar eventos sem que ninguém toque na aplicação que escreve nele.

## 🖱⌨️ Console e CLI, lado a lado

Nos passos hands-on, cada ação aparece em dupla:

- **🖱 Pela Console** — o caminho de cliques, como você vê **na aula gravada**
- **⌨️ Pela CLI** — o comando copy-pasteável, com o output esperado capturado de execução real

Use a Console para *ver* e a CLI para *reproduzir e verificar*. Se um menu da Console
mudar de nome (acontece), o comando ao lado continua valendo — ele é a fonte de verdade
do guia.

## 💸 O curso mais caro do bootcamp

Até aqui, o cluster Basic ocioso custava ~zero. Isso muda:

> **Conector ligado cobra por task/hora — mesmo sem tráfego.**
> **Pausar não zera. Só deletar zera.**

E há um segundo custo que quase ninguém antecipa:

> **Um conector CDC ligado impede o Neon de hibernar**, queimando CU-hours do free
> tier (100/mês). Um conector esquecido ligado **estoura o banco em ~4 dias**.

Por isso todo guia deste curso termina com teardown — e os de CDC têm **duas etapas**:
deletar o conector *e* remover o replication slot.

```bash
confluent connect cluster list
```

- `confluent connect cluster list` — deve dizer "None found."

## As aulas

| Aula | Tema | Conector | Precisa do Neon? |
|---|---|---|---|
| [Setup](setup/README.md) | O banco legado no Neon | — | ☁️ Provisiona |
| [1](aula1/README.md) | Kafka Connect: integração por configuração | `DatagenSource` | **Não** |
| [2](aula2/README.md) | CDC com Debezium | `PostgresCdcSourceV2` | Sim |
| [3](aula3/README.md) | Sink connectors | `PostgresSink` | Sim |
| [4](aula4/README.md) | SMT e Dead Letter Topic | sink + SMT | Sim |

> 💡 **Sem conseguir expor um Postgres?** A [Aula 1](aula1/README.md) usa o
> `DatagenSource`, que gera os próprios dados e não depende de banco nenhum. Você
> ainda faz um conector gerenciado de ponta a ponta.

## Por que Neon, e não o Docker do Curso 1

O conector gerenciado roda **na infraestrutura da Confluent**. Ele precisa alcançar
seu Postgres pela internet, na porta 5432 — e um `docker compose` local é invisível
para ele.

O Neon resolve isso de graça: Postgres serverless com endpoint público e TLS, sem VPN
nem túnel. O [Setup](setup/README.md) provisiona tudo.

## O tabuleiro acumulado

```
Curso 2  → cluster, service account, API keys (cluster + Schema Registry)
Setup    → Neon com replicação lógica + 2.820 registros de payments
Aula 1   → primeiro conector (Datagen) — criado e deletado
Aula 2   → tópico CDC com o envelope Debezium
Aula 3   → tabela de destino escrita por um sink
Aula 4   → SMT no caminho + Dead Letter Topic
```

## Configs de referência

As configurações validadas (com as armadilhas que só aparecem executando) estão em
[`confluent_cloud/connect/`](../../confluent_cloud/connect/README.md) — incluindo as
três que fazem um sink JDBC falhar com erro genérico.

## Ao terminar o curso

```bash
confluent connect cluster list
```

- `confluent connect cluster list` — None found.

E do lado do banco:

```bash
docker run --rm postgres:16-alpine psql "$PGURL" \
  -c "SELECT slot_name, active FROM pg_replication_slots;"
```

Só `wal_proposer_slot` (interno do Neon) deve permanecer. Qualquer `bootcamp_cdc_slot`
sobrando é WAL sendo retido e compute sendo queimado.

---

| ↑ Trilha |
|---|
| [Todos os cursos](../README.md) |
