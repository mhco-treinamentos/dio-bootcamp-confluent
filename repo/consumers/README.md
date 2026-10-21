# consumers/ — do simples ao robusto

A progressão de consumers, espelho da de [`producers/`](../producers/):

| Arquivo | Degrau | Aula |
|---|---|---|
| `hello_consumer.py` | Poll loop mínimo, auto-commit | Curso 1 · Aula 4 |
| `json_consumer.py` | Desserialização + tratamento de erro | Curso 1 · Aula 4 |
| `avro_consumer.py` | + Schema Registry | Curso 2 · Aula 4 |
| `reliable.py` | Commit manual **após** processar (at-least-once), rebalance listener | Curso 1 · Aulas 4–5 |
| `dlq_consumer.py` | try/retry/desvia para DLT — o padrão do Curso 3, Aula 4 | Curso 3 |
| `config.py` | `build_consumer_config` — perfis `local`/`cloud` | — |

A lição transversal: a garantia fim a fim termina no **seu** código — idempotência no
efeito é responsabilidade do consumidor (Curso 7, Aula 5).
