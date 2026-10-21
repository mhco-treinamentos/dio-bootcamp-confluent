# producers/ — do simples ao robusto

A progressão de producers do bootcamp, um degrau por aula. Cada arquivo existe para
ensinar **uma** coisa nova; o anterior continua válido como contraste.

| Arquivo | Degrau | Aula |
|---|---|---|
| `hello_kafka.py` | O mínimo: string, sem Schema Registry | Curso 2 · Aula 4 |
| `json_producer.py` | dict → JSON + callback de entrega | Curso 2 · Aula 4 |
| `avro_producer.py` | + Schema Registry: contrato na ingestão | Curso 2 · Aula 4 |
| `reliable.py` | Idempotência, `acks=all`, métricas | Curso 1 · Aula 5 / Curso 2 |
| `poison.py` | A pílula envenenada que o DLQ do Curso 3 desvia | Curso 3 · Aula 4 |
| `config.py` | `build_producer_config` — perfis `local`/`cloud` via `KAFKA_PROFILE` | — |

Credenciais sempre via env vars (`CC_*`, `SR_*`) — copie `.env.example` e preencha.
