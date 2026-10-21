# confluent_cloud/ — o que roda DENTRO do serviço gerenciado

A fronteira deste diretório: artefatos que **executam no Confluent Cloud** — você os
submete, a plataforma os roda. A infra que você mesmo sobe (Docker local) vive em
[`deployment/`](../deployment/).

| Pasta | O quê |
|---|---|
| [`cli/`](cli/) | `teardown.sh` — inventário do que cobra por hora + destruição com confirmação |
| [`connect/`](connect/) | Configs de referência dos conectores gerenciados (Datagen, CDC Debezium, Sink Postgres) — secrets sempre via `${ENV_VAR}` |
| [`flink/`](flink/) | Statements Flink SQL do Curso 4: watermarks, dedup, quality gate, temporal join, auditoria de late events e a regra de fraude `RULE_VELOCITY` |

Regra de ouro do bootcamp: conector e statement **cobram por existir** — todo guia
termina com teardown, e o `cli/teardown.sh` é a rede de segurança.
