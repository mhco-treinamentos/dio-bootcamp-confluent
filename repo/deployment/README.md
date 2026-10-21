# deployment/ — infraestrutura que VOCÊ provisiona

A fronteira deste diretório: aqui vive a infra que **você sobe e opera**; o que roda
**dentro** do serviço gerenciado (statements Flink, configs de conector, schemas)
vive em [`confluent_cloud/`](../confluent_cloud/).

| Pasta | O quê |
|---|---|
| [`docker/`](docker/) | Sandbox local do Curso 1: Kafka KRaft single-broker + Schema Registry (`make up`) |

O ambiente cloud (environment, cluster, service accounts) é criado pelos guias do
[Curso 2, Aula 0](../docs/curso2/aula0/README.md) via Console/CLI — sem IaC neste
bootcamp, de propósito: o aluno precisa ver cada recurso nascer.
