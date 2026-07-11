# Kafka Connect (Managed) Skill

## Skill: `/confluent-connect`

Gerenciamento geral de **conectores gerenciados** no Confluent Cloud: descobrir plugins, validar configs, ciclo de vida (create/pause/resume/update/delete) e monitoramento via CLI `confluent` (v4).

> **Connect é cloud-only neste bootcamp** — o fallback local ([local-dev-fallback.md](local-dev-fallback.md)) não tem worker de Connect. Para o passo a passo do source Debezium Postgres e sinks do módulo 3 (SMT, DLQ, banco fonte), ver [managed-connector.md](managed-connector.md).

## Usage

```
/confluent-connect <action> [options]
```

### Actions
- `plugins` — Listar/descrever plugins disponíveis
- `create` — Criar conector a partir de JSON
- `status` — Estado de conectores e tasks
- `pause|resume|delete <id>` — Ciclo de vida
- `update <id>` — Atualizar configuração
- `events` — Logs/eventos de conectores

---

## Descobrir plugins e configs obrigatórias

```bash
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>

# Catálogo de conectores gerenciados disponível na região
confluent connect plugin list
confluent connect plugin list | grep -iE "postgres|s3|snowflake|mongo"

# Campos obrigatórios/opcionais do plugin (fonte da verdade para o JSON)
confluent connect plugin describe PostgresCdcSourceV2
confluent connect plugin describe S3_SINK
```

### Plugins usados no bootcamp

| Plugin | Tipo | Módulo |
|--------|------|--------|
| `PostgresCdcSourceV2` (Debezium) | Source CDC | 3 |
| `DatagenSource` | Source sintético (mock rápido) | 2–3 |
| `S3_SINK` | Sink Object Storage | 3 e 5 (comparação com Tableflow) |
| `SnowflakeSink` | Sink DW | 3 |
| `MongoDbAtlasSink` | Sink NoSQL | 3 |

---

## Ciclo de vida

```bash
# Criar (config JSON versionada no lab, segredos via env/placeholder)
confluent connect cluster create --config-file connector.json

# Listar / inspecionar
confluent connect cluster list
confluent connect cluster describe <lcc-id>

# Pausar / retomar / atualizar / deletar
confluent connect cluster pause <lcc-id>
confluent connect cluster resume <lcc-id>
confluent connect cluster update <lcc-id> --config-file connector.json
confluent connect cluster delete <lcc-id> --force     # teardown do lab
```

### Exemplo mínimo: DatagenSource (mock antes do CDC real)

```json
{
  "name": "datagen-transactions",
  "connector.class": "DatagenSource",
  "kafka.auth.mode": "SERVICE_ACCOUNT",
  "kafka.service.account.id": "<sa-id>",
  "kafka.topic": "lab.payments.gen.transactions.created.v1",
  "output.data.format": "AVRO",
  "quickstart": "TRANSACTIONS",
  "max.interval": "1000",
  "tasks.max": "1"
}
```

```bash
confluent connect cluster create --config-file datagen-transactions.json
confluent kafka topic consume lab.payments.gen.transactions.created.v1 --value-format avro
```

---

## Monitoramento

```bash
# Status resumido de todos os conectores
confluent connect cluster list -o json | jq -r '.[] | "\(.name): \(.status)"'

# Detalhe por task (trace de erro aparece aqui)
confluent connect cluster describe <lcc-id>

# Eventos/logs de conectores do environment
confluent connect event describe
```

Estados possíveis: `PROVISIONING` → `RUNNING` | `PAUSED` | `FAILED`. Conector `FAILED` com `errors.tolerance=all` configurado geralmente indica problema de conexão/auth, não de dado (dado ruim vai para a DLQ — ver [managed-connector.md](managed-connector.md)).

---

## Custos e teardown

- Conector gerenciado cobra **por task-hora + throughput** — mesmo parado em `RUNNING` sem tráfego há custo de task
- `pause` reduz mas não zera; **delete no fim do lab** (entra no `teardown.sh` — [cloud-setup.md](cloud-setup.md))
- `tasks.max=1` em todos os labs — throughput dos labs não justifica mais

## Troubleshooting rápido

| Sintoma | Verificar |
|---------|-----------|
| `FAILED` ao criar | `confluent connect cluster describe` → trace; geralmente rede/credencial do sistema externo |
| Sem dados no topic | Fonte tem dados novos? `snapshot.mode`? SMT filtrando tudo? |
| Erro de serialização | `output.data.format` vs. schema no SR; compatibilidade BACKWARD |
| ACL negada | Service account do conector tem write no prefixo do topic? (`confluent kafka acl list --service-account <sa-id>`) |

Diagnóstico completo → [troubleshoot.md](troubleshoot.md).

---

**Última atualização**: 2026-07-11
