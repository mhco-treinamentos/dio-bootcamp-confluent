# Schema Registry Skill

## Skill: `/confluent-schema`

Gerenciar schemas (Avro/JSON Schema/Protobuf) no **Schema Registry do Confluent Cloud** via CLI `confluent` (v4). Base dos Data Contracts do módulo 2. Fallback local (REST em `localhost:8081`) só onde indicado.

## Usage

```
/confluent-schema <action> [options]
```

### Actions
- `list` — Listar subjects
- `describe <subject>` — Ver schema/versões
- `create <subject>` — Registrar schema
- `compatibility` — Ver/definir compatibilidade
- `delete <subject>` — Remover subject/versão
- `exporter` — (avançado) exportar schemas entre SRs

---

## Contexto e credenciais

```bash
confluent environment use <env-id>
confluent schema-registry cluster describe        # endpoint (SR_URL) + id lsrc-xxxxx

# API key do SR (ver cloud-setup.md); a CLI usa o login, clients usam SR_API_KEY/SECRET
```

## Schema do bootcamp (exemplo)

Namespace Avro: **`com.bootcamp.payments`**; campos sensíveis com `confluent:tags` (hook bloqueia `.avsc` sem tags — ver [rules/schemas.md](../../rules/schemas.md)).

```json
{
  "type": "record",
  "name": "TransactionCreated",
  "namespace": "com.bootcamp.payments",
  "doc": "Evento de transacao criada (PIX/cartao)",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "card_id", "type": ["null", "string"], "default": null},
    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}, "doc": "Valor em BRL"},
    {"name": "currency", "type": "string", "default": "BRL"},
    {"name": "mcc", "type": ["null", "string"], "default": null},
    {"name": "uf", "type": "string"},
    {"name": "cpf", "type": "string", "confluent:tags": ["PII", "LGPD"]},
    {"name": "card_pan_masked", "type": ["null", "string"], "default": null, "confluent:tags": ["PCI"]},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

---

## Comandos (Confluent Cloud CLI)

### Listar / inspecionar

```bash
confluent schema-registry subject list
confluent schema-registry subject list --prefix "dev.payments"

# Últimas versões de um subject
confluent schema-registry schema describe \
  --subject dev.payments.gen.transactions.created.v1-value \
  --version latest

confluent schema-registry schema list \
  --subject-prefix dev.payments.gen.transactions.created.v1-value
```

### Registrar

```bash
# Avro (arquivo .avsc versionado em repo/schemas/payments/)
confluent schema-registry schema create \
  --subject dev.payments.gen.transactions.created.v1-value \
  --schema ./schemas/payments/transaction_created.avsc \
  --type avro

# JSON Schema / Protobuf
confluent schema-registry schema create --subject <subject> --schema ./schema.json --type json
confluent schema-registry schema create --subject <subject> --schema ./schema.proto --type protobuf
```

### Compatibilidade

```bash
# Ver config global e por subject
confluent schema-registry configuration describe
confluent schema-registry subject describe dev.payments.gen.transactions.created.v1-value

# Definir BACKWARD (default do bootcamp — Curso 2, Aula 3)
confluent schema-registry subject update dev.payments.gen.transactions.created.v1-value \
  --compatibility backward
```

| Nível | Significado |
|-------|-------------|
| `BACKWARD` | Schema novo lê dados do schema antigo (**default do bootcamp**) |
| `BACKWARD_TRANSITIVE` | Backward contra todas as versões |
| `FORWARD` / `FORWARD_TRANSITIVE` | Schema antigo lê dados do novo |
| `FULL` / `FULL_TRANSITIVE` | Ambos |
| `NONE` | Sem verificação — proibido nos labs |

### Deletar (teardown / correções)

```bash
# Soft delete (recuperável)
confluent schema-registry schema delete \
  --subject dev.payments.gen.transactions.created.v1-value --version latest

# Hard delete (permanente — só em teardown de lab)
confluent schema-registry schema delete \
  --subject dev.payments.gen.transactions.created.v1-value --version all --permanent
```

---

## Evolução de schema (BACKWARD)

### Mudanças seguras
- Adicionar campo **opcional com default** (`["null", "string"], "default": null`)
- Remover campo que tinha default
- Alargar união para incluir `null`

### Breaking (bloqueadas pelo SR em BACKWARD)
- Adicionar campo obrigatório sem default
- Mudar tipo de campo / renomear campo
- Remover campo obrigatório

```json
// v1 → v2 backward-compatible: novo campo opcional
{"name": "fraud_score", "type": ["null", "double"], "default": null}
```

Lab do módulo 2: registrar v1, produzir, registrar v2 com campo novo, mostrar consumer antigo continuando a funcionar; depois tentar mudança breaking e ver o **409** do SR.

---

## Cliente Python (perfil cloud|local)

```python
import os

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

if os.environ.get("KAFKA_PROFILE", "cloud") == "cloud":
    sr_conf = {
        "url": os.environ["SR_URL"],
        "basic.auth.user.info": f"{os.environ['SR_API_KEY']}:{os.environ['SR_API_SECRET']}",
    }
else:  # fallback local — ver local-dev-fallback.md
    sr_conf = {"url": "http://localhost:8081"}

sr_client = SchemaRegistryClient(sr_conf)

with open("schemas/payments/transaction_created.avsc") as f:
    schema = Schema(f.read(), "AVRO")
schema_id = sr_client.register_schema(
    "dev.payments.gen.transactions.created.v1-value", schema
)
```

---

## Subject naming strategies

| Strategy | Padrão | Uso no bootcamp |
|----------|--------|-----------------|
| TopicNameStrategy | `<topic>-key` / `<topic>-value` | **Default — usar sempre** |
| RecordNameStrategy | `<namespace>.<record>` | Só se múltiplos tipos por topic |
| TopicRecordNameStrategy | `<topic>-<namespace>.<record>` | Idem |

---

## Fallback local (REST, sem auth)

```bash
curl -s http://localhost:8081/subjects | jq
curl -s http://localhost:8081/subjects/<subject>/versions/latest | jq
curl -X POST http://localhost:8081/subjects/<subject>/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'
```

## Troubleshooting

| Erro | Causa | Correção |
|------|-------|----------|
| `409 Conflict` | Schema incompatível com BACKWARD | Tornar o campo opcional com default; nunca baixar para NONE nos labs |
| `404 Subject not found` | Nome errado ou soft-deleted | `subject list`; conferir sufixo `-value` |
| `422 Invalid schema` | Sintaxe Avro inválida | Validar o `.avsc` (o hook de PII também valida estrutura) |
| `401/403` no client | `SR_API_KEY/SECRET` errados ou de outro environment | Recriar key contra o `lsrc-` correto |

---

**Última atualização**: 2026-07-11
