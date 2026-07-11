# Schema Registry Skill

## Skill: `/confluent-schema`

Manage schemas in Confluent Schema Registry for Avro, JSON Schema, and Protobuf.

## Usage

```
/confluent-schema <action> [options]
```

### Actions
- `list` - List all subjects
- `get <subject>` - Get schema for subject
- `register <subject>` - Register new schema
- `delete <subject>` - Delete subject/schema
- `compatibility` - Check/set compatibility
- `versions <subject>` - List schema versions

---

## Schema Types

### Avro Schema Example

```json
{
  "type": "record",
  "name": "Membro",
  "namespace": "com.workshop.payments.beneficiarios",
  "fields": [
    {"name": "membro_id", "type": "string"},
    {"name": "cpf", "type": "string"},
    {"name": "nome", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "telefone", "type": ["null", "string"], "default": null},
    {"name": "tipo_plano", "type": {"type": "enum", "name": "TipoPlano", "symbols": ["ENFERMARIA", "APARTAMENTO", "EXECUTIVO", "PREMIUM"]}},
    {"name": "status_membro", "type": {"type": "enum", "name": "StatusMembro", "symbols": ["ATIVO", "SUSPENSO", "CANCELADO", "CARENCIA"]}},
    {"name": "data_adesao", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

### JSON Schema Example

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Membro",
  "type": "object",
  "properties": {
    "membro_id": {"type": "string"},
    "cpf": {"type": "string", "pattern": "^\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}$"},
    "nome": {"type": "string"},
    "email": {"type": "string", "format": "email"},
    "cns": {"type": "string", "pattern": "^\\d{15}$"}
  },
  "required": ["membro_id", "cpf", "nome"]
}
```

### Protobuf Schema Example

```protobuf
syntax = "proto3";

package com.workshop.payments.beneficiarios;

message Membro {
  string membro_id = 1;
  string cpf = 2;
  string nome = 3;
  optional string email = 4;
  optional string telefone = 5;
  TipoPlano tipo_plano = 6;
  StatusMembro status_membro = 7;
  int64 data_adesao = 8;
}

enum TipoPlano {
  ENFERMARIA = 0;
  APARTAMENTO = 1;
  EXECUTIVO = 2;
  PREMIUM = 3;
}

enum StatusMembro {
  ATIVO = 0;
  SUSPENSO = 1;
  CANCELADO = 2;
  CARENCIA = 3;
}
```

---

## API Commands

### List Subjects

```bash
# List all subjects
curl -s http://localhost:8081/subjects | jq

# List subjects with prefix
curl -s "http://localhost:8081/subjects?subjectPrefix=membros" | jq
```

### Get Schema

```bash
# Get latest schema for subject
curl -s http://localhost:8081/subjects/membros-value/versions/latest | jq

# Get specific version
curl -s http://localhost:8081/subjects/membros-value/versions/1 | jq

# Get schema by ID
curl -s http://localhost:8081/schemas/ids/1 | jq

# List all versions
curl -s http://localhost:8081/subjects/membros-value/versions | jq
```

### Register Schema

```bash
# Register Avro schema
curl -X POST http://localhost:8081/subjects/membros-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Membro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"nome\",\"type\":\"string\"}]}"
  }'

# Register JSON schema
curl -X POST http://localhost:8081/subjects/guias-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "JSON",
    "schema": "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"valor_autorizado\":{\"type\":\"number\"}}}"
  }'

# Register Protobuf schema
curl -X POST http://localhost:8081/subjects/events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "PROTOBUF",
    "schema": "syntax = \"proto3\"; message Event { string id = 1; string type = 2; }"
  }'
```

### Delete Schema

```bash
# Soft delete subject (all versions)
curl -X DELETE http://localhost:8081/subjects/membros-value

# Soft delete specific version
curl -X DELETE http://localhost:8081/subjects/membros-value/versions/1

# Hard delete (permanent)
curl -X DELETE "http://localhost:8081/subjects/membros-value?permanent=true"

# Hard delete specific version
curl -X DELETE "http://localhost:8081/subjects/membros-value/versions/1?permanent=true"
```

---

## Compatibility Management

### Compatibility Levels

| Level | Description |
|-------|-------------|
| `BACKWARD` | New schema can read data written by old schema |
| `BACKWARD_TRANSITIVE` | Backward compatible with all previous versions |
| `FORWARD` | Old schema can read data written by new schema |
| `FORWARD_TRANSITIVE` | Forward compatible with all previous versions |
| `FULL` | Both backward and forward compatible |
| `FULL_TRANSITIVE` | Full compatibility with all previous versions |
| `NONE` | No compatibility checking |

### Check/Set Compatibility

```bash
# Get global compatibility level
curl -s http://localhost:8081/config | jq

# Set global compatibility level
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Get subject compatibility level
curl -s http://localhost:8081/config/membros-value | jq

# Set subject compatibility level
curl -X PUT http://localhost:8081/config/membros-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL"}'

# Test compatibility before registering
curl -X POST http://localhost:8081/compatibility/subjects/membros-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{...}"}'
```

---

## Schema Evolution Best Practices

### Backward Compatible Changes (Safe)
- Add optional field with default value
- Remove field (if consumers don't require it)
- Change field to union with null

### Breaking Changes (Avoid)
- Remove required field
- Change field type
- Rename field
- Add required field without default

### Example: Safe Evolution

**Version 1:**
```json
{
  "type": "record",
  "name": "Membro",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "nome", "type": "string"}
  ]
}
```

**Version 2 (Backward Compatible):**
```json
{
  "type": "record",
  "name": "Membro",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "nome", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

---

## Python Client Example

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

# Connect to Schema Registry
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Register schema
schema_str = """
{
  "type": "record",
  "name": "Membro",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "nome", "type": "string"}
  ]
}
"""
from confluent_kafka.schema_registry import Schema
schema = Schema(schema_str, "AVRO")
schema_id = schema_registry_client.register_schema("membros-value", schema)

# Create serializer/deserializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
avro_deserializer = AvroDeserializer(schema_registry_client)
```

---

## Subject Naming Strategies

| Strategy | Pattern | Example |
|----------|---------|---------|
| TopicNameStrategy | `<topic>-key`, `<topic>-value` | `membros-value` |
| RecordNameStrategy | `<namespace>.<record>` | `com.workshop.payments.Membro` |
| TopicRecordNameStrategy | `<topic>-<namespace>.<record>` | `membros-com.workshop.payments.Membro` |

### Configure in Connect

```json
{
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy"
}
```

---

## Troubleshooting

### Common Errors

1. **409 Conflict - Incompatible schema**
   - Check compatibility settings
   - Test compatibility before registering

2. **404 Not Found - Subject not found**
   - Verify subject name
   - Check if schema was soft-deleted

3. **422 Unprocessable Entity - Invalid schema**
   - Validate schema syntax
   - Check for missing required fields
