# Skill: Schema Designer

> **Design Avro and JSON schemas for synthetic data entities**

## Purpose

Design schemas for:
- Domain entities (payments: customers, accounts, cards, merchants)
- Event entities (transactions, refunds, fraud alerts)
- Custom domains

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `entity` | Yes | Entity to design schema for |
| `domain` | Yes | Domain (payments — default do bootcamp) |
| `format` | No | Schema format (avro, json) - default: avro |
| `requirements` | No | Specific field requirements |

## Process

1. **Analyze Domain**: Understand entity context
2. **Identify Fields**: Core fields, metadata, relationships
3. **Choose Types**: Appropriate data types and logical types
4. **Add Constraints**: Validation rules, defaults, PII/PCI tags
5. **Document**: Field descriptions and examples

## Output Example

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.bootcamp.payments",
  "doc": "Transação de pagamento (cartão/PIX)",
  "fields": [
    {
      "name": "transaction_id",
      "type": "string",
      "doc": "Identificador único da transação"
    },
    {
      "name": "account_id",
      "type": "string",
      "doc": "ID da conta",
      "confluent:tags": ["PII", "LGPD"]
    },
    {
      "name": "card_id",
      "type": ["null", "string"],
      "default": null,
      "doc": "ID do cartão (null para PIX/boleto)"
    },
    {
      "name": "channel",
      "type": {
        "type": "enum",
        "name": "Channel",
        "symbols": ["PIX", "CREDIT", "DEBIT", "BOLETO"]
      },
      "doc": "Canal da transação"
    },
    {
      "name": "amount",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 15,
        "scale": 2
      },
      "doc": "Valor em BRL"
    },
    {
      "name": "merchant_mcc",
      "type": ["null", "string"],
      "default": null,
      "doc": "MCC do estabelecimento (ISO 18245)"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "TransactionStatus",
        "symbols": ["CREATED", "AUTHORIZED", "SETTLED", "DECLINED", "REFUNDED"]
      }
    },
    {
      "name": "created_at",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Data/hora de criação da transação"
    }
  ]
}
```

> **Regras completas** (namespace, tags PII/PCI obrigatórias, envelope Debezium, compatibilidade BACKWARD): [rules/schemas.md](../../rules/schemas.md). O hook `validate-avro-pii.sh` bloqueia schemas com campos sensíveis sem `confluent:tags`.

## Example Usage

```
"Design an Avro schema for payment transactions"
"Create a JSON schema for accounts/customers"
"Design a merchant schema with MCC enum"
"Create an Avro schema for refunds com reason patterns"
```

---

**Última atualização**: 2026-07-11
