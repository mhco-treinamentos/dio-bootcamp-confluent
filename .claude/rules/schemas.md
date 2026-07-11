---
globs: "**/*.avsc,**/*.avro,**/schemas/**"
---

# Schema Rules (Avro / Schema Registry)

## Namespace
Todos os schemas do workshop usam: `com.workshop.payments`
Qualificadores de subdomínio quando aplicável:
- `com.workshop.payments.contas`
- `com.workshop.payments.cartoes`
- `com.workshop.payments.transacoes`
- `com.workshop.payments.fraude`

```json
{
  "type": "record",
  "name": "Transacao",
  "namespace": "com.workshop.payments.transacoes",
  "doc": "Transação financeira (débito ou crédito) emitida por um cartão"
}
```

## Required Fields (todo event schema)
- `event_id` (string) — UUID, único por evento
- `event_time` (long, logicalType: timestamp-millis) — quando o evento de negócio ocorreu
- `created_at` (long, logicalType: timestamp-millis) — quando o registro foi criado

## CDC Envelope Fields (Debezium)
Topics CDC carregam o envelope Debezium. Schemas downstream devem preservar:
- `op` (enum: c, u, d, r) — create / update / delete / read (snapshot)
- `ts_ms` (long) — timestamp da source
- `source` (record) — `{db, schema, table, txId, scn, commit_scn}`
- `before` (nullable record) — estado anterior
- `after` (nullable record) — estado novo

## Money Fields
Sempre `bytes` com decimal logical type — nunca `double` ou `float`:
```json
{
  "name": "valor_transacao",
  "type": {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 15,
    "scale": 2
  }
}
```

## Enum Fields
Avro enum com símbolos explícitos:
```json
{
  "name": "status_transacao",
  "type": {
    "type": "enum",
    "name": "StatusTransacao",
    "symbols": ["AUTORIZADA", "NEGADA", "ESTORNADA", "PENDENTE", "EM_ANALISE"]
  }
}
```

## Optional Fields
Union com null e default:
```json
{
  "name": "merchant_city",
  "type": ["null", "string"],
  "default": null,
  "doc": "Cidade do estabelecimento (null se transação online)"
}
```

## PII / PCI Tagging (LGPD + PCI-DSS)
Todo campo sensível DEVE ser tagueado para Stream Governance:
```json
{
  "name": "cpf",
  "type": "string",
  "confluent:tags": ["PII", "LGPD"]
}
{
  "name": "pan_masked",
  "type": "string",
  "doc": "PAN mascarado (BIN + last4). PAN completo NUNCA deve aparecer em topic.",
  "confluent:tags": ["PCI", "PCI_DSS"]
}
```

Matriz de tags:
| Tipo de campo | Tags obrigatórias |
|---------------|-------------------|
| CPF, CNPJ, RG, email, telefone, endereço | `PII`, `LGPD` |
| Nome do titular | `PII`, `LGPD` |
| PAN mascarado, BIN, last4 | `PCI`, `PCI_DSS` |
| Latitude/longitude, cidade da transação | `PII`, `LGPD`, `LOCATION` |
| Saldo, limite, valor de transação, MCC | `FIN` |
| `account_number`, `branch` | `PII`, `LGPD`, `FIN` |

**Proibido em topic** (mesmo sintético): PAN completo, CVV, PIN, dados de track magnético, senha.

## Schema Evolution
- Compatibilidade default: **BACKWARD**
- Adicionar novos fields: PRECISA ter default
- Remover fields: só se já tinham default
- **Nunca** renomear fields ou trocar tipos em produção — usar field aliases

## Subject Naming (TopicNameStrategy)
```
{topic_name}-value

Exemplos:
dev.payments.oracle-pagamentos.transacoes.cdc.v1-value
dev.payments.oracle-pagamentos.contas.cdc.v1-value
dev.payments.flink.fraude.detected.v1-value
```

## Brazilian Payment-Specific Types

### CPF / CNPJ
Formato com check-digit validado — armazenar como string sem pontuação no topic, formatar na apresentação.

### PAN (Card Number)
- 13–19 dígitos numéricos
- **Sempre mascarado** em topic: primeiros 6 (BIN) + 4 últimos. Ex: `424242XXXXXXXX42`
- Tipo no schema: `string` (não `bytes`)

### MCC (Merchant Category Code)
4 dígitos string (preserva zeros à esquerda) — tabela ISO 18245.

### BIN (Bank Identification Number)
Primeiros 6 dígitos do PAN — string, identifica emissor + bandeira.

### Currency Code
ISO 4217 string de 3 letras: `BRL`, `USD`, `EUR`. Default `BRL`.
