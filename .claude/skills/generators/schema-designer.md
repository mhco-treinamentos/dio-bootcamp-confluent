# Skill: Schema Designer

> **Design Avro and JSON schemas for synthetic data entities**

## Purpose

Design schemas for:
- Domain entities (legacy health: membros, planos, guias TISS, prestadores; current payments: contas, cartoes, transacoes)
- Financial entities (mensalidades, coparticipacoes, faturas, glosas)
- Clinical entities (laudos, procedimentos, diagnosticos)
- Custom domains

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `entity` | Yes | Entity to design schema for |
| `domain` | Yes | Domain (saude, financeiro, clinico) |
| `format` | No | Schema format (avro, json) - default: avro |
| `requirements` | No | Specific field requirements |

## Process

1. **Analyze Domain**: Understand entity context
2. **Identify Fields**: Core fields, metadata, relationships
3. **Choose Types**: Appropriate data types and logical types
4. **Add Constraints**: Validation rules, defaults
5. **Document**: Field descriptions and examples

## Output Example

```json
{
  "type": "record",
  "name": "GuiaTISS",
  "namespace": "com.workshop.payments.autorizacoes",
  "doc": "Guia TISS de autorização de procedimento de saúde",
  "fields": [
    {
      "name": "guia_id",
      "type": "string",
      "doc": "Identificador único da guia"
    },
    {
      "name": "numero_guia",
      "type": "string",
      "doc": "Número da guia TISS"
    },
    {
      "name": "tipo_guia",
      "type": {
        "type": "enum",
        "name": "TipoGuia",
        "symbols": ["CONSULTA", "SP_SADT", "INTERNACAO", "ODONTOLOGICA", "HONORARIOS"]
      },
      "doc": "Tipo da guia conforme padrão TISS"
    },
    {
      "name": "valor_autorizado",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2
      },
      "doc": "Valor autorizado em BRL"
    },
    {
      "name": "membro_id",
      "type": "string",
      "doc": "ID do beneficiário/membro"
    },
    {
      "name": "prestador_id",
      "type": "string",
      "doc": "ID do prestador de serviço"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "GuiaStatus",
        "symbols": ["SOLICITADA", "EM_ANALISE", "AUTORIZADA", "NEGADA", "EXECUTADA", "FATURADA", "PAGA", "GLOSADA"]
      }
    },
    {
      "name": "data_solicitacao",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Data/hora da solicitação da guia"
    }
  ]
}
```

## Example Usage

```
"Design an Avro schema for guias TISS de autorização"
"Create a JSON schema for membros/beneficiários"
"Design a prestador schema for the saude domain"
"Create an Avro schema for glosas de faturamento"
```
