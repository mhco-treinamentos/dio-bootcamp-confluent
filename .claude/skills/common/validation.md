# Skill: Validation

> **Validate configurations, schemas, and generated data**

## Purpose

Validate:
- Kafka configurations
- Avro/JSON schemas
- Generator configurations
- Generated data samples

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `target` | Yes | Configuration/schema to validate |
| `type` | Yes | Type: kafka, avro, json, generator, data |

## Process

1. **Parse Target**: Load and parse the configuration
2. **Schema Validation**: Check against expected structure
3. **Value Validation**: Verify values are within bounds
4. **Compatibility Check**: Verify compatibility with existing schemas
5. **Report**: Generate validation report

## Validation Rules

### Kafka Configuration
- Required fields present (bootstrap.servers)
- Valid producer settings
- Security configuration if needed

### Avro Schema
- Valid Avro syntax
- Field types supported
- Default values valid
- Logical types correct

### Generator Configuration
- Required parameters present
- Valid ranges and types
- Seed reproducibility

## Output

```markdown
## Validation Report

### Target: producer_config.json
### Type: Kafka Configuration

### Status: PASSED with warnings

### Checks
- [PASS] bootstrap.servers present
- [PASS] client.id configured
- [WARN] acks=1, consider acks=all for reliability
- [PASS] compression configured

### Recommendations
- Set acks=all for production reliability
```

## Example Usage

```
"Validate this Kafka producer configuration"
"Check this Avro schema for compatibility"
```
