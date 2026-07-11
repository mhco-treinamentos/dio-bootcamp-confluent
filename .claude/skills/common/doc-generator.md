# Skill: Documentation Generator

> **Generate documentation for data generators and schemas**

## Purpose

Create documentation for:
- Generator classes and methods
- Avro/JSON schemas
- Configuration files
- API endpoints

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `target` | Yes | Code/schema to document |
| `format` | No | Output format (markdown, docstring, README) |

## Process

1. **Analyze Code**: Parse structure, methods, parameters
2. **Extract Examples**: Generate usage examples
3. **Document Schema**: Describe fields, types, constraints
4. **Create Sections**: Overview, usage, configuration, examples

## Output

```markdown
# GuiaGenerator

Generate synthetic guia TISS records with realistic patterns.

## Usage

```python
from generators.saude import GuiaGenerator

gen = GuiaGenerator(seed=42)
for guia in gen.generate(1000):
    print(guia)
```

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `seed` | int | None | Random seed for reproducibility |
| `batch_size` | int | 100 | Records per batch |

## Schema

| Field | Type | Description |
|-------|------|-------------|
| `guia_id` | string | Unique identifier (UUID) |
| `valor_autorizado` | decimal | Valor autorizado em BRL |
```

## Example Usage

```
"Document the GuiaGenerator class"
"Generate README for the saude generators module"
```
