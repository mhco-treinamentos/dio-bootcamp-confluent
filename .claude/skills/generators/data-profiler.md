# Skill: Data Profiler

> **Profile and analyze generated synthetic data for quality and realism**

## Purpose

Profile synthetic data for:
- Statistical distribution analysis
- Uniqueness and completeness checks
- Referential integrity validation
- Anomaly detection

## Inputs

| Input | Required | Description |
|-------|----------|-------------|
| `data` | Yes | Dataset to profile (list, DataFrame, file) |
| `schema` | No | Expected schema for validation |
| `checks` | No | Specific checks to run |

## Process

1. **Load Data**: Parse input data
2. **Basic Stats**: Count, nulls, uniques
3. **Distribution Analysis**: Histograms, percentiles
4. **Integrity Checks**: Foreign keys, constraints
5. **Report**: Generate profiling report

## Output Example

```markdown
## Data Profile Report

### Dataset: guias.json
### Records: 10,000

---

### Field: guia_id
- Type: string (UUID)
- Unique: 10,000 (100%)
- Nulls: 0 (0%)
- Format: Valid UUIDs

### Field: valor_autorizado
- Type: decimal
- Min: 50.00
- Max: 98,523.45
- Mean: 1,342.56
- Median: 287.23
- Std Dev: 3,245.67
- Distribution: Pareto (as expected)

### Field: data_solicitacao
- Type: timestamp
- Range: 2025-01-01 to 2025-01-20
- Business hours: 78% (expected: 80%)
- Weekend: 8%

### Field: status
- Type: enum
- Values:
  - SOLICITADA: 25%
  - EM_ANALISE: 15%
  - AUTORIZADA: 35%
  - EXECUTADA: 25%

---

### Quality Score: 95/100

### Issues
- [INFO] 2 guias with valor_autorizado > 80,000 (rare but valid)

### Recommendations
- Consider adding more INTERNACAO guias for realism
```

## Example Usage

```
"Profile the generated guia TISS data"
"Analyze the membro dataset for distribution"
"Validate prestador data against the schema"
```
