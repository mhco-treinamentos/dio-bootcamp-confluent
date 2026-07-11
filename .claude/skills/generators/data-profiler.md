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

### Dataset: transactions.json
### Records: 10,000

---

### Field: transaction_id
- Type: string (UUID)
- Unique: 10,000 (100%)
- Nulls: 0 (0%)
- Format: Valid UUIDs

### Field: amount
- Type: decimal
- Min: 1.00
- Max: 9,852.45
- Mean: 134.56
- Median: 81.23
- Std Dev: 324.67
- Distribution: Log-normal, median ~R$80 (as expected — rules.md)

### Field: created_at
- Type: timestamp
- Range: 2026-06-11 to 2026-07-11
- Bimodal peaks (11-13h, 18-21h): 68% (expected: 70%)
- Weekend: 27%

### Field: status
- Type: enum
- Values:
  - SETTLED: 88%
  - AUTHORIZED: 7%
  - DECLINED: 5%

### Field: fraud_flag
- Rate: 0.9% (expected: 0.5-1.5%)
- Geo correlation (UF card × UF txn): 87% (expected: ≥85%)

---

### Quality Score: 95/100

### Issues
- [INFO] 2 transactions with amount > 8,000 (rare but valid — long tail)

### Recommendations
- Consider adding more BOLETO transactions for channel realism
```

## Example Usage

```
"Profile the generated transaction data"
"Analyze the account dataset for Pareto distribution"
"Validate merchant data against the schema"
```
