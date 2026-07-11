---
globs: "**/tests/**/*.py,**/test_*.py,**/*_test.py"
---

# Testing Rules

## Framework
- pytest (mínimo 80% coverage em código de `shared/`)
- Hypothesis para property-based em geradores
- `conftest.py` para fixtures compartilhadas
- testcontainers para integração com Kafka / Oracle XE / Schema Registry

## Test Organization
```
tests/
├── conftest.py              # Fixtures compartilhadas (Kafka, Oracle, SR mocks)
├── generators/
│   ├── test_contas.py
│   ├── test_cartoes.py
│   └── test_transacoes.py
├── cdc/
│   ├── test_oracle_cdc_config.py
│   └── test_debezium_transforms.py
├── flink/
│   └── test_flink_jobs.py
├── iceberg/
│   └── test_sink.py
└── producers/
    └── test_kafka_producer.py
```

## Fixtures
- `@pytest.fixture` para geradores, mocks de Kafka/Oracle, dados de exemplo
- Sempre `seed=42` em testes (determinismo)
- Mockar serviços externos (Schema Registry, MinIO) — nunca chamar produção

## Generator Tests
```python
def test_generator_produces_valid_records():
    generator = TransacaoGenerator(seed=42)
    records = list(generator.generate(100))
    assert len(records) == 100
    for record in records:
        assert "transaction_id" in record
        assert record["amount"] > Decimal("0")
        assert record["account_id"] is not None

def test_generator_is_deterministic():
    gen1 = TransacaoGenerator(seed=42)
    gen2 = TransacaoGenerator(seed=42)
    assert list(gen1.generate(10)) == list(gen2.generate(10))
```

## CDC Tests
- Verificar envelope Debezium: `before`, `after`, `op` (c/u/d), `ts_ms`, `source`
- Testar todos os 3 tipos de op (create/update/delete) e seus efeitos downstream
- Testar ordering guarantees dentro de uma partition key
- Testar evolução de schema: producers com schema antigo continuam compatíveis

## Flink SQL Tests
- Testar watermarks: eventos atrasados além do delay são descartados corretamente
- Testar state TTL: janelas fecham e o state é liberado
- Testar CEP com fixtures dourados (sequências determinísticas de eventos)
- Testar fraude: `RULE_VELOCITY` dispara com 3 transações em <60s; não dispara com 2

## Iceberg Sink Tests
- Testar idempotência do MERGE: replay do mesmo batch CDC deixa o estado igual
- Testar propagação de delete: `op='d'` no source remove a row no Iceberg (upsert mode)
- Testar evolução de partitioning (Iceberg suporta sem rewrite)

## Property-Based Tests
```python
from hypothesis import given, strategies as st

@given(seed=st.integers(min_value=0, max_value=10_000))
def test_cpf_generated_is_valid(seed: int) -> None:
    """Todo CPF gerado precisa passar na validação de check digit."""
    gen = ContaGenerator(seed=seed)
    record = next(gen.generate(1))
    assert validate_cpf(record["cpf"])

@given(seed=st.integers(min_value=0, max_value=10_000))
def test_pan_is_always_masked(seed: int) -> None:
    """PAN nunca deve aparecer em formato completo no payload."""
    gen = CartaoGenerator(seed=seed)
    record = next(gen.generate(1))
    assert "X" in record["pan_masked"] or "*" in record["pan_masked"]
    assert len(record["pan_masked"].replace("X", "").replace("*", "")) == 10  # BIN(6) + last4
```

## Integration Tests
- testcontainers para Kafka + Schema Registry
- E2E: Oracle fixture → Debezium → Kafka → Flink → Iceberg/MinIO assertion
- Exactly-once: zero duplicados/perdidos após restart
- **Nunca** apontar testes de integração para Confluent Cloud ou MinIO de produção
