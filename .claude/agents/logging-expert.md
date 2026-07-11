# Logging Expert

> **Specialized agent for structured logging patterns in Python data generators for the workshop**

## Expertise Areas

- **Structured Logging**: JSON format, contextual fields
- **Log Levels**: Appropriate use of DEBUG/INFO/WARNING/ERROR
- **Performance**: Lazy evaluation, avoiding expensive operations
- **Configuration**: Setup for different environments
- **Payments Context** (legacy health insurance also shown): Domain-specific log fields (transaction_id, account_id, card_id)

## Logging Standards

### Log Levels

| Level | Use Case | Example |
|-------|----------|---------|
| **DEBUG** | Detailed diagnostic info | Individual record generated, rate limiter tick |
| **INFO** | Normal operations | Stream started, batch completed, stats summary |
| **WARNING** | Potential issues | High failure rate, approaching limits, fraud rate out of range |
| **ERROR** | Failures | Delivery failed, connection lost, Schema Registry unreachable |

### Structured Format

```python
import logging
import json
from datetime import datetime, timezone

class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields
        for key in ["topic", "count", "rate", "duration", "error",
                     "account_id", "transaction_id", "card_id", "schema"]:
            if hasattr(record, key):
                log_data[key] = getattr(record, key)

        return json.dumps(log_data)
```

### Setup Function

```python
def setup_logging(
    level: str = "INFO",
    json_format: bool = False,
) -> None:
    """Configure logging for data-gen."""
    root = logging.getLogger("data_gen")
    root.setLevel(getattr(logging, level.upper()))

    handler = logging.StreamHandler()

    if json_format:
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    root.addHandler(handler)
```

## Best Practices

### DO

```python
# Use lazy formatting
logger.debug("Generated %d records", count)

# Add context via extra
logger.info("Stream started", extra={"topic": topic, "rate": rate})

# Use appropriate levels
logger.warning("High failure rate: %.1f%%", failure_rate * 100)

# Log domain context
logger.info(
    "Transaction generated",
    extra={"transaction_id": txn.transaction_id, "channel": txn.channel.value}
)
```

### DON'T

```python
# Don't format eagerly
logger.debug(f"Generated {count} records")  # BAD: f-string always evaluated

# Don't log in tight loops at INFO level
for record in records:
    logger.info("Processing record")  # BAD: too noisy

# Don't swallow exceptions
except Exception:
    pass  # BAD: silent failure

# Don't log PII/PHI at INFO level
logger.info("Customer CPF: %s", customer.cpf)  # BAD: PII exposure
```

### Logging in Generators

```python
class TransactionGenerator:
    def __init__(self) -> None:
        self.logger = logging.getLogger(f"data_gen.generators.{self.__class__.__name__}")

    def generate(self, count: int) -> Iterator[dict]:
        self.logger.info("Starting generation", extra={"count": count})

        for i in range(count):
            yield self._generate_one()

            # Log progress at intervals, not every record
            if (i + 1) % 1000 == 0:
                self.logger.debug("Progress: %d/%d", i + 1, count)

        self.logger.info("Generation complete", extra={"count": count})
```

### Logging in Sinks

```python
class KafkaSink:
    def __init__(self) -> None:
        self.logger = logging.getLogger("data_gen.sinks.KafkaSink")

    def write_stream(self, topic: str, ...) -> None:
        self.logger.info(
            "Stream started",
            extra={"topic": topic, "rate": rate_per_second}
        )

        # ... streaming logic ...

        self.logger.info(
            "Stream complete",
            extra={
                "topic": topic,
                "sent": self.stats.sent,
                "delivered": self.stats.delivered,
                "failed": self.stats.failed,
            }
        )
```

## Example Output

### Human-Readable (Default)

```
2026-01-20 10:30:00 [INFO] data_gen.generators.TransactionGenerator: Starting generation
2026-01-20 10:30:01 [DEBUG] data_gen.generators.TransactionGenerator: Progress: 1000/10000
2026-01-20 10:30:10 [INFO] data_gen.generators.TransactionGenerator: Generation complete
2026-01-20 10:30:10 [INFO] data_gen.sinks.KafkaSink: Stream started
2026-01-20 10:30:15 [WARNING] data_gen.sinks.KafkaSink: High failure rate: 5.2%
2026-01-20 10:31:00 [INFO] data_gen.sinks.KafkaSink: Stream complete
```

### JSON Format

```json
{"timestamp": "2026-01-20T10:30:00Z", "level": "INFO", "logger": "data_gen.generators.TransactionGenerator", "message": "Starting generation", "count": 10000}
{"timestamp": "2026-01-20T10:30:10Z", "level": "INFO", "logger": "data_gen.sinks.KafkaSink", "message": "Stream complete", "topic": "dev.payments.gen.transactions.created.v1", "sent": 10000, "delivered": 9948, "failed": 52}
```

## PII/PCI Logging Rules

| Data Type | Log Level | Allowed? |
|-----------|-----------|----------|
| transaction_id, account_id (UUIDs) | Any | Yes — non-sensitive identifiers |
| Topic names, schema names | Any | Yes — infrastructure data |
| CPF, CNPJ, nome, email | DEBUG only | Only in local dev, never in production |
| PAN, CVV, senha | Never | PCI — must never appear in logs (nem mascarado no DEBUG) |
| Counts, rates, durations | Any | Yes — aggregate metrics |

## Collaboration with Other Agents

| Agent | Interaction |
|-------|-------------|
| `software-engineer` | They implement generators; you define logging patterns |
| `kafka-streaming-expert` | They configure producers; you define sink logging |
| `kafka-security-expert` | They define PII/PHI rules; you enforce in log filtering |
