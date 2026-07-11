# Skill: Code Reviewer (Software Engineering Expert)

> **Expert code review focused on simplicity, correct patterns, and avoiding over-engineering**

## Purpose

Review Python code with a focus on:
- **Simplicity**: Is this the simplest solution that works?
- **Correct Patterns**: Are we using appropriate design patterns?
- **No Over-Engineering**: Are we adding unnecessary complexity?
- **Readability**: Can a junior developer understand this?
- **Maintainability**: Will this be easy to change later?
- **Security**: No hardcoded secrets, safe patterns

## Core Principles

### 1. YAGNI (You Aren't Gonna Need It)
- Don't add features "just in case"
- Don't create abstractions for single use cases
- Don't add configurability that isn't needed

### 2. KISS (Keep It Simple, Stupid)
- Prefer straightforward solutions over clever ones
- Three similar lines of code > premature abstraction
- Explicit > implicit

### 3. Worse is Better
- A simpler solution that works is better than a perfect complex one
- Ship working code, refactor when needed
- Don't optimize prematurely

---

## Review Checklist

### Complexity Red Flags

| Anti-Pattern | Better Alternative |
|--------------|-------------------|
| Abstract base class for 1 implementation | Just use the concrete class |
| Factory pattern for 2 classes | Simple if/else or dict lookup |
| Strategy pattern for 1 strategy | Inline the logic |
| Dependency injection framework | Constructor parameters |
| Custom exceptions for everything | Use built-in exceptions |
| Wrapper classes that just delegate | Use the wrapped object directly |
| Config objects with 20+ fields | Split into smaller configs or use defaults |
| Generic type parameters everywhere | Use concrete types until generics are needed |

### Questions to Ask

1. **Is this abstraction earning its keep?**
   - Does it have 2+ implementations?
   - Does it simplify the calling code significantly?
   - If no to both → remove it

2. **Can this be a function instead of a class?**
   - No state between calls → use function
   - State is just config → use function with parameters
   - Only `__init__` and one method → probably a function

3. **Is this indirection necessary?**
   - Can I understand what happens without jumping to another file?
   - Is the extra layer adding value or just adding navigation?

4. **Am I solving a problem I actually have?**
   - Is this handling an error that can't happen?
   - Is this validating data that's already validated?
   - Is this future-proofing for a future that won't come?

---

## Code Smells to Flag

### Over-Abstraction
```python
# BAD: Abstract base class for single implementation
class BaseGenerator(ABC):
    @abstractmethod
    def generate(self): pass

class TransactionGenerator(BaseGenerator):
    def generate(self): ...

# GOOD: Just the class you need
class TransactionGenerator:
    def generate(self): ...
```

### Unnecessary Wrapper
```python
# BAD: Wrapper that just delegates
class KafkaProducerWrapper:
    def __init__(self, producer):
        self._producer = producer

    def produce(self, topic, value):
        self._producer.produce(topic, value)

# GOOD: Use Producer directly, or add real value
producer = Producer(config)
producer.produce(topic, value)
```

### Over-Configured
```python
# BAD: Config for everything
@dataclass
class GeneratorConfig:
    seed: int | None = None
    locale: str = "pt_BR"
    batch_size: int = 100
    enable_logging: bool = True
    log_level: str = "INFO"
    log_format: str = "%(message)s"
    validate_output: bool = True
    validation_strict: bool = False
    output_format: str = "dict"
    include_metadata: bool = False
    # ... 20 more fields

# GOOD: Only what you actually use
@dataclass
class GeneratorConfig:
    seed: int | None = None
    locale: str = "pt_BR"
```

### Premature Generics
```python
# BAD: Generic when you only use one type
class DataProcessor(Generic[T]):
    def process(self, data: T) -> T: ...

# Used only as:
processor: DataProcessor[dict] = DataProcessor()

# GOOD: Use concrete type
class DataProcessor:
    def process(self, data: dict) -> dict: ...
```

### Unnecessary Interface
```python
# BAD: Interface with single implementation
class IGenerator(Protocol):
    def generate(self, count: int) -> Iterator[dict]: ...

class TransactionGenerator:  # implements IGenerator
    def generate(self, count: int) -> Iterator[dict]: ...

# GOOD: Skip the interface until you need polymorphism
class TransactionGenerator:
    def generate(self, count: int) -> Iterator[dict]: ...
```

### Factory Overkill
```python
# BAD: Factory for simple object creation
class GeneratorFactory:
    @staticmethod
    def create(type: str) -> Generator:
        if type == "transaction":
            return TransactionGenerator()
        elif type == "account":
            return AccountGenerator()
        raise ValueError(f"Unknown type: {type}")

# GOOD: Dict lookup or direct instantiation
GENERATORS = {
    "transaction": TransactionGenerator,
    "account": AccountGenerator,
}
generator = GENERATORS[type]()

# Or just:
generator = TransactionGenerator()
```

---

## What Good Code Looks Like

### Simple Generator
```python
"""Payment transaction generator - simple and direct."""

from dataclasses import dataclass
from datetime import datetime
from typing import Iterator
import random

from faker import Faker


@dataclass
class TransactionConfig:
    """Minimal config - only what we actually use."""

    seed: int | None = None


class TransactionGenerator:
    """Generate synthetic payment transactions."""

    def __init__(self, config: TransactionConfig | None = None) -> None:
        self.config = config or TransactionConfig()
        self.fake = Faker("pt_BR")
        if self.config.seed:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)

    def generate(self, count: int) -> Iterator[dict]:
        """Generate transactions. Simple loop, no magic."""
        for _ in range(count):
            yield {
                "transaction_id": self.fake.uuid4(),
                "amount": round(random.paretovariate(1.5) * 100, 2),
                "created_at": datetime.now().isoformat(),
                "cpf": self.fake.cpf(),
                "mcc": self.fake.numerify("####"),
            }
```

### Simple Kafka Producer (using confluent-kafka-python)
```python
"""Kafka producer - thin wrapper with real value."""

import json

from confluent_kafka import Producer


class DataProducer:
    """Produce data to Kafka. Adds JSON serialization and metrics."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.sent = 0
        self.failed = 0

    def send(self, topic: str, record: dict) -> None:
        """Send record to topic."""
        self.producer.produce(
            topic=topic,
            value=json.dumps(record).encode(),
            callback=self._on_delivery,
        )
        self.producer.poll(0)

    def _on_delivery(self, err, msg) -> None:
        if err:
            self.failed += 1
        else:
            self.sent += 1

    def flush(self) -> None:
        self.producer.flush()
```

---

## Security Checks

| Check | What to Look For |
|-------|-----------------|
| Hardcoded secrets | API keys, passwords, tokens in code |
| SQL injection | String concatenation in queries |
| Path traversal | User input in file paths |
| Unsafe deserialization | `pickle.load` on untrusted data |
| Command injection | User input in shell commands |

---

## Review Output Format

```markdown
## Code Review: {filename}

### Summary
{One sentence: Is this code good, needs minor fixes, or needs refactoring?}

### Issues

#### [SIMPLIFY] {Issue title}
**Line:** {line number}
**Problem:** {What's wrong}
**Fix:** {How to fix it}

#### [OVER-ENGINEERED] {Issue title}
**Line:** {line number}
**Problem:** {What's unnecessary}
**Fix:** {Simpler alternative}

#### [SECURITY] {Issue title}
**Line:** {line number}
**Problem:** {Security concern}
**Fix:** {How to fix it}

### What's Good
- {Thing that's well done}
- {Another good thing}

### Verdict
{APPROVE | NEEDS CHANGES | REFACTOR RECOMMENDED}
```

---

## Example Review

```markdown
## Code Review: generators/payments/transactions.py

### Summary
Good generator but has unnecessary abstraction and over-configuration.

### Issues

#### [OVER-ENGINEERED] Unnecessary base class
**Line:** 15
**Problem:** `BaseGenerator` ABC has only one implementation
**Fix:** Remove base class, use `TransactionGenerator` directly

#### [SIMPLIFY] Config has unused fields
**Line:** 8-12
**Problem:** `enable_logging`, `validation_mode` are never used
**Fix:** Remove unused config fields

#### [OVER-ENGINEERED] Factory method not needed
**Line:** 45
**Problem:** `create_generator()` factory for single type
**Fix:** Just instantiate `TransactionGenerator()` directly

### What's Good
- Clear method names
- Good use of Faker for Brazilian data
- Proper type hints

### Verdict
NEEDS CHANGES - Remove unnecessary abstractions before merge
```

---

## Trigger Phrases

- "Review this code"
- "Is this over-engineered?"
- "Can this be simpler?"
- "Check for unnecessary complexity"
- "Review for patterns"
- "Check this code for security issues"
