# Faker Patterns Reference

> **Comprehensive guide to using Faker for synthetic payments data generation**

## Basic Setup

```python
from faker import Faker

# Brazilian locale
fake = Faker("pt_BR")

# Multiple locales
fake = Faker(["pt_BR", "en_US"])

# Deterministic output
Faker.seed(42)
fake.seed_instance(42)
```

## Brazilian Providers

### Personal Data
```python
fake.cpf()                    # "123.456.789-09"
fake.cpf(formatted=False)     # "12345678909"
fake.name()                   # "João Silva"
fake.first_name()             # "Maria"
fake.last_name()              # "Santos"
```

### Company Data
```python
fake.cnpj()                   # "12.345.678/0001-95"
fake.cnpj(formatted=False)    # "12345678000195"
fake.company()                # "Silva e Filhos Ltda"
```

### Address
```python
fake.address()                # Full address
fake.street_address()         # "Rua das Flores, 123"
fake.city()                   # "Belo Horizonte"
fake.state()                  # "MG"
fake.postcode()               # "30130-100"
```

### Phone
```python
fake.phone_number()           # "+55 (31) 99999-9999"
fake.cellphone_number()       # Mobile format
fake.landline_number()        # Landline format
```

## Payments Domain Providers

### Card Provider (PAN sempre mascarado)
```python
from faker.providers import BaseProvider

class CardProvider(BaseProvider):
    """Custom provider for card data — PAN is NEVER generated in cleartext."""

    BRANDS_BINS = {
        "VISA": ["40171234", "45312345"],
        "MASTERCARD": ["51231234", "55412345"],
        "ELO": ["50663912", "50900112"],
        "HIPERCARD": ["60627812", "38412345"],
    }

    def card_brand(self) -> str:
        # Market share aproximado no Brasil
        return self.random_element(
            ["VISA", "MASTERCARD", "ELO", "HIPERCARD"],
            weights=[0.40, 0.35, 0.20, 0.05],
        )

    def pan_masked(self, brand: str | None = None) -> str:
        """Generate masked PAN: BIN(8) + ****** + last4. Never cleartext."""
        brand = brand or self.card_brand()
        bin_ = self.random_element(self.BRANDS_BINS[brand])
        last4 = self.generator.numerify("####")
        return f"{bin_}******{last4}"

    def card_type(self) -> str:
        return self.random_element(["CREDIT", "DEBIT", "MULTIPLE"], weights=[0.45, 0.35, 0.20])
```

### MCC Provider (ISO 18245)
```python
class MCCProvider(BaseProvider):
    """Custom provider for Merchant Category Codes (ISO 18245)."""

    # Top MCCs concentram ~70% das transações (ver rules.md)
    TOP_MCCS = [
        ("5411", "Supermercados"),
        ("5541", "Postos de combustível"),
        ("5812", "Restaurantes"),
        ("5814", "Fast food"),
        ("5912", "Farmácias"),
        ("5651", "Vestuário"),
        ("4121", "Táxi/rideshare"),
        ("5732", "Eletrônicos"),
        ("5942", "Livrarias"),
        ("7995", "Apostas"),  # útil p/ cenários de risco
    ]

    LONG_TAIL_MCCS = [
        ("742", "Veterinários"),
        ("780", "Paisagismo"),
        ("1731", "Eletricistas"),
        ("4111", "Transporte urbano"),
        ("4899", "TV a cabo/streaming"),
        ("5045", "Computadores"),
        ("5192", "Jornais/revistas"),
        ("5661", "Calçados"),
        ("5945", "Brinquedos"),
        ("8011", "Médicos"),
    ]

    def mcc(self) -> tuple[str, str]:
        """Return (code, description) — 70% top MCCs, 30% long tail."""
        if self.generator.random.random() < 0.70:
            return self.random_element(self.TOP_MCCS)
        return self.random_element(self.LONG_TAIL_MCCS)
```

### PIX Provider
```python
class PixProvider(BaseProvider):
    """Custom provider for PIX keys and end-to-end IDs."""

    def pix_key_type(self) -> str:
        return self.random_element(
            ["CPF", "EMAIL", "PHONE", "EVP"],
            weights=[0.35, 0.25, 0.25, 0.15],
        )

    def pix_key(self, key_type: str | None = None) -> str:
        key_type = key_type or self.pix_key_type()
        if key_type == "CPF":
            return self.generator.cpf(formatted=False)
        if key_type == "EMAIL":
            return self.generator.email()
        if key_type == "PHONE":
            return f"+55{self.generator.msisdn()[2:13]}"
        return str(self.generator.uuid4())  # EVP — chave aleatória

    def end_to_end_id(self) -> str:
        """E2E ID: 'E' + ISPB(8) + yyyyMMddHHmm + sequencial(11)."""
        ispb = self.generator.numerify("########")
        ts = self.generator.date_time_this_year().strftime("%Y%m%d%H%M")
        seq = self.generator.lexify("???????????").upper()
        return f"E{ispb}{ts}{seq}"
```

### Transaction Provider
```python
class TransactionProvider(BaseProvider):
    """Custom provider for transaction attributes."""

    CHANNELS = ["PIX", "CREDIT", "DEBIT", "BOLETO"]
    STATUS = ["CREATED", "AUTHORIZED", "SETTLED", "DECLINED", "REFUNDED"]
    DECLINE_REASONS = ["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD", "EXPIRED_CARD", "LIMIT_EXCEEDED"]

    def channel(self) -> str:
        # PIX domina volume no Brasil
        return self.random_element(self.CHANNELS, weights=[0.45, 0.25, 0.20, 0.10])

    def transaction_status(self, weights: list[float] | None = None) -> str:
        # Aprovação 92–97% (ver rules.md)
        weights = weights or [0.02, 0.05, 0.88, 0.04, 0.01]
        return self.random_element(self.STATUS, weights=weights)

    def decline_reason(self) -> str:
        return self.random_element(self.DECLINE_REASONS, weights=[0.55, 0.25, 0.12, 0.08])

# Register providers
fake.add_provider(CardProvider)
fake.add_provider(MCCProvider)
fake.add_provider(PixProvider)
fake.add_provider(TransactionProvider)
```

## Financial Data

### Transaction Amounts
```python
fake.pricetag()               # "R$ 1.234,56"

# Custom amount generation per channel
import random
from decimal import Decimal

CHANNEL_VALUE_RANGES = {
    "PIX": (1, 5000),
    "CREDIT": (10, 10000),
    "DEBIT": (5, 2000),
    "BOLETO": (50, 10000),
}

def generate_transaction_amount(channel: str = "PIX") -> Decimal:
    """Generate realistic transaction amount — log-normal, median ~R$80."""
    min_val, max_val = CHANNEL_VALUE_RANGES.get(channel, (1, 10000))
    amount = random.lognormvariate(4.4, 1.1)  # median ≈ e^4.4 ≈ R$81
    return Decimal(str(round(min(max(amount, min_val), max_val), 2)))

def generate_credit_limit(tier: str = "BASIC") -> Decimal:
    """Generate card credit limit per account tier."""
    ranges = {"BASIC": (500, 3000), "PLUS": (3000, 15000), "PREMIUM": (15000, 80000)}
    min_val, max_val = ranges.get(tier, (500, 3000))
    return Decimal(str(round(random.uniform(min_val, max_val), -2)))  # múltiplos de 100
```

## Date/Time Patterns

### Bimodal Daily Pattern (payments)
```python
from datetime import datetime, timedelta

def generate_transaction_timestamp(fake):
    """Generate timestamp with the bimodal daily pattern (11-13h, 18-21h BRT)."""
    base = fake.date_time_between(start_date="-30d", end_date="now")
    roll = fake.random.random()
    if roll < 0.35:
        hour = fake.random_int(min=11, max=13)   # pico almoço
    elif roll < 0.70:
        hour = fake.random_int(min=18, max=21)   # pico noite
    else:
        hour = fake.random_int(min=6, max=23)    # resto do dia
    return base.replace(hour=hour)
```

### Ordered Timestamps
```python
def generate_ordered_timestamps(fake, count, start_date="-30d"):
    """Generate ordered sequence of timestamps (created < authorized < settled)."""
    timestamps = []
    current = fake.date_time_between(start_date=start_date, end_date="now")

    for _ in range(count):
        timestamps.append(current)
        # Add random interval (1 second to 5 minutes for payments)
        current += timedelta(seconds=fake.random_int(1, 300))

    return timestamps
```

## Statistical Distributions

### Log-Normal (Transaction Values)
```python
import random

def lognormal_amount(mu=4.4, sigma=1.1, min_val=1, max_val=10000):
    """Generate transaction amount following log-normal (median ~R$80, long tail)."""
    amount = random.lognormvariate(mu, sigma)
    return round(max(min_val, min(amount, max_val)), 2)
```

### Pareto (Transactions per Account)
```python
import random

def transactions_for_account(alpha=1.2, base=5, max_val=500):
    """20% of accounts generate 80% of transactions."""
    count = int(random.paretovariate(alpha) * base)
    return min(count, max_val)
```

### Weighted Categories
```python
def weighted_category(fake, categories, weights):
    """Select category with custom weights."""
    return fake.random_element(
        elements=categories,
        weights=weights,
    )

# Example: transaction status (approval 92-97%)
status = weighted_category(
    fake,
    ["SETTLED", "AUTHORIZED", "DECLINED"],
    [0.88, 0.07, 0.05],
)
```

## Deterministic Generation

### Consistent Entity Data
```python
def generate_customer(seed: int) -> dict:
    """Generate deterministic customer data — same seed, same customer."""
    fake = Faker("pt_BR")
    fake.seed_instance(seed)

    return {
        "customer_id": f"CUS-{seed:08d}",
        "cpf": fake.cpf(formatted=False),
        "nome": fake.name(),
        "email": fake.email(),
        "telefone": fake.phone_number(),
        "data_nascimento": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
        "uf": fake.estado_sigla(),
    }

# Same seed = same customer (PII consistente entre runs — ver rules.md)
customer1 = generate_customer(12345)
customer2 = generate_customer(12345)
assert customer1 == customer2
```

### Batch Generation
```python
def generate_batch(generator_func, count: int, seed: int = 42):
    """Generate deterministic batch of records."""
    records = []
    for i in range(count):
        record = generator_func(seed + i)
        records.append(record)
    return records
```

---

**Última atualização**: 2026-07-11
