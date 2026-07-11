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

## Domain examples (legacy: health insurance) Providers

### CNS Provider (Cartão Nacional de Saúde)
```python
from faker.providers import BaseProvider

class CNSProvider(BaseProvider):
    """Custom provider for CNS (Cartão Nacional de Saúde) numbers."""

    def cns(self) -> str:
        """Generate valid CNS number (15 digits)."""
        # First digit determines type: 1-2 = definitivo, 7-9 = provisório
        first = self.random_element(["1", "2", "7", "8", "9"])
        body = self.generator.numerify("##############")
        return first + body[:14]

    def cns_definitivo(self) -> str:
        """Generate definitive CNS (starts with 1 or 2)."""
        first = self.random_element(["1", "2"])
        return first + self.generator.numerify("##############")[:14]
```

### TUSS Provider
```python
class TUSSProvider(BaseProvider):
    """Custom provider for TUSS (medical procedure) codes."""

    CONSULTAS = [
        ("10101012", "Consulta em consultório"),
        ("10101039", "Consulta em pronto socorro"),
        ("10102019", "Consulta em domicílio"),
    ]

    EXAMES_LAB = [
        ("40301630", "Hemograma completo"),
        ("40302040", "Glicose"),
        ("40302580", "Colesterol total"),
        ("40301150", "Creatinina"),
        ("40302423", "TSH"),
    ]

    EXAMES_IMAGEM = [
        ("40801020", "Radiografia de tórax"),
        ("40901033", "Ultrassonografia abdome total"),
        ("40808017", "Tomografia computadorizada crânio"),
        ("41001010", "Ressonância magnética crânio"),
    ]

    INTERNACAO = [
        ("30101557", "Apendicectomia"),
        ("31003079", "Parto cesáreo"),
        ("30715016", "Artroscopia de joelho"),
        ("30401054", "Colecistectomia videolaparoscópica"),
    ]

    def codigo_tuss(self, tipo: str = None) -> tuple[str, str]:
        """Return (code, description) for a TUSS procedure."""
        if tipo == "CONSULTA":
            return self.random_element(self.CONSULTAS)
        elif tipo == "EXAME_LAB":
            return self.random_element(self.EXAMES_LAB)
        elif tipo == "EXAME_IMAGEM":
            return self.random_element(self.EXAMES_IMAGEM)
        elif tipo == "INTERNACAO":
            return self.random_element(self.INTERNACAO)
        else:
            all_procs = self.CONSULTAS + self.EXAMES_LAB + self.EXAMES_IMAGEM + self.INTERNACAO
            return self.random_element(all_procs)
```

### CID-10 Provider
```python
class CID10Provider(BaseProvider):
    """Custom provider for ICD-10 diagnosis codes."""

    COMMON_CIDS = [
        ("J06.9", "Infecção aguda das vias aéreas superiores"),
        ("M54.5", "Dor lombar baixa"),
        ("J18.9", "Pneumonia não especificada"),
        ("I10", "Hipertensão essencial"),
        ("E11", "Diabetes mellitus tipo 2"),
        ("K29.7", "Gastrite não especificada"),
        ("N39.0", "Infecção do trato urinário"),
        ("J45", "Asma"),
        ("F41.1", "Ansiedade generalizada"),
        ("M79.3", "Paniculite não especificada"),
    ]

    def cid_10(self) -> tuple[str, str]:
        """Return (code, description) for a CID-10 diagnosis."""
        return self.random_element(self.COMMON_CIDS)
```

### Guia TISS Provider
```python
class GuiaTISSProvider(BaseProvider):
    """Custom provider for TISS authorization guide data."""

    GUIDE_TYPES = ["CONSULTA", "SP_SADT", "INTERNACAO", "ODONTOLOGICA", "HONORARIOS"]
    GUIDE_STATUS = ["SOLICITADA", "EM_ANALISE", "AUTORIZADA", "NEGADA", "EXECUTADA", "FATURADA"]

    PLAN_NAMES = [
        "Premium", "Basic", "Empresarial",
        "Adesão", "Nacional",
    ]

    def tipo_guia(self) -> str:
        weights = [0.40, 0.35, 0.10, 0.10, 0.05]
        return self.random_element(self.GUIDE_TYPES, weights=weights)

    def status_guia(self, weights: list[float] | None = None) -> str:
        weights = weights or [0.05, 0.05, 0.20, 0.05, 0.35, 0.30]
        return self.random_element(self.GUIDE_STATUS, weights=weights)

    def numero_guia(self) -> str:
        """Generate TISS guide number (10 digits)."""
        return self.generator.numerify("##########")

    def senha_autorizacao(self) -> str:
        """Generate authorization password."""
        return self.generator.lexify("????").upper() + self.generator.numerify("######")

    def nome_plano(self) -> str:
        return self.random_element(self.PLAN_NAMES)

# Register providers
fake.add_provider(CNSProvider)
fake.add_provider(TUSSProvider)
fake.add_provider(CID10Provider)
fake.add_provider(GuiaTISSProvider)
```

## Financial Data

### Domain examples (legacy: health insurance) Amounts
```python
fake.pricetag()               # "R$ 1.234,56"

# Custom amount generation for health procedures
import random
from decimal import Decimal

PROCEDURE_VALUE_RANGES = {
    "CONSULTA": (80, 250),
    "SP_SADT_SIMPLES": (50, 500),
    "SP_SADT_COMPLEXO": (500, 5000),
    "INTERNACAO": (2000, 100000),
    "ODONTOLOGICA": (50, 2000),
    "HONORARIOS": (200, 15000),
}

def generate_procedure_amount(tipo: str = "CONSULTA") -> Decimal:
    """Generate realistic procedure amount."""
    min_val, max_val = PROCEDURE_VALUE_RANGES.get(tipo, (50, 5000))
    amount = random.paretovariate(1.5) * min_val
    return Decimal(str(round(min(max(amount, min_val), max_val), 2)))

PREMIUM_RANGES = {
    "INDIVIDUAL": (200, 800),
    "FAMILIAR": (500, 2500),
    "EMPRESARIAL": (150, 600),
    "ADESAO": (250, 1200),
}

def generate_premium(tipo_plano: str = "INDIVIDUAL") -> Decimal:
    """Generate monthly premium amount."""
    min_val, max_val = PREMIUM_RANGES.get(tipo_plano, (200, 800))
    return Decimal(str(round(random.uniform(min_val, max_val), 2)))
```

## Date/Time Patterns

### Business Hours
```python
from datetime import datetime, timedelta

def generate_business_timestamp(fake, business_hours_weight=0.7):
    """Generate timestamp weighted toward business hours."""
    base = fake.date_time_between(start_date="-30d", end_date="now")

    if fake.boolean(chance_of_getting_true=int(business_hours_weight * 100)):
        # Business hours: 7am - 6pm (clinics/hospitals)
        hour = fake.random_int(min=7, max=17)
    else:
        hour = fake.random_int(min=0, max=23)

    return base.replace(hour=hour)
```

### Ordered Timestamps
```python
def generate_ordered_timestamps(fake, count, start_date="-30d"):
    """Generate ordered sequence of timestamps."""
    timestamps = []
    current = fake.date_time_between(start_date=start_date, end_date="now")

    for _ in range(count):
        timestamps.append(current)
        # Add random interval (1 minute to 1 hour)
        current += timedelta(minutes=fake.random_int(1, 60))

    return timestamps
```

## Statistical Distributions

### Pareto (Procedure Values)
```python
import random

def pareto_amount(alpha=1.5, scale=80, max_val=100000):
    """Generate procedure value following Pareto distribution."""
    amount = random.paretovariate(alpha) * scale
    return round(min(amount, max_val), 2)
```

### Log-Normal (Premium Values)
```python
import random

def lognormal_amount(mu=6, sigma=0.5, min_val=150, max_val=3000):
    """Generate premium amount following log-normal distribution."""
    amount = random.lognormvariate(mu, sigma)
    return round(max(min_val, min(amount, max_val)), 2)
```

### Weighted Categories
```python
def weighted_category(fake, categories, weights):
    """Select category with custom weights."""
    return fake.random_element(
        elements=categories,
        weights=weights,
    )

# Example: Guide status
status = weighted_category(
    fake,
    ["AUTORIZADA", "EXECUTADA", "NEGADA"],
    [0.85, 0.10, 0.05],
)
```

## Deterministic Generation

### Consistent Entity Data
```python
def generate_membro(seed: int) -> dict:
    """Generate deterministic health plan member data."""
    fake = Faker("pt_BR")
    fake.seed_instance(seed)

    return {
        "membro_id": f"MBR-{seed:08d}",
        "cpf": fake.cpf(),
        "cns": fake.numerify("###############"),
        "nome": fake.name(),
        "email": fake.email(),
        "telefone": fake.phone_number(),
        "data_nascimento": str(fake.date_of_birth(minimum_age=0, maximum_age=90)),
        "sexo": fake.random_element(["M", "F"]),
    }

# Same seed = same member
membro1 = generate_membro(12345)
membro2 = generate_membro(12345)
assert membro1 == membro2
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
