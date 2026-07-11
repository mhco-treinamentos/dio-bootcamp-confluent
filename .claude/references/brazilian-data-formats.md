# Brazilian Data Formats Reference

> **Comprehensive guide to Brazilian data formats for synthetic data generation**

## CPF (Cadastro de Pessoa Física)

### Format
- 11 digits: `XXX.XXX.XXX-XX`
- First 9 digits + 2 check digits

### Validation Algorithm
```python
def validate_cpf(cpf: str) -> bool:
    """Validate Brazilian CPF."""
    # Remove formatting
    cpf = cpf.replace(".", "").replace("-", "")

    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False

    # First check digit
    sum1 = sum(int(cpf[i]) * (10 - i) for i in range(9))
    d1 = (sum1 * 10) % 11
    d1 = 0 if d1 == 10 else d1

    # Second check digit
    sum2 = sum(int(cpf[i]) * (11 - i) for i in range(10))
    d2 = (sum2 * 10) % 11
    d2 = 0 if d2 == 10 else d2

    return cpf[-2:] == f"{d1}{d2}"


def generate_cpf(formatted: bool = True) -> str:
    """Generate valid CPF."""
    import random

    # Generate first 9 digits
    digits = [random.randint(0, 9) for _ in range(9)]

    # Calculate check digits
    sum1 = sum(d * (10 - i) for i, d in enumerate(digits))
    d1 = (sum1 * 10) % 11
    d1 = 0 if d1 == 10 else d1
    digits.append(d1)

    sum2 = sum(d * (11 - i) for i, d in enumerate(digits))
    d2 = (sum2 * 10) % 11
    d2 = 0 if d2 == 10 else d2
    digits.append(d2)

    cpf = "".join(str(d) for d in digits)

    if formatted:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    return cpf
```

## CNPJ (Cadastro Nacional de Pessoa Jurídica)

### Format
- 14 digits: `XX.XXX.XXX/XXXX-XX`
- 8 base digits + 4 branch digits + 2 check digits

### Validation Algorithm
```python
def validate_cnpj(cnpj: str) -> bool:
    """Validate Brazilian CNPJ."""
    cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "")

    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    sum1 = sum(int(cnpj[i]) * weights1[i] for i in range(12))
    d1 = 11 - (sum1 % 11)
    d1 = 0 if d1 >= 10 else d1

    sum2 = sum(int(cnpj[i]) * weights2[i] for i in range(13))
    d2 = 11 - (sum2 % 11)
    d2 = 0 if d2 >= 10 else d2

    return cnpj[-2:] == f"{d1}{d2}"


def generate_cnpj(formatted: bool = True) -> str:
    """Generate valid CNPJ."""
    import random

    # Generate base (8) + branch (4) digits
    digits = [random.randint(0, 9) for _ in range(8)]
    digits.extend([0, 0, 0, 1])  # Common branch: 0001

    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    sum1 = sum(digits[i] * weights1[i] for i in range(12))
    d1 = 11 - (sum1 % 11)
    d1 = 0 if d1 >= 10 else d1
    digits.append(d1)

    sum2 = sum(digits[i] * weights2[i] for i in range(13))
    d2 = 11 - (sum2 % 11)
    d2 = 0 if d2 >= 10 else d2
    digits.append(d2)

    cnpj = "".join(str(d) for d in digits)

    if formatted:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj
```

## Phone Numbers

### Formats
| Type | Format | Example |
|------|--------|---------|
| Mobile | `+55 (XX) 9XXXX-XXXX` | `+55 (11) 99999-1234` |
| Landline | `+55 (XX) XXXX-XXXX` | `+55 (11) 3456-7890` |

### Area Codes (DDD)
```python
AREA_CODES = {
    # São Paulo
    "11": "São Paulo",
    "12": "Vale do Paraíba",
    "13": "Baixada Santista",
    "14": "Bauru",
    "15": "Sorocaba",
    "16": "Ribeirão Preto",
    "17": "São José do Rio Preto",
    "18": "Presidente Prudente",
    "19": "Campinas",

    # Rio de Janeiro
    "21": "Rio de Janeiro",
    "22": "Campos dos Goytacazes",
    "24": "Volta Redonda",

    # Minas Gerais
    "31": "Belo Horizonte",
    "32": "Juiz de Fora",
    "33": "Governador Valadares",
    "34": "Uberlândia",
    "35": "Poços de Caldas",
    "37": "Divinópolis",
    "38": "Montes Claros",

    # Other major cities
    "41": "Curitiba",
    "47": "Joinville",
    "48": "Florianópolis",
    "51": "Porto Alegre",
    "61": "Brasília",
    "62": "Goiânia",
    "71": "Salvador",
    "81": "Recife",
    "85": "Fortaleza",
    "91": "Belém",
    "92": "Manaus",
}


def generate_phone(mobile: bool = True, ddd: str = None) -> str:
    """Generate Brazilian phone number."""
    import random

    ddd = ddd or random.choice(list(AREA_CODES.keys()))

    if mobile:
        # Mobile: 9 + 8 digits
        number = f"9{random.randint(1000, 9999)}{random.randint(1000, 9999)}"
    else:
        # Landline: 8 digits starting with 2-5
        first = random.randint(2, 5)
        number = f"{first}{random.randint(100, 999)}{random.randint(1000, 9999)}"

    return f"+55 ({ddd}) {number[:5]}-{number[5:]}"
```

## CEP (Código de Endereçamento Postal)

### Format
- 8 digits: `XXXXX-XXX`

### Regional Prefixes
```python
CEP_REGIONS = {
    "0": "Grande São Paulo",
    "1": "Interior de São Paulo",
    "2": "Rio de Janeiro e Espírito Santo",
    "3": "Minas Gerais",
    "4": "Bahia e Sergipe",
    "5": "Pernambuco, Alagoas, Paraíba, Rio Grande do Norte",
    "6": "Ceará, Piauí, Maranhão, Pará, Amazonas, Acre, Amapá, Roraima",
    "7": "Distrito Federal e Goiás",
    "8": "Paraná e Santa Catarina",
    "9": "Rio Grande do Sul",
}


def generate_cep(region: str = None) -> str:
    """Generate Brazilian CEP."""
    import random

    first = region or str(random.randint(0, 9))
    rest = "".join(str(random.randint(0, 9)) for _ in range(7))

    cep = first + rest
    return f"{cep[:5]}-{cep[5:]}"
```

## Pix Keys

### Key Types
| Type | Format | Example |
|------|--------|---------|
| CPF | 11 digits | `12345678909` |
| CNPJ | 14 digits | `12345678000195` |
| Email | email address | `usuario@email.com` |
| Phone | +55 + DDD + number | `+5511999999999` |
| EVP | UUID v4 | `123e4567-e89b-12d3-a456-426614174000` |

### E2E ID (End-to-End Identifier)
```python
def generate_e2e_id() -> str:
    """Generate Pix E2E ID.

    Format: E{ISPB}{YYYYMMDDHHMMSS}{SEQ}
    - ISPB: 8 digits (bank identifier)
    - Timestamp: 14 digits
    - SEQ: 10 alphanumeric characters
    """
    import random
    import string
    from datetime import datetime

    ispb = "".join(str(random.randint(0, 9)) for _ in range(8))
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    seq = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

    return f"E{ispb}{timestamp}{seq}"
```

## Bank Codes (COMPE)

### Major Banks
```python
BANK_CODES = {
    "001": "Banco do Brasil",
    "033": "Santander",
    "104": "Caixa Econômica Federal",
    "237": "Bradesco",
    "341": "Itaú",
    "399": "HSBC",
    "422": "Safra",
    "745": "Citibank",
    "756": "Bancoob",

    # Digital banks
    "077": "Inter",
    "212": "Original",
    "260": "Nubank",
    "290": "PagSeguro",
    "323": "Mercado Pago",
    "336": "C6 Bank",
    "380": "PicPay",
}


def generate_bank_account() -> dict:
    """Generate Brazilian bank account."""
    import random

    bank_code = random.choice(list(BANK_CODES.keys()))

    return {
        "bank_code": bank_code,
        "bank_name": BANK_CODES[bank_code],
        "branch": f"{random.randint(1, 9999):04d}",
        "account_number": f"{random.randint(1, 999999):06d}",
        "check_digit": str(random.randint(0, 9)),
    }
```

## Currency Formatting

### BRL Format
```python
import locale

# Set Brazilian locale
locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")


def format_brl(value: float) -> str:
    """Format value as Brazilian Real."""
    return locale.currency(value, grouping=True, symbol="R$")


def format_brl_simple(value: float) -> str:
    """Format BRL without locale dependency."""
    formatted = f"{value:,.2f}"
    # Brazilian format: swap . and ,
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


# Examples:
# format_brl(1234.56) -> "R$ 1.234,56"
# format_brl(1000000) -> "R$ 1.000.000,00"
```

## Date/Time Formats

### Brazilian Standards
```python
from datetime import datetime

# Date formats
DATE_FORMAT = "%d/%m/%Y"           # 20/01/2025
DATETIME_FORMAT = "%d/%m/%Y %H:%M" # 20/01/2025 14:30
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"   # 2025-01-20T14:30:00


def format_date_br(dt: datetime) -> str:
    """Format datetime in Brazilian format."""
    return dt.strftime(DATE_FORMAT)


def format_datetime_br(dt: datetime) -> str:
    """Format datetime with time in Brazilian format."""
    return dt.strftime(DATETIME_FORMAT)
```

## State Codes

```python
STATES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}
```
