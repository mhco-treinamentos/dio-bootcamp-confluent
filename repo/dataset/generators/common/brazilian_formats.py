"""Brazilian data formats: generation and validation with valid check digits.

All generation functions take a ``random.Random`` instance so callers control
determinism — never the module-level ``random`` (see .claude/rules/python.md).
Reference: .claude/references/brazilian-data-formats.md.
"""

from __future__ import annotations

import random

# DDDs válidos (subconjunto representativo — ver brazilian-data-formats.md)
AREA_CODES: tuple[str, ...] = (
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "21",
    "22",
    "24",
    "31",
    "32",
    "33",
    "34",
    "35",
    "37",
    "38",
    "41",
    "47",
    "48",
    "51",
    "61",
    "62",
    "71",
    "81",
    "85",
    "91",
    "92",
)

# UFs com pesos aproximados de população (concentra SP/MG/RJ como no Brasil real)
UF_WEIGHTS: dict[str, float] = {
    "SP": 0.215,
    "MG": 0.098,
    "RJ": 0.082,
    "BA": 0.070,
    "PR": 0.055,
    "RS": 0.052,
    "PE": 0.045,
    "CE": 0.043,
    "PA": 0.041,
    "SC": 0.036,
    "GO": 0.034,
    "MA": 0.032,
    "AM": 0.020,
    "ES": 0.019,
    "PB": 0.019,
    "MT": 0.017,
    "RN": 0.016,
    "AL": 0.015,
    "PI": 0.015,
    "DF": 0.014,
    "MS": 0.013,
    "SE": 0.011,
    "RO": 0.008,
    "TO": 0.007,
    "AC": 0.004,
    "AP": 0.004,
    "RR": 0.003,
}


def _cpf_check_digits(digits: list[int]) -> tuple[int, int]:
    """Compute the two CPF check digits for the first nine digits.

    Parameters
    ----------
    digits : list[int]
        First nine CPF digits.

    Returns
    -------
    tuple[int, int]
        First and second check digits.
    """
    sum1 = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
    d1 = (sum1 * 10) % 11
    d1 = 0 if d1 == 10 else d1

    sum2 = sum(d * (11 - i) for i, d in enumerate(digits[:9] + [d1]))
    d2 = (sum2 * 10) % 11
    d2 = 0 if d2 == 10 else d2
    return d1, d2


def validate_cpf(cpf: str) -> bool:
    """Validate a Brazilian CPF (check digits).

    Parameters
    ----------
    cpf : str
        CPF, with or without formatting.

    Returns
    -------
    bool
        True if the CPF has 11 digits and valid check digits.
    """
    cpf = cpf.replace(".", "").replace("-", "")
    if len(cpf) != 11 or not cpf.isdigit() or cpf == cpf[0] * 11:
        return False
    digits = [int(c) for c in cpf]
    d1, d2 = _cpf_check_digits(digits[:9])
    return digits[9] == d1 and digits[10] == d2


def generate_cpf(rng: random.Random) -> str:
    """Generate a valid CPF (11 digits, no formatting — schema format).

    Parameters
    ----------
    rng : random.Random
        Seeded random instance (determinism is the caller's contract).

    Returns
    -------
    str
        Valid unformatted CPF, e.g. ``"12345678909"``.
    """
    digits = [rng.randint(0, 9) for _ in range(9)]
    d1, d2 = _cpf_check_digits(digits)
    return "".join(str(d) for d in digits + [d1, d2])


def _cnpj_check_digits(digits: list[int]) -> tuple[int, int]:
    """Compute the two CNPJ check digits for the first twelve digits.

    Parameters
    ----------
    digits : list[int]
        First twelve CNPJ digits (base + branch).

    Returns
    -------
    tuple[int, int]
        First and second check digits.
    """
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    sum1 = sum(d * w for d, w in zip(digits[:12], weights1, strict=False))
    d1 = 11 - (sum1 % 11)
    d1 = 0 if d1 >= 10 else d1

    sum2 = sum(d * w for d, w in zip(digits[:12] + [d1], weights2, strict=False))
    d2 = 11 - (sum2 % 11)
    d2 = 0 if d2 >= 10 else d2
    return d1, d2


def validate_cnpj(cnpj: str) -> bool:
    """Validate a Brazilian CNPJ (check digits).

    Parameters
    ----------
    cnpj : str
        CNPJ, with or without formatting.

    Returns
    -------
    bool
        True if the CNPJ has 14 digits and valid check digits.
    """
    cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "")
    if len(cnpj) != 14 or not cnpj.isdigit() or cnpj == cnpj[0] * 14:
        return False
    digits = [int(c) for c in cnpj]
    d1, d2 = _cnpj_check_digits(digits[:12])
    return digits[12] == d1 and digits[13] == d2


def generate_cnpj(rng: random.Random) -> str:
    """Generate a valid CNPJ (14 digits, no formatting, branch 0001).

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.

    Returns
    -------
    str
        Valid unformatted CNPJ, e.g. ``"12345678000195"``.
    """
    digits = [rng.randint(0, 9) for _ in range(8)] + [0, 0, 0, 1]
    d1, d2 = _cnpj_check_digits(digits)
    return "".join(str(d) for d in digits + [d1, d2])


def generate_phone(rng: random.Random, mobile: bool = True) -> str:
    """Generate a Brazilian phone number in topic format ``+55XXXXXXXXXXX``.

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.
    mobile : bool, optional
        Mobile (9 + 8 digits) or landline (8 digits), by default True.

    Returns
    -------
    str
        Phone in ``+55`` + DDD + number format (no punctuation — schema format).
    """
    ddd = rng.choice(AREA_CODES)
    if mobile:
        number = f"9{rng.randint(10000000, 99999999)}"
    else:
        number = f"{rng.randint(2, 5)}{rng.randint(1000000, 9999999)}"
    return f"+55{ddd}{number}"


def generate_cep(rng: random.Random) -> str:
    """Generate a CEP (8 digits, no punctuation — schema format).

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.

    Returns
    -------
    str
        CEP like ``"30130100"``.
    """
    return f"{rng.randint(0, 99999999):08d}"


def pick_uf(rng: random.Random) -> str:
    """Pick a UF weighted by (approximate) population share.

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.

    Returns
    -------
    str
        Two-letter state code (e.g. ``"SP"``).
    """
    ufs = list(UF_WEIGHTS.keys())
    weights = list(UF_WEIGHTS.values())
    return rng.choices(ufs, weights=weights, k=1)[0]
