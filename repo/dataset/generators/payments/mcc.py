"""MCC (Merchant Category Code, ISO 18245) tables and weighted picker.

Top-10 MCCs concentrate ~70% of transactions (.claude/rules.md — Domain Rules).
Codes are 4-character strings to preserve leading zeros.
"""

from __future__ import annotations

import random

# (mcc, descrição) — top 10 concentram ~70% do volume
TOP_MCCS: tuple[tuple[str, str], ...] = (
    ("5411", "Supermercados"),
    ("5541", "Postos de combustível"),
    ("5812", "Restaurantes"),
    ("5814", "Fast food"),
    ("5912", "Farmácias"),
    ("5651", "Vestuário"),
    ("4121", "Táxi/rideshare"),
    ("5732", "Eletrônicos"),
    ("5942", "Livrarias"),
    ("5311", "Lojas de departamento"),
)

LONG_TAIL_MCCS: tuple[tuple[str, str], ...] = (
    ("0742", "Veterinários"),
    ("0780", "Paisagismo"),
    ("1731", "Eletricistas"),
    ("4111", "Transporte urbano"),
    ("4899", "TV a cabo/streaming"),
    ("5045", "Computadores"),
    ("5192", "Jornais/revistas"),
    ("5661", "Calçados"),
    ("5945", "Brinquedos"),
    ("7995", "Apostas"),  # útil para cenários de risco/fraude
    ("8011", "Médicos"),
)

ALL_MCCS: tuple[tuple[str, str], ...] = TOP_MCCS + LONG_TAIL_MCCS


def pick_mcc(rng: random.Random) -> tuple[str, str]:
    """Pick an MCC: 70% chance top-10, 30% long tail.

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.

    Returns
    -------
    tuple[str, str]
        ``(code, description)`` — code is a 4-char string with leading zeros.
    """
    pool = TOP_MCCS if rng.random() < 0.70 else LONG_TAIL_MCCS
    return rng.choice(pool)
