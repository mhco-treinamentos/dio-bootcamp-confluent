"""Deterministic time helpers for synthetic data.

All timestamps derive from a fixed ``REFERENCE_TIME`` anchor instead of
``datetime.now()`` — same seed, same records, on any day the student runs the
generator (labs rule: every student sees the same data).
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

#: Fixed "now" for all generators. Bump deliberately when refreshing datasets.
REFERENCE_TIME = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)


def bimodal_hour(rng: random.Random) -> int:
    """Pick an hour following the payments daily pattern (11–13h and 18–21h BRT).

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.

    Returns
    -------
    int
        Hour of day (0–23), biased to lunch and evening peaks.
    """
    roll = rng.random()
    if roll < 0.35:
        return rng.randint(11, 13)
    if roll < 0.70:
        return rng.randint(18, 21)
    return rng.randint(6, 23)


def past_datetime(
    rng: random.Random,
    max_days: int,
    min_days: int = 0,
    anchor: datetime = REFERENCE_TIME,
) -> datetime:
    """Return a random datetime between ``anchor - max_days`` and ``anchor - min_days``.

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.
    max_days : int
        Farthest offset in the past, in days.
    min_days : int, optional
        Nearest offset in the past, in days, by default 0.
    anchor : datetime, optional
        Reference "now", by default :data:`REFERENCE_TIME`.

    Returns
    -------
    datetime
        Timezone-aware UTC datetime in the requested window.
    """
    span = (max_days - min_days) * 86_400
    offset = min_days * 86_400 + rng.randint(0, max(span, 1))
    return anchor - timedelta(seconds=offset)


def transaction_datetime(
    rng: random.Random,
    max_days: int = 30,
    anchor: datetime = REFERENCE_TIME,
) -> datetime:
    """Random event time in the last ``max_days``, with the bimodal hour pattern.

    Parameters
    ----------
    rng : random.Random
        Seeded random instance.
    max_days : int, optional
        Lookback window in days, by default 30.
    anchor : datetime, optional
        Reference "now", by default :data:`REFERENCE_TIME`.

    Returns
    -------
    datetime
        Timezone-aware UTC datetime with hour drawn from :func:`bimodal_hour`.
    """
    base = past_datetime(rng, max_days=max_days, min_days=0, anchor=anchor)
    result = base.replace(
        hour=bimodal_hour(rng), minute=rng.randint(0, 59), second=rng.randint(0, 59)
    )
    # Trocar a hora pode empurrar o timestamp para depois da âncora — recua 1 dia
    if result > anchor:
        result -= timedelta(days=1)
    return result
