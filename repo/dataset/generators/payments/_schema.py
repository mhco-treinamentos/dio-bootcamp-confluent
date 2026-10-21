"""Loader for the Avro schemas in ``schemas/payments/``.

The ``.avsc`` files are the single source of truth for event contracts;
generators return them via ``get_schema()`` instead of duplicating JSON.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

# parents[3] = raiz do repo (schemas/ é irmã de dataset/, não vive dentro dela)
_SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas" / "payments"


@cache
def load_schema(name: str) -> dict[str, Any]:
    """Load an Avro schema by file stem (e.g. ``"transaction"``).

    Parameters
    ----------
    name : str
        Schema file name without the ``.avsc`` extension.

    Returns
    -------
    dict[str, Any]
        Parsed Avro schema.

    Raises
    ------
    FileNotFoundError
        If ``schemas/payments/{name}.avsc`` does not exist.
    """
    path = _SCHEMAS_DIR / f"{name}.avsc"
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)  # type: ignore[no-any-return]
