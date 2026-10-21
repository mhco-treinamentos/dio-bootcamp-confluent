"""Base interface for all synthetic data generators.

Every generator in the bootcamp is deterministic: same seed, same records.
This is a teaching requirement (labs rule: every student sees the same data
in section N) and a testing requirement (fixtures with ``seed=42``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class BaseGenerator(ABC):
    """Base class for all synthetic data generators.

    Subclasses must be deterministic given the same seed: two instances
    built with identical configuration must yield identical records.
    """

    @abstractmethod
    def generate(self, count: int) -> Iterator[dict[str, Any]]:
        """Yield ``count`` synthetic records.

        Parameters
        ----------
        count : int
            Number of records to generate.

        Yields
        ------
        dict[str, Any]
            One synthetic record per iteration, matching :meth:`get_schema`.
        """

    @abstractmethod
    def get_schema(self) -> dict[str, Any]:
        """Return the Avro schema (as a dict) of the generated records.

        Returns
        -------
        dict[str, Any]
            Parsed Avro schema for the records yielded by :meth:`generate`.
        """
