"""Duck-typing contracts (``Protocol``) for ETL components.

Abstract base classes in :mod:`etl.core.base` are the primary extension
point. Protocols below allow structural checks without inheritance —
useful for tests and third-party adapters.
"""

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Readable(Protocol):
    """Anything that can produce DataFrame chunks."""

    def read(self) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks lazily."""
        ...


@runtime_checkable
class Writable(Protocol):
    """Anything that can consume DataFrame chunks."""

    def write(self, data: Iterator[pd.DataFrame]) -> int:
        """Consume chunks, return number of rows written."""
        ...


@runtime_checkable
class Transformable(Protocol):
    """Anything that maps an iterator of frames to another iterator."""

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Apply transformation lazily."""
        ...
