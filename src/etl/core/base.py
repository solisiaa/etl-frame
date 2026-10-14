"""Abstract base classes for ETL components.

Every component declares ``config_schema`` — a Pydantic model used to
validate the ``params`` mapping from YAML. The orchestrator instantiates
components via ``cls(config=params_dict)``.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, ClassVar

import pandas as pd
from pydantic import BaseModel

if TYPE_CHECKING:
    from etl.core.context import PipelineContext


class _EmptyConfig(BaseModel):
    """Fallback schema accepting any params (used when a component has no schema)."""

    model_config = {"extra": "allow"}


class BaseSource(ABC):
    """Extract data as a lazy iterator of DataFrames."""

    config_schema: ClassVar[type[BaseModel]] = _EmptyConfig

    def __init__(self, name: str = "source", params: dict[str, Any] | None = None) -> None:
        """Store name and raw params; validate via :attr:`config_schema`.

        Args:
            name: Logical name from YAML.
            params: Raw ``params`` mapping from YAML.
        """
        self.name = name
        self.params: dict[str, Any] = params or {}
        # Validate early — raises pydantic.ValidationError on bad input.
        self.config = self.config_schema.model_validate(self.params)

    @abstractmethod
    def read(self) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks (lazy, streaming)."""
        ...

    async def read_async(self) -> list[pd.DataFrame]:
        """Async variant; defaults to running sync :meth:`read` in a thread.

        Override for natively-async sources (e.g. HTTP) to get real concurrency.
        """
        chunks = await asyncio.to_thread(lambda: list(self.read()))
        return chunks

    def read_combined(self) -> pd.DataFrame:
        """Convenience: concatenate all chunks into one frame."""
        import pandas as pd

        frames = list(self.read())
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class BaseSink(ABC):
    """Load data from a lazy iterator of DataFrames."""

    config_schema: ClassVar[type[BaseModel]] = _EmptyConfig

    def __init__(self, name: str = "sink", params: dict[str, Any] | None = None) -> None:
        """Store name and params; validate via :attr:`config_schema`."""
        self.name = name
        self.params: dict[str, Any] = params or {}
        self.config = self.config_schema.model_validate(self.params)

    @abstractmethod
    def write(self, data: Iterator[pd.DataFrame], ctx: PipelineContext | None = None) -> int:
        """Consume chunks, return number of rows written."""
        ...


class BaseTransformer(ABC):
    """Transform a lazy stream of DataFrames into another lazy stream."""

    config_schema: ClassVar[type[BaseModel]] = _EmptyConfig

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Store params; validate via :attr:`config_schema`."""
        self.params: dict[str, Any] = params or {}
        self.config = self.config_schema.model_validate(self.params)

    @abstractmethod
    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Yield transformed chunks lazily."""
        ...
