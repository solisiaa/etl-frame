"""Streaming CSV source (generator-based, low memory)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseSource
from etl.core.exceptions import SourceError
from etl.core.registry import register_source
from etl.decorators import with_metrics
from etl.io.stream import read_chunks


class CsvSourceConfig(BaseModel):
    """Validated params for :class:`CsvSource`."""

    path: str = Field(description="Path to CSV file.")
    chunk_size: int = Field(default=10_000, ge=1)
    dtype: Optional[dict[str, str]] = None
    parse_dates: Optional[list[str]] = None
    usecols: Optional[list[str]] = None


@register_source("csv")
class CsvSource(BaseSource):
    """Read a CSV file in chunks via :func:`etl.io.stream.read_chunks`.

    Example YAML:
        ```yaml
        - name: sales_csv
          type: csv
          params: {path: data/sample_sales.csv, chunk_size: 50000}
        ```
    """

    config_schema = CsvSourceConfig

    @with_metrics(times=3, backoff=1.0, exceptions=(IOError, OSError))
    def read(self) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks; wraps IO errors in :class:`SourceError`."""
        cfg = self.config
        assert isinstance(cfg, CsvSourceConfig)
        kwargs: dict[str, Any] = {}
        if cfg.dtype:
            kwargs["dtype"] = cfg.dtype
        if cfg.parse_dates:
            kwargs["parse_dates"] = cfg.parse_dates
        if cfg.usecols:
            kwargs["usecols"] = cfg.usecols
        try:
            yield from read_chunks(cfg.path, chunk_size=cfg.chunk_size, **kwargs)
        except (IOError, OSError) as exc:
            raise SourceError(f"CSV source '{self.name}' failed: {exc}") from exc
