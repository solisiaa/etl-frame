"""CSV file sink (streaming append, header only once)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel

from etl.core.base import BaseSink
from etl.core.context import PipelineContext
from etl.core.exceptions import SinkError
from etl.core.registry import register_sink
from etl.decorators import with_metrics


class CsvSinkConfig(BaseModel):
    """Validated params for :class:`CsvSink`."""

    path: str
    mode: Literal["overwrite", "append"] = "overwrite"


@register_sink("csv")
class CsvSink(BaseSink):
    """Write the stream to a local CSV file (outer-join for heterogeneous streams)."""

    config_schema = CsvSinkConfig

    @with_metrics(times=1, backoff=0.0, exceptions=(IOError,))
    def write(self, data: Iterator[pd.DataFrame], ctx: PipelineContext | None = None) -> int:
        """Buffer chunks, outer-join columns, write once; return total rows."""
        cfg = self.config
        assert isinstance(cfg, CsvSinkConfig)
        try:
            frames = [f for f in data if not f.empty]
            total = sum(len(f) for f in frames)
            if not frames:
                return 0
            # Outer join handles heterogeneous multi-source schemas (fills NaN).
            merged = pd.concat(frames, ignore_index=True, sort=False)
            out = Path(cfg.path)
            out.parent.mkdir(parents=True, exist_ok=True)
            if cfg.mode == "append" and out.exists():
                prev = pd.read_csv(out)
                merged = pd.concat([prev, merged], ignore_index=True, sort=False)
            merged.to_csv(out, index=False)
        except Exception as exc:
            raise SinkError(f"CSV sink '{self.name}' failed: {exc}") from exc
        return total
