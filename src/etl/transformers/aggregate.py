"""Aggregation transformer: groupby with NumPy-backed reductions."""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseTransformer
from etl.core.exceptions import TransformerError
from etl.core.registry import register_transformer
from etl.decorators import with_metrics


class AggregateConfig(BaseModel):
    """Validated params for :class:`AggregateTransformer`."""

    group_by: list[str] = Field(min_length=1)
    aggs: dict[str, str] = Field(
        description="Mapping column -> agg func (sum, mean, min, max, count)."
    )


_ALLOWED_AGGS = {"sum", "mean", "min", "max", "count", "std", "first", "last"}


@register_transformer("aggregate")
class AggregateTransformer(BaseTransformer):
    """Group and aggregate. Buffers the whole stream (shuffle boundary).

    Streaming groupby across chunk boundaries requires a global view, so this
    transformer materializes its input — the standard shuffle exception in an
    otherwise lazy pipeline.
    """

    config_schema = AggregateConfig

    @with_metrics(times=1, backoff=0.0, exceptions=(ValueError,))
    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Consume all chunks, yield a single aggregated frame."""
        cfg = self.config
        assert isinstance(cfg, AggregateConfig)
        try:
            frames = [f for f in data if not f.empty]
            if not frames:
                yield pd.DataFrame()
                return
            merged = pd.concat(frames, ignore_index=True)
            for col, func in cfg.aggs.items():
                if func not in _ALLOWED_AGGS:
                    raise TransformerError(
                        f"Unsupported agg '{func}' for column '{col}'. "
                        f"Allowed: {sorted(_ALLOWED_AGGS)}"
                    )
            result = merged.groupby(cfg.group_by, as_index=False).agg(cfg.aggs)
            yield result
        except TransformerError:
            raise
        except Exception as exc:
            raise TransformerError(f"Aggregate transformer failed: {exc}") from exc
