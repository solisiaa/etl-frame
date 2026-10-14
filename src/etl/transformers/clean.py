"""Data cleaning transformer: nulls, duplicates, type casts."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Union

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseTransformer
from etl.core.exceptions import TransformerError
from etl.core.registry import register_transformer
from etl.decorators import with_metrics


class CleanConfig(BaseModel):
    """Validated params for :class:`CleanTransformer`."""

    drop_nulls: list[str] = Field(default_factory=list)
    dedup_on: list[str] = Field(default_factory=list)
    fillna: dict[str, Union[str, float, int]] = Field(default_factory=dict)
    astype: dict[str, str] = Field(default_factory=dict)


@register_transformer("clean")
class CleanTransformer(BaseTransformer):
    """Drop null rows, drop duplicates, fill NA and cast dtypes — per chunk.

    Note: ``dedup_on`` is applied per chunk (streaming). For global
    deduplication, aggregate downstream or use the Postgres upsert sink.
    """

    config_schema = CleanConfig

    @with_metrics(times=1, backoff=0.0, exceptions=(ValueError,))
    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Yield cleaned chunks lazily."""
        cfg = self.config
        assert isinstance(cfg, CleanConfig)
        for chunk in data:
            try:
                df = chunk.copy()
                if cfg.drop_nulls:
                    existing = [c for c in cfg.drop_nulls if c in df.columns]
                    df = df.dropna(subset=existing)
                for col, val in cfg.fillna.items():
                    if col in df.columns:
                        df[col] = df[col].fillna(val)
                if cfg.astype:
                    casts = {k: v for k, v in cfg.astype.items() if k in df.columns}
                    if casts:
                        df = df.astype(casts)
                if cfg.dedup_on:
                    existing_keys = [c for c in cfg.dedup_on if c in df.columns]
                    if existing_keys:
                        df = df.drop_duplicates(subset=existing_keys)
                yield df
            except Exception as exc:
                raise TransformerError(f"Clean transformer failed: {exc}") from exc
