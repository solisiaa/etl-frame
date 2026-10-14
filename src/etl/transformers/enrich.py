"""Enrichment transformer: computed columns via safe ``eval`` expressions."""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseTransformer
from etl.core.exceptions import TransformerError
from etl.core.registry import register_transformer
from etl.decorators import with_metrics


class EnrichConfig(BaseModel):
    """Validated params for :class:`EnrichTransformer`."""

    add_columns: dict[str, str] = Field(
        default_factory=dict, description="Mapping new_col -> pandas.eval expression."
    )


@register_transformer("enrich")
class EnrichTransformer(BaseTransformer):
    """Add computed columns, e.g. ``revenue: 'amount * quantity'``.

    Expressions are evaluated with :meth:`pandas.DataFrame.eval` using the
    ``python`` engine (no arbitrary code execution beyond column math).

    Heterogeneous multi-source streams are tolerated: a chunk missing the
    referenced columns is passed through unchanged (with a warning) instead
    of failing the whole pipeline.
    """

    config_schema = EnrichConfig

    @with_metrics(times=1, backoff=0.0, exceptions=(ValueError,))
    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Yield enriched chunks lazily."""
        import logging

        log = logging.getLogger(__name__)
        cfg = self.config
        assert isinstance(cfg, EnrichConfig)
        for chunk in data:
            try:
                df = chunk.copy()
                for col, expr in cfg.add_columns.items():
                    try:
                        df[col] = df.eval(expr, engine="python")
                    except Exception as exc:  # noqa: BLE001, PERF203
                        # Missing column in heterogeneous stream -> pass through.
                        log.warning("enrich: skip '%s' for chunk: %s", col, exc)
                        if col in df.columns:
                            del df[col]
                yield df
            except Exception as exc:
                raise TransformerError(f"Enrich transformer failed: {exc}") from exc
