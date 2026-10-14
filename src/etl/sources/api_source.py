"""Async HTTP API source (httpx, natively async)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseSource
from etl.core.exceptions import SourceError
from etl.core.registry import register_source
from etl.decorators import with_metrics


class ApiSourceConfig(BaseModel):
    """Validated params for :class:`ApiSource`."""

    url: str
    method: Literal["GET", "POST"] = "GET"
    timeout: float = Field(default=10.0, ge=0.1)
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    body: Optional[dict[str, Any]] = None
    record_path: Optional[str] = Field(
        default=None, description="Dot path into JSON payload, e.g. 'data.rates'."
    )


def _dig(payload: Any, path: Optional[str]) -> Any:
    if not path:
        return payload
    cur: Any = payload
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


@register_source("api")
class ApiSource(BaseSource):
    """Fetch JSON over HTTP and expose it as a single-chunk frame stream."""

    config_schema = ApiSourceConfig

    def read(self) -> Iterator[pd.DataFrame]:
        """Sync fallback: run the async fetch in a fresh event loop."""
        import asyncio

        frames = asyncio.run(self.read_async())
        yield from frames

    @with_metrics(times=3, backoff=1.0, exceptions=(IOError, OSError))
    async def read_async(self) -> list[pd.DataFrame]:
        """Fetch JSON with httpx and normalize to a DataFrame."""
        import httpx

        cfg = self.config
        assert isinstance(cfg, ApiSourceConfig)
        try:
            async with httpx.AsyncClient(timeout=cfg.timeout, headers=cfg.headers) as client:
                if cfg.method == "GET":
                    resp = await client.get(cfg.url, params=cfg.params)
                else:
                    resp = await client.post(cfg.url, params=cfg.params, json=cfg.body)
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:
            raise SourceError(f"API source '{self.name}' failed: {exc}") from exc
        records = _dig(payload, cfg.record_path)
        if records is None:
            records = payload
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            records = [{"value": records}]
        return [pd.DataFrame(records)]
