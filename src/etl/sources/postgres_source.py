"""Postgres source: SELECT a table/query in chunks (sync + async)."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseSource
from etl.core.exceptions import SourceError
from etl.core.registry import register_source
from etl.decorators import with_metrics


class PostgresSourceConfig(BaseModel):
    """Validated params for :class:`PostgresSource`."""

    dsn: str = Field(default="", description="Libpq DSN; defaults to $POSTGRES_URL.")
    table: str = Field(default="", description="Table to SELECT * from (or use query).")
    query: str = Field(default="", description="Custom SELECT query (overrides table).")
    chunk_size: int = Field(default=10_000, ge=1)


@register_source("postgres")
class PostgresSource(BaseSource):
    """Stream rows from Postgres using server-side cursors."""

    config_schema = PostgresSourceConfig

    def _dsn(self) -> str:
        cfg = self.config
        assert isinstance(cfg, PostgresSourceConfig)
        dsn = cfg.dsn or os.getenv("POSTGRES_URL", "")
        if not dsn:
            raise SourceError("Postgres DSN is empty; set POSTGRES_URL or params.dsn.")
        return dsn

    def _sql(self) -> str:
        cfg = self.config
        assert isinstance(cfg, PostgresSourceConfig)
        if cfg.query:
            return cfg.query
        if cfg.table:
            return f"SELECT * FROM {cfg.table}"
        raise SourceError("Postgres source needs params.table or params.query.")

    @with_metrics(times=3, backoff=1.0, exceptions=(IOError, OSError))
    def read(self) -> Iterator[pd.DataFrame]:
        """Yield chunks via psycopg server-side cursor."""
        import psycopg

        cfg = self.config
        assert isinstance(cfg, PostgresSourceConfig)
        try:
            with psycopg.connect(self._dsn()) as conn, conn.cursor(name="etl_stream") as cur:
                cur.execute(self._sql())
                cols = [d[0] for d in (cur.description or [])]
                while True:
                    rows = cur.fetchmany(cfg.chunk_size)
                    if not rows:
                        break
                    yield pd.DataFrame(rows, columns=cols)
        except Exception as exc:
            raise SourceError(f"Postgres source '{self.name}' failed: {exc}") from exc

    async def read_async(self) -> list[pd.DataFrame]:
        """Native async read via asyncpg (falls back to thread if unavailable)."""
        cfg = self.config
        assert isinstance(cfg, PostgresSourceConfig)
        try:
            import asyncpg
        except ImportError:
            import asyncio

            return await asyncio.to_thread(lambda: list(self.read()))
        dsn = self._dsn()
        try:
            conn = await asyncpg.connect(dsn)
            try:
                rows = await conn.fetch(self._sql())
                if not rows:
                    return [pd.DataFrame()]
                return [pd.DataFrame([dict(r) for r in rows])]
            finally:
                await conn.close()
        except Exception as exc:
            raise SourceError(f"Postgres async source '{self.name}' failed: {exc}") from exc
