"""Batched Postgres sink with insert / upsert modes."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseSink
from etl.core.context import PipelineContext
from etl.core.exceptions import SinkError
from etl.core.registry import register_sink
from etl.decorators import with_metrics
from etl.io.postgres_conn import PostgresConnection


class PostgresSinkConfig(BaseModel):
    """Validated params for :class:`PostgresSink`."""

    dsn: str = Field(default="", description="Libpq DSN; defaults to $POSTGRES_URL.")
    table: str
    mode: Literal["append", "replace", "upsert"] = "append"
    conflict_keys: list[str] = Field(default_factory=list)
    batch_size: int = Field(default=5000, ge=1)


@register_sink("postgres")
class PostgresSink(BaseSink):
    """Write DataFrame chunks to Postgres in batches.

    - ``append``: plain INSERT.
    - ``replace``: DROP + CREATE from first chunk schema.
    - ``upsert``: INSERT ... ON CONFLICT (keys) DO UPDATE.
    """

    config_schema = PostgresSinkConfig

    def _dsn(self) -> str:
        cfg = self.config
        assert isinstance(cfg, PostgresSinkConfig)
        dsn = cfg.dsn or os.getenv("POSTGRES_URL", "")
        if not dsn:
            raise SinkError("Postgres DSN is empty; set POSTGRES_URL or params.dsn.")
        return dsn

    @with_metrics(times=3, backoff=1.0, exceptions=(IOError, OSError))
    def write(self, data: Iterator[pd.DataFrame], ctx: PipelineContext | None = None) -> int:
        """Consume the stream and write batches; return total rows."""
        cfg = self.config
        assert isinstance(cfg, PostgresSinkConfig)
        total = 0
        try:
            with PostgresConnection(self._dsn()) as conn, conn.cursor() as cur:
                if cfg.mode == "replace":
                    cur.execute(f'DROP TABLE IF EXISTS "{cfg.table}"')
                first = True
                for chunk in data:
                    if chunk.empty:
                        continue
                    if cfg.mode == "replace" and first:
                        self._create_from_frame(cur, cfg.table, chunk)
                    elif first and cfg.mode in ("append", "upsert"):
                        # upsert needs the table AND a unique arbiter for
                        # ON CONFLICT; both are IF NOT EXISTS → idempotent.
                        self._ensure_table(cur, cfg.table, chunk)
                        if cfg.mode == "upsert" and cfg.conflict_keys:
                            self._ensure_conflict_index(cur, cfg)
                    self._write_chunk(cur, cfg, chunk)
                    total += len(chunk)
                    first = False
        except Exception as exc:
            raise SinkError(f"Postgres sink '{self.name}' failed: {exc}") from exc
        return total

    @staticmethod
    def _sql_type(dtype: str) -> str:
        mapping = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }
        return mapping.get(dtype, "TEXT")

    def _ensure_table(self, cur: Any, table: str, frame: pd.DataFrame) -> None:
        cols = ", ".join(f'"{c}" {self._sql_type(str(t))}' for c, t in frame.dtypes.items())
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})')

    def _create_from_frame(self, cur: Any, table: str, frame: pd.DataFrame) -> None:
        cols = ", ".join(f'"{c}" {self._sql_type(str(t))}' for c, t in frame.dtypes.items())
        cur.execute(f'CREATE TABLE "{table}" ({cols})')

    @staticmethod
    def _ensure_conflict_index(cur: Any, cfg: PostgresSinkConfig) -> None:
        """Create a unique index over conflict keys for ON CONFLICT (idempotent)."""
        cols = ", ".join(f'"{k}"' for k in cfg.conflict_keys)
        cur.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{cfg.table}_upsert_uidx" ON "{cfg.table}" ({cols})'
        )

    def _write_chunk(self, cur: Any, cfg: PostgresSinkConfig, chunk: pd.DataFrame) -> None:
        cols = list(chunk.columns)
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        rows = [tuple(r) for r in chunk.itertuples(index=False, name=None)]
        if cfg.mode == "upsert" and cfg.conflict_keys:
            conflict = ", ".join(f'"{k}"' for k in cfg.conflict_keys)
            updates = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in cfg.conflict_keys
            )
            if not updates:
                updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols)
            sql = (
                f'INSERT INTO "{cfg.table}" ({col_list}) VALUES ({placeholders}) '
                f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
            )
        else:
            sql = f'INSERT INTO "{cfg.table}" ({col_list}) VALUES ({placeholders})'
        cur.executemany(sql, rows)
