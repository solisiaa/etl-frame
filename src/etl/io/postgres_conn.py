"""Sync Postgres connection context manager (psycopg v3)."""

from __future__ import annotations

import os
from types import TracebackType
from typing import Any


class PostgresConnection:
    """Manage a psycopg connection + cursor lifecycle.

    Example:
        ```python
        with PostgresConnection(dsn) as conn:
            conn.execute("SELECT 1")
        ```
    Commits on clean exit, rolls back on exception, always closes.
    """

    def __init__(self, dsn: str | None = None, **connect_kwargs: Any) -> None:
        """Store DSN (falls back to ``POSTGRES_URL`` env) and kwargs."""
        self.dsn = dsn or os.getenv("POSTGRES_URL", "")
        if not self.dsn:
            raise ValueError("Postgres DSN is empty; set POSTGRES_URL or pass dsn=...")
        self.connect_kwargs = connect_kwargs
        self._conn: Any = None

    def __enter__(self) -> Any:
        """Open connection (psycopg imported lazily so unit tests stay light)."""
        import psycopg

        self._conn = psycopg.connect(self.dsn, **self.connect_kwargs)
        self._conn.autocommit = False
        return self._conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Commit/rollback then close; never suppresses exceptions."""
        try:
            if self._conn is not None:
                if exc_type is None:
                    self._conn.commit()
                else:
                    self._conn.rollback()
        finally:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
