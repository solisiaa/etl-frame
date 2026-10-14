"""Unit tests for sources with mocked backends (no network/DB needed)."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from etl.core.exceptions import SourceError
from etl.sources.api_source import ApiSource
from etl.sources.csv_source import CsvSource
from etl.sources.postgres_source import PostgresSource


def test_csv_source_missing_file_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing CSV raises SourceError (sleep patched to keep suite fast)."""
    import time

    monkeypatch.setattr(time, "sleep", lambda s: None)
    src = CsvSource(name="s", params={"path": "nope/missing.csv"})
    with pytest.raises(SourceError):
        list(src.read())


# --- ApiSource (mocked httpx) ---


class _FakeResp:
    def __init__(self, payload: Any, fail: bool = False) -> None:
        """Store JSON payload; optionally fail on status check."""
        self.payload = payload
        self.fail = fail

    def raise_for_status(self) -> None:
        """Raise when the fake server errors."""
        if self.fail:
            raise RuntimeError("500")

    def json(self) -> Any:
        """Return canned payload."""
        return self.payload


class _FakeAsyncClient:
    """Minimal stand-in for httpx.AsyncClient."""

    payload: Any = [{"a": 1}]
    fail = False
    seen: dict[str, Any] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Ignore client options."""

    async def __aenter__(self) -> _FakeAsyncClient:
        """Enter session."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit session."""
        return None

    async def get(self, url: str, params: Any = None) -> _FakeResp:
        """Record GET and return canned response."""
        _FakeAsyncClient.seen = {"method": "GET", "url": url, "params": params}
        return _FakeResp(_FakeAsyncClient.payload, _FakeAsyncClient.fail)

    async def post(self, url: str, params: Any = None, json: Any = None) -> _FakeResp:
        """Record POST and return canned response."""
        _FakeAsyncClient.seen = {"method": "POST", "url": url, "json": json}
        return _FakeResp(_FakeAsyncClient.payload, _FakeAsyncClient.fail)


def _patch_httpx(monkeypatch: pytest.MonkeyPatch, payload: Any, fail: bool = False) -> None:
    import httpx

    _FakeAsyncClient.payload = payload
    _FakeAsyncClient.fail = fail
    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)


def test_api_source_get_with_record_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET + record_path digs into nested JSON."""
    import asyncio

    payload = {"data": {"rates": [{"cur": "EUR", "v": 1.1}]}}
    _patch_httpx(monkeypatch, payload)
    src = ApiSource(
        name="fx",
        params={"url": "https://x.test", "method": "GET", "record_path": "data.rates"},
    )
    frames = asyncio.run(src.read_async())
    assert frames[0]["cur"].tolist() == ["EUR"]


def test_api_source_post_and_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST sends body; dict/scalar payloads normalize to one-row frames."""
    import asyncio

    _patch_httpx(monkeypatch, [{"a": 1}])
    src = ApiSource(name="p", params={"url": "https://x.test", "method": "POST", "body": {"q": 1}})
    frames = asyncio.run(src.read_async())
    assert len(frames[0]) == 1
    assert _FakeAsyncClient.seen["method"] == "POST"

    _patch_httpx(monkeypatch, {"single": True})
    dict_frames = asyncio.run(ApiSource(name="d", params={"url": "https://x.test"}).read_async())
    assert len(dict_frames[0]) == 1

    _patch_httpx(monkeypatch, 42)
    scalar_frames = asyncio.run(ApiSource(name="v", params={"url": "https://x.test"}).read_async())
    assert scalar_frames[0]["value"].tolist() == [42]


def test_api_source_failure_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP errors surface as SourceError."""
    import asyncio

    _patch_httpx(monkeypatch, {}, fail=True)
    with pytest.raises(SourceError):
        asyncio.run(ApiSource(name="f", params={"url": "https://x.test"}).read_async())


# --- PostgresSource (mocked psycopg / asyncpg) ---


class _FakePgCursor:
    def __init__(self, batches: list[list[tuple]]) -> None:
        """Queue fetchmany batches."""
        self.batches = list(batches)
        self.description = [("customer_id",), ("revenue",)]

    def __enter__(self) -> _FakePgCursor:
        """Enter cursor."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit cursor."""
        return None

    def execute(self, sql: str) -> None:
        """Record SQL."""
        self.sql = sql

    def fetchmany(self, n: int) -> list[tuple]:
        """Pop the next batch or return []."""
        if self.batches:
            return self.batches.pop(0)
        return []


class _FakePgConn:
    batches: list[list[tuple]] = [[(1, 10.0)], [(2, 20.0)]]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Ignore connect args."""

    def __enter__(self) -> _FakePgConn:
        """Enter connection."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit connection."""
        return None

    def cursor(self, name: Any = None) -> _FakePgCursor:
        """Return cursor over canned batches."""
        return _FakePgCursor(_FakePgConn.batches)


def _install_fake_psycopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType("psycopg")
    module.connect = lambda *a, **k: _FakePgConn()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", module)


def test_postgres_source_read_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    """SELECT streams canned batches as frames."""
    _install_fake_psycopg(monkeypatch)
    _FakePgConn.batches = [[(1, 10.0)], [(2, 20.0)]]
    src = PostgresSource(name="pg", params={"dsn": "db", "table": "t", "chunk_size": 1})
    chunks = list(src.read())
    assert sum(len(c) for c in chunks) == 2
    assert list(chunks[0].columns) == ["customer_id", "revenue"]


def test_postgres_source_custom_query_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Custom query works; missing table/query and empty DSN -> SourceError."""
    _install_fake_psycopg(monkeypatch)
    _FakePgConn.batches = [[(1, 1.0)]]
    src = PostgresSource(name="pg", params={"dsn": "db", "query": "SELECT 1"})
    assert sum(len(c) for c in src.read()) == 1

    with pytest.raises(SourceError):
        list(PostgresSource(name="pg", params={"dsn": "db"}).read())

    monkeypatch.delenv("POSTGRES_URL", raising=False)
    with pytest.raises(SourceError):
        list(PostgresSource(name="pg", params={"table": "t"}).read())


def test_postgres_source_async_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without asyncpg, read_async falls back to a thread running read()."""
    import asyncio

    _install_fake_psycopg(monkeypatch)
    monkeypatch.delitem(sys.modules, "asyncpg", raising=False)
    _FakePgConn.batches = [[(1, 1.0)]]
    src = PostgresSource(name="pg", params={"dsn": "db", "table": "t"})
    frames = asyncio.run(src.read_async())
    assert sum(len(f) for f in frames) == 1


def test_postgres_source_async_native(monkeypatch: pytest.MonkeyPatch) -> None:
    """With asyncpg present, read_async uses it natively."""
    import asyncio

    class _FakeAsyncConn:
        async def fetch(self, sql: str) -> list[dict]:
            """Return canned rows."""
            return [{"customer_id": 1, "revenue": 5.0}]

        async def close(self) -> None:
            """Close (no-op)."""
            return None

    module = types.ModuleType("asyncpg")

    async def _connect(dsn: str) -> _FakeAsyncConn:
        return _FakeAsyncConn()

    module.connect = _connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "asyncpg", module)
    src = PostgresSource(name="pg", params={"dsn": "db", "table": "t"})
    frames = asyncio.run(src.read_async())
    assert frames[0]["revenue"].tolist() == [5.0]


def test_postgres_source_async_empty_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty fetch -> one empty frame; connect errors -> SourceError."""
    import asyncio

    module = types.ModuleType("asyncpg")

    async def _empty(dsn: str) -> Any:
        class _C:
            async def fetch(self, sql: str) -> list:
                """No rows."""
                return []

            async def close(self) -> None:
                """Close."""
                return None

        return _C()

    async def _boom(dsn: str) -> Any:
        raise RuntimeError("down")

    module.connect = _empty  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "asyncpg", module)
    frames = asyncio.run(PostgresSource(name="pg", params={"dsn": "db", "table": "t"}).read_async())
    assert frames[0].empty

    module.connect = _boom  # type: ignore[attr-defined]
    with pytest.raises(SourceError):
        asyncio.run(PostgresSource(name="pg", params={"dsn": "db", "table": "t"}).read_async())
