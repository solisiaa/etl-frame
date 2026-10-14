"""Unit tests for IO managers, protocols, metrics and logging (mocked)."""

from __future__ import annotations

import sys
import types
from typing import Any

import pandas as pd
import pytest


def test_postgres_connection_commit_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit on success, rollback on error, close always; empty DSN -> ValueError."""
    from etl.io.postgres_conn import PostgresConnection

    calls: dict[str, Any] = {}

    class _Conn:
        def __init__(self) -> None:
            """Track lifecycle calls."""
            calls["opened"] = True
            self.autocommit = True

        def commit(self) -> None:
            """Record commit."""
            calls["commit"] = True

        def rollback(self) -> None:
            """Record rollback."""
            calls["rollback"] = True

        def close(self) -> None:
            """Record close."""
            calls["close"] = True

    module = types.ModuleType("psycopg")
    module.connect = lambda *a, **k: _Conn()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", module)

    with PostgresConnection("db") as conn:
        assert isinstance(conn, _Conn)
    assert calls.get("commit") and calls.get("close")

    calls.clear()
    with pytest.raises(RuntimeError), PostgresConnection("db"):
        raise RuntimeError("boom")
    assert calls.get("rollback") and calls.get("close")

    monkeypatch.delenv("POSTGRES_URL", raising=False)
    with pytest.raises(ValueError):
        PostgresConnection("")


def test_s3_client_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """S3Client creates a boto3 client and closes the pool on exit."""
    from etl.io.s3_client import S3Client

    closed: dict[str, bool] = {}

    class _Boto:
        def close(self) -> None:
            """Record close."""
            closed["yes"] = True

    module = types.ModuleType("boto3")
    module.client = lambda *a, **k: _Boto()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", module)

    with S3Client(endpoint_url="http://x") as client:
        assert isinstance(client, _Boto)
    assert closed.get("yes")


def test_s3_client_async_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """S3ClientAsync enters/exits the aioboto3 session client."""
    import asyncio

    from etl.io.s3_client import S3ClientAsync

    exited: dict[str, bool] = {}

    class _CM:
        async def __aenter__(self) -> str:
            """Return fake client."""
            return "s3-async"

        async def __aexit__(self, *args: Any) -> None:
            """Record exit."""
            exited["yes"] = True

    class _Session:
        def client(self, *args: Any, **kwargs: Any) -> _CM:
            """Return fake context."""
            return _CM()

    module = types.ModuleType("aioboto3")
    module.Session = _Session  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aioboto3", module)

    async def _main() -> None:
        async with S3ClientAsync(endpoint_url="http://x") as s3:
            assert s3 == "s3-async"
        assert exited.get("yes")

    asyncio.run(_main())


def test_protocols_runtime_checks() -> None:
    """Registered components satisfy the duck-typing protocols."""
    from etl.core.protocols import Readable, Transformable, Writable
    from etl.sinks.csv_sink import CsvSink
    from etl.sources.csv_source import CsvSource
    from etl.transformers.clean import CleanTransformer

    assert isinstance(CsvSource(name="s", params={"path": "x.csv"}), Readable)
    assert isinstance(CsvSink(name="o", params={"path": "o.csv"}), Writable)
    assert isinstance(CleanTransformer(params={}), Transformable)
    assert not isinstance(object(), Readable)
    assert not isinstance(object(), Writable)
    assert not isinstance(object(), Transformable)


def test_metrics_and_logging() -> None:
    """MetricsCollector aggregates; logger emits structured JSON."""
    import json
    import logging

    from etl.utils.logging import JsonFormatter, get_logger
    from etl.utils.metrics import MetricsCollector

    m = MetricsCollector()
    m.record("rows", 3.0)
    m.increment("rows")
    m.increment("other", 2.0)
    with m.timer("op"):
        pass
    summary = m.summary()
    assert summary["rows"]["last"] == 4.0
    assert summary["other"]["sum"] == 2.0
    assert m.as_table_rows()

    log = get_logger("etl-test-io")
    assert log.handlers
    record = logging.LogRecord("etl", logging.INFO, __file__, 1, "hello", (), None)
    out = JsonFormatter().format(record)
    assert json.loads(out)["msg"] == "hello"

    try:
        raise ValueError("bad")
    except ValueError:
        import sys as _sys

        record_exc = logging.LogRecord(
            "etl", logging.ERROR, __file__, 1, "fail", (), _sys.exc_info()
        )
    assert "exc" in JsonFormatter().format(record_exc)


def test_base_source_read_combined() -> None:
    """read_combined concatenates chunks; empty stream -> empty frame."""
    from collections.abc import Iterator

    from etl.sources.csv_source import CsvSource

    src = CsvSource(name="s", params={"path": "x.csv"})
    src_empty = CsvSource(name="s", params={"path": "y.csv"})
    monkey_frames = [pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [3]})]

    def _fake_read() -> Iterator[pd.DataFrame]:
        yield from monkey_frames

    src.read = _fake_read  # type: ignore[method-assign]
    src_empty.read = lambda: iter([])  # type: ignore[method-assign]
    assert src.read_combined()["a"].tolist() == [1, 2, 3]
    assert src_empty.read_combined().empty
