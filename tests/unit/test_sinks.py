"""Unit tests for sinks with mocked backends (no Postgres/MinIO needed)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from etl.core.exceptions import SinkError
from etl.sinks.csv_sink import CsvSink
from etl.sinks.postgres_sink import PostgresSink
from etl.sinks.s3_sink import S3Sink


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"customer_id": [1, 2], "revenue": [10.0, 20.0]})


# --- CsvSink ---


def test_csv_sink_overwrite_and_append(tmp_path: Path) -> None:
    """overwrite writes once; append outer-joins with previous content."""
    out = tmp_path / "out.csv"
    sink = CsvSink(name="o", params={"path": str(out), "mode": "overwrite"})
    assert sink.write(iter([_frame()])) == 2
    assert len(pd.read_csv(out)) == 2

    appender = CsvSink(name="o", params={"path": str(out), "mode": "append"})
    extra = pd.DataFrame({"customer_id": [3], "other": ["x"]})
    assert appender.write(iter([extra])) == 1
    back = pd.read_csv(out)
    assert len(back) == 3
    assert "other" in back.columns


def test_csv_sink_empty_stream(tmp_path: Path) -> None:
    """Empty stream writes nothing and returns 0."""
    out = tmp_path / "out.csv"
    sink = CsvSink(name="o", params={"path": str(out)})
    assert sink.write(iter([pd.DataFrame()])) == 0
    assert not out.exists()


# --- S3Sink (mocked S3Client) ---


class _FakeS3:
    """In-memory stand-in for a boto3 S3 client."""

    instances: list[_FakeS3] = []

    def __init__(self, fail: bool = False) -> None:
        """Record instance; optionally fail on upload."""
        self.fail = fail
        self.uploads: list[tuple[str, str, bytes]] = []
        _FakeS3.instances.append(self)

    def head_bucket(self, Bucket: str) -> None:  # noqa: N803
        """Pretend the bucket is missing so the sink creates it."""
        raise RuntimeError("no bucket")

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803
        """Record bucket creation (no-op)."""
        self.created = Bucket

    def upload_fileobj(self, buf: Any, bucket: str, key: str, ExtraArgs: Any = None) -> None:
        """Capture uploaded bytes; optionally raise."""
        if self.fail:
            raise RuntimeError("upload down")
        self.uploads.append((bucket, key, buf.read()))


class _FakeS3Client:
    """Context manager yielding _FakeS3 (patches etl.sinks.s3_sink.S3Client)."""

    fail = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Ignore credentials."""
        self.backend = _FakeS3(fail=_FakeS3Client.fail)

    def __enter__(self) -> _FakeS3:
        """Enter fake session."""
        return self.backend

    def __exit__(self, *args: Any) -> None:
        """Exit fake session (no-op)."""
        return None


def test_s3_sink_csv_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    """CSV format uploads one object with header + rows."""
    monkeypatch.setattr("etl.sinks.s3_sink.S3Client", _FakeS3Client)
    _FakeS3Client.fail = False
    _FakeS3.instances.clear()
    sink = S3Sink(name="m", params={"bucket": "b", "key": "k/out.csv", "format": "csv"})
    assert sink.write(iter([_frame()])) == 2
    bucket, key, payload = _FakeS3.instances[-1].uploads[0]
    assert (bucket, key) == ("b", "k/out.csv")
    assert payload.decode().splitlines()[0] == "customer_id,revenue"


def test_s3_sink_empty_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty stream returns 0 without touching S3."""
    monkeypatch.setattr("etl.sinks.s3_sink.S3Client", _FakeS3Client)
    _FakeS3.instances.clear()
    sink = S3Sink(name="m", params={"bucket": "b", "key": "k", "format": "csv"})
    assert sink.write(iter([pd.DataFrame()])) == 0
    assert _FakeS3.instances == []


def test_s3_sink_failure_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backend errors surface as SinkError."""
    monkeypatch.setattr("etl.sinks.s3_sink.S3Client", _FakeS3Client)
    _FakeS3Client.fail = True
    try:
        sink = S3Sink(name="m", params={"bucket": "b", "key": "k", "format": "csv"})
        with pytest.raises(SinkError):
            sink.write(iter([_frame()]))
    finally:
        _FakeS3Client.fail = False


# --- PostgresSink (mocked PostgresConnection) ---


class _FakeCursor:
    """Records execute/executemany calls."""

    def __init__(self, fail: bool = False) -> None:
        """Store failure flag."""
        self.fail = fail
        self.statements: list[str] = []
        self.batches: list[tuple[str, list[Any]]] = []

    def __enter__(self) -> _FakeCursor:
        """Enter cursor context."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit cursor context."""
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        """Record DDL; optionally raise."""
        if self.fail:
            raise RuntimeError("db down")
        self.statements.append(sql)

    def executemany(self, sql: str, rows: Any) -> None:
        """Record DML batches."""
        self.batches.append((sql, list(rows)))


class _FakeConn:
    """Connection yielding a single shared fake cursor."""

    cursor_obj: _FakeCursor | None = None

    def cursor(self) -> _FakeCursor:
        """Return the shared cursor."""
        assert _FakeConn.cursor_obj is not None
        return _FakeConn.cursor_obj


class _FakePGConnection:
    """Patches etl.sinks.postgres_sink.PostgresConnection."""

    fail = False

    def __init__(self, dsn: str) -> None:
        """Record DSN and prepare a cursor."""
        self.dsn = dsn
        _FakeConn.cursor_obj = _FakeCursor(fail=_FakePGConnection.fail)

    def __enter__(self) -> _FakeConn:
        """Enter connection."""
        return _FakeConn()

    def __exit__(self, *args: Any) -> None:
        """Exit connection."""
        return None


def test_postgres_sink_append(monkeypatch: pytest.MonkeyPatch) -> None:
    """append creates the table once and inserts batches."""
    monkeypatch.setattr("etl.sinks.postgres_sink.PostgresConnection", _FakePGConnection)
    sink = PostgresSink(name="pg", params={"dsn": "db", "table": "t", "mode": "append"})
    assert sink.write(iter([_frame(), _frame()])) == 4
    cur = _FakeConn.cursor_obj
    assert cur is not None
    assert any("CREATE TABLE IF NOT EXISTS" in s for s in cur.statements)
    assert len(cur.batches) == 2


def test_postgres_sink_replace_and_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    """replace drops+creates; upsert emits ON CONFLICT."""
    monkeypatch.setattr("etl.sinks.postgres_sink.PostgresConnection", _FakePGConnection)
    replace = PostgresSink(name="pg", params={"dsn": "db", "table": "t", "mode": "replace"})
    assert replace.write(iter([_frame()])) == 2
    assert any("DROP TABLE" in s for s in _FakeConn.cursor_obj.statements)

    upsert = PostgresSink(
        name="pg",
        params={"dsn": "db", "table": "t", "mode": "upsert", "conflict_keys": ["customer_id"]},
    )
    assert upsert.write(iter([_frame()])) == 2
    assert "ON CONFLICT" in _FakeConn.cursor_obj.batches[0][0]


def test_postgres_sink_empty_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty stream returns 0; missing DSN and backend errors -> SinkError."""
    monkeypatch.setattr("etl.sinks.postgres_sink.PostgresConnection", _FakePGConnection)
    sink = PostgresSink(name="pg", params={"dsn": "db", "table": "t"})
    assert sink.write(iter([pd.DataFrame()])) == 0

    monkeypatch.delenv("POSTGRES_URL", raising=False)
    no_dsn = PostgresSink(name="pg", params={"table": "t"})
    with pytest.raises(SinkError):
        no_dsn.write(iter([_frame()]))

    _FakePGConnection.fail = True
    try:
        failing = PostgresSink(name="pg", params={"dsn": "db", "table": "t"})
        with pytest.raises(SinkError):
            failing.write(iter([_frame()]))
    finally:
        _FakePGConnection.fail = False


def test_postgres_sink_sql_types() -> None:
    """_sql_type maps known dtypes and falls back to TEXT."""
    assert PostgresSink._sql_type("int64") == "BIGINT"
    assert PostgresSink._sql_type("weird_dtype_xyz") == "TEXT"
