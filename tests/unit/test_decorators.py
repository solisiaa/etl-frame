"""Unit tests for universal decorators (sync + async)."""

from __future__ import annotations

import asyncio

import pytest

from etl.decorators import logged, retry, timed, with_metrics
from etl.utils.metrics import MetricsCollector


def test_retry_eventually_succeeds() -> None:
    """@retry makes N attempts before returning."""
    calls = {"n": 0}

    @retry(times=3, backoff=0.0, exceptions=(IOError,))
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise IOError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_retry_exhausts_and_reraises() -> None:
    """@retry re-raises the last error after `times` attempts."""

    @retry(times=2, backoff=0.0, exceptions=(IOError,))
    def always_fail() -> str:
        raise IOError("nope")

    with pytest.raises(IOError):
        always_fail()


def test_retry_async() -> None:
    """@retry also works on coroutine functions."""
    calls = {"n": 0}

    @retry(times=2, backoff=0.0, exceptions=(IOError,))
    async def flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise IOError("first")
        return 7

    assert asyncio.run(flaky()) == 7


def test_timed_records_metric() -> None:
    """@timed logs duration and stores it via collector_attr."""
    m = MetricsCollector()

    class Svc:
        metrics = m

        @timed("my.op", collector_attr="metrics")
        def work(self) -> int:
            return 1

    assert Svc().work() == 1
    assert "my.op" in m.summary()


def test_timed_async() -> None:
    """@timed measures async functions too."""

    @timed("async.op")
    async def work() -> int:
        await asyncio.sleep(0)
        return 2

    assert asyncio.run(work()) == 2


def test_logged_does_not_swallow() -> None:
    """@logged re-raises exceptions after logging them."""

    @logged
    def boom() -> None:
        raise ValueError("bad")

    with pytest.raises(ValueError):
        boom()


def test_logged_async_passthrough() -> None:
    """@logged preserves async return values."""

    @logged
    async def greet(name: str) -> str:
        return f"hi {name}"

    assert asyncio.run(greet("etl")) == "hi etl"


def test_with_metrics_combined() -> None:
    """@with_metrics retries + times + logs in one decorator."""

    @with_metrics(times=2, backoff=0.0, exceptions=(IOError,))
    def maybe(n: int) -> int:
        if n == 0:
            raise IOError("retry me")
        return n

    with pytest.raises(IOError):
        maybe(0)
    assert maybe(5) == 5
