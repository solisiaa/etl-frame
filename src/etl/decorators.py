"""Universal decorators: work on both sync and async functions.

Provided decorators:

- :func:`logged` — structured logging of entry/exit/errors.
- :func:`retry` — exponential backoff retry.
- :func:`timed` — measure duration, optionally record into metrics.
- :func:`with_metrics` — combined: logs + times + retries.

Example:
    ```python
    from etl.decorators import with_metrics

    @with_metrics(times=3, backoff=1.0)
    def load(path: str) -> pd.DataFrame: ...
    ```
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from typing import Any, Optional, TypeVar, cast

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def _is_coro(fn: Callable[..., Any]) -> bool:
    """Return True for ``async def`` functions (incl. wrapped ones)."""
    return inspect.iscoroutinefunction(fn)


def logged(fn: F) -> F:
    """Log function entry, exit and exceptions at DEBUG/ERROR level.

    Works transparently for ``async def`` functions.
    """

    @functools.wraps(fn)
    def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        logger.debug("enter %s args=%r kwargs=%r", fn.__qualname__, args, kwargs)
        try:
            result = fn(*args, **kwargs)
        except Exception:
            logger.exception("error in %s", fn.__qualname__)
            raise
        logger.debug("exit %s ok", fn.__qualname__)
        return result

    @functools.wraps(fn)
    async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
        logger.debug("enter %s args=%r kwargs=%r", fn.__qualname__, args, kwargs)
        try:
            result = await fn(*args, **kwargs)
        except Exception:
            logger.exception("error in %s", fn.__qualname__)
            raise
        logger.debug("exit %s ok", fn.__qualname__)
        return result

    if _is_coro(fn):
        return cast("F", _async_wrapper)
    return cast("F", _sync_wrapper)


def retry(
    times: int = 3,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (IOError, OSError),
) -> Callable[[F], F]:
    """Retry a function with exponential backoff.

    Args:
        times: Total attempts (including the first one).
        backoff: Base sleep in seconds; attempt ``n`` sleeps ``backoff * 2**(n-1)``.
        exceptions: Only these exceptions trigger a retry.

    Works for both sync and async functions.
    """

    def _decorator(fn: F) -> F:
        @functools.wraps(fn)
        def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            last: Optional[BaseException] = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last = exc
                    if attempt == times:
                        break
                    delay = backoff * (2.0 ** (attempt - 1))
                    logger.warning(
                        "retry %s attempt %d/%d after %.2fs: %s",
                        fn.__qualname__,
                        attempt,
                        times,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
            assert last is not None
            raise last

        @functools.wraps(fn)
        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            last_async: Optional[BaseException] = None
            for attempt in range(1, times + 1):
                try:
                    return await fn(*args, **kwargs)
                except exceptions as exc:
                    last_async = exc
                    if attempt == times:
                        break
                    delay = backoff * (2.0 ** (attempt - 1))
                    logger.warning(
                        "retry %s attempt %d/%d after %.2fs: %s",
                        fn.__qualname__,
                        attempt,
                        times,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
            assert last_async is not None
            raise last_async

        if _is_coro(fn):
            return cast("F", _async_wrapper)
        return cast("F", _sync_wrapper)

    return _decorator


def timed(
    metric_name: Optional[str] = None,
    collector_attr: Optional[str] = None,
) -> Callable[[F], F]:
    """Measure execution time and log it.

    Args:
        metric_name: Key to store under; defaults to function qualname.
        collector_attr: If given, look for ``self.<collector_attr>`` with a
            ``record(name, value)`` method and store the duration there.
            (Keeps the decorator dependency-free for unit tests.)
    """

    def _decorator(fn: F) -> F:
        name = metric_name or fn.__qualname__

        def _store(first_arg: Any, value: float) -> None:
            if collector_attr and first_arg is not None and hasattr(first_arg, collector_attr):
                collector = getattr(first_arg, collector_attr)
                record = getattr(collector, "record", None)
                if callable(record):
                    record(name, value)

        @functools.wraps(fn)
        def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                logger.info("%s took %.3fs", name, elapsed)
                if args:
                    _store(args[0], elapsed)

        @functools.wraps(fn)
        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return await fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                logger.info("%s took %.3fs", name, elapsed)
                if args:
                    _store(args[0], elapsed)

        if _is_coro(fn):
            return cast("F", _async_wrapper)
        return cast("F", _sync_wrapper)

    return _decorator


def with_metrics(
    times: int = 3,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (IOError, OSError),
    metric_name: Optional[str] = None,
) -> Callable[[F], F]:
    """Combined decorator: :func:`logged` + :func:`timed` + :func:`retry`.

    Args:
        times: Retry attempts.
        backoff: Backoff base seconds.
        exceptions: Retriable exceptions.
        metric_name: Optional metric key override.
    """

    def _decorator(fn: F) -> F:
        wrapped: F = logged(timed(metric_name)(retry(times, backoff, exceptions)(fn)))
        return wrapped

    return _decorator
