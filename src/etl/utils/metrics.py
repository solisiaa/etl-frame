"""In-memory metrics collector (durations, counters, row counts)."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


class MetricsCollector:
    """Thread-safe collector of named numeric metrics.

    Example:
        ```python
        m = MetricsCollector()
        m.record("rows_in", 100)
        with m.timer("extract"):
            ...
        ```
    """

    def __init__(self) -> None:
        """Initialize empty store."""
        self._lock = threading.Lock()
        self._data: dict[str, list[float]] = {}

    def record(self, name: str, value: float) -> None:
        """Append ``value`` under ``name``."""
        with self._lock:
            self._data.setdefault(name, []).append(value)

    def increment(self, name: str, delta: float = 1.0) -> None:
        """Add ``delta`` to the last value (or start from zero)."""
        with self._lock:
            values = self._data.setdefault(name, [0.0])
            values[-1] = values[-1] + delta

    @contextmanager
    def timer(self, name: str) -> Iterator[None]:
        """Context manager measuring a block and recording its duration."""
        start = time.perf_counter()
        try:
            yield
        finally:
            self.record(name, time.perf_counter() - start)

    def summary(self) -> dict[str, Any]:
        """Return aggregated stats: count/sum/last/avg per metric."""
        with self._lock:
            out: dict[str, Any] = {}
            for key, values in self._data.items():
                out[key] = {
                    "count": len(values),
                    "sum": sum(values),
                    "last": values[-1] if values else 0.0,
                    "avg": (sum(values) / len(values)) if values else 0.0,
                }
            return out

    def as_table_rows(self) -> list[tuple[str, str]]:
        """Flatten summary for Rich table rendering."""
        rows: list[tuple[str, str]] = []
        for key, stats in self.summary().items():
            s = stats  # dict with count/sum/last/avg
            rows.append((key, f"count={s['count']} sum={s['sum']:.3f} last={s['last']:.3f}"))
        return rows
