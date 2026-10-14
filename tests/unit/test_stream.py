"""Unit tests for chunked streaming (generators, memory)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.io.stream import concat_stream, read_chunks


def test_read_chunks_counts(tmp_csv: Path) -> None:
    """Chunks cover all rows without overlap."""
    chunks = list(read_chunks(tmp_csv, chunk_size=30))
    assert len(chunks) == 4  # 30+30+30+10
    assert sum(len(c) for c in chunks) == 100
    assert all(isinstance(c, pd.DataFrame) for c in chunks)


def test_read_chunks_lazy(tmp_csv: Path) -> None:
    """read_chunks returns a lazy iterator (generator)."""
    import types

    stream = read_chunks(tmp_csv, chunk_size=10)
    assert isinstance(stream, types.GeneratorType)
    first = next(stream)
    assert len(first) == 10


def test_concat_stream_empty() -> None:
    """Empty stream -> empty frame (no crash)."""
    out = concat_stream(iter([]))
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_memory_bounded(tmp_csv: Path) -> None:
    """Peak memory stays bounded: process chunk-by-chunk via tracemalloc."""
    import tracemalloc

    tracemalloc.start()
    peak_before = tracemalloc.get_traced_memory()[1]
    total = 0
    for chunk in read_chunks(tmp_csv, chunk_size=10):
        total += len(chunk)
        # Simulate per-chunk work; chunk goes out of scope each iteration.
        _ = chunk.sum(numeric_only=True)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert total == 100
    # Peak should be far below "load everything 10x" pathological usage.
    assert peak < peak_before + 50_000_000  # 50 MB guard
